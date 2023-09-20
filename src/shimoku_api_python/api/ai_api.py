import asyncio
from typing import Optional, List, Dict, Tuple
from ..resources.app import App
from ..resources.universe import Universe
from ..resources.activity import Activity
from ..resources.file import File
from ..resources.activity_template import ActivityTemplate
from ..async_execution_pool import async_auto_call_manager, ExecutionPoolContext
from ..exceptions import WorkflowError, ShimokuFileError

import logging
from shimoku_api_python.execution_logger import logging_before_and_after, log_error
logger = logging.getLogger(__name__)


class AiApi:
    @logging_before_and_after(logging_level=logger.debug)
    def __init__(self, universe: Universe, app: App, execution_pool_context: ExecutionPoolContext):
        self._app: App = app
        self._universe: Universe = universe
        if app is not None and universe['id'] != app.parent.parent['id']:
            log_error(logger, f"App {str(app)} does not belong to the universe {str(universe)}", WorkflowError)
        self.epc = execution_pool_context

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def show_available_workflows(self):
        print()
        print("///////////////////////////////////////////////////")
        print("/////////////// Available workflows ///////////////")

        templates = await self._universe.get_activity_templates()
        while len(templates) > 0:
            template = templates.pop(0)
            if not template['enabled']:
                continue
            versions = [template['version']]
            while len(templates) > 0 and templates[0]['name'] == template['name']:
                template = templates.pop(0)
                versions.append(template['version'])
            print()
            print(f" \033[1m- AI function:\033[0m {template['name']} (v{', v'.join(versions)})")
            print(f"   \033[1mDescription:\033[0m {template['description']} (wait time between runs: >{template['minRunInterval']}s)")
            print(f"   \033[1mInput parameters:\033[0m")
            for param_name, param in template['inputSettings'].items():
                print(f"     \033[1m- {param_name}{' (Optional)' if not param['mandatory'] else ''}:\033[0m {param['datatype']}")
                if param['description']:
                    print(f"       {param['description']}")

        print()
        print("///////////////////////////////////////////////////")
        print()

    @logging_before_and_after(logging_level=logger.debug)
    async def _get_universe_api_key(self, universe_api_key: Optional[str]) -> str:
        """ Get the universe API key or create one if none exists
        :param universe_api_key: Optional universe API key
        :return: Universe API key
        """
        if universe_api_key is None:
            universe_api_keys = await self._universe.get_universe_api_keys()
            if len(universe_api_keys) == 0:
                return (await self._universe.create_universe_api_key('AI workflows key'))['id']
            return universe_api_keys[0]['id']
        return universe_api_key

    @logging_before_and_after(logging_level=logger.debug)
    async def _get_activity_template(self, name: str, version: Optional[str]) -> ActivityTemplate:
        """ Get an activity template
        :param name: Name of the activity template
        :param version: Version of the activity template
        :return: Activity template
        """
        if version is not None:
            activity_template = await self._universe.get_activity_template(name_version=(name, version))
        else:
            activity_templates = await self._universe.get_activity_templates()
            activity_template = activity_templates[-1] if len(activity_templates) > 0 else None
        if activity_template is None:
            log_error(logger, f"The workflow {name + (version if version else '')} does not exist", WorkflowError)
        return activity_template

    @logging_before_and_after(logging_level=logger.debug)
    def _check_app_is_set(self):
        """ Check that the app is set """
        if self._app is None:
            log_error(logger, 'Menu path not set. Please use set_menu_path() method first.', AttributeError)

    @logging_before_and_after(logging_level=logger.debug)
    async def _get_activity_from_template(
        self, activity_template: ActivityTemplate, universe_api_key: str = '', create_if_not_exists: bool = True
    ) -> Activity:
        """ Get an activity from an activity template
        :param activity_template: Activity template
        :param universe_api_key: Universe API key
        :return: Activity
        """
        universe_api_key = await self._get_universe_api_key(universe_api_key)

        self._check_app_is_set()

        name = activity_template['name']
        activity_name = 'shimoku_generated_activity_' + name

        activity: Optional[Activity] = await self._app.get_activity(name=activity_name)
        if activity is None:
            if not create_if_not_exists:
                log_error(logger, f"The activity for the workflow {name} does not exist", WorkflowError)
            activity: Activity = await self._app.create_activity(
                name=activity_name,
                template_id=activity_template['id'],
                universe_api_key=universe_api_key
            )
            logger.info(f'Created activity for workflow {name}')

        return activity

    @logging_before_and_after(logging_level=logger.debug)
    async def _create_input_file(self, activity_template: ActivityTemplate, file: bytes, param_name: str) -> str:
        """ Create an input file for a workflow and return its uuid
        :param activity_template: Activity template of the workflow
        :param file: File to be uploaded
        :return: The uuid of the created file
        """
        file_name = f"shimoku_generated_file_{param_name}"
        file = await self._app.create_file(
            name=file_name, file_object=file,
            tags=['shimoku_generated', 'ai_input_file'],
            metadata={'template': activity_template['name']}
        )
        logger.info(f'Created input file {file_name}')
        return file['id']

    @staticmethod
    @logging_before_and_after(logging_level=logger.debug)
    def _get_output_file_name(
        activity_template: ActivityTemplate, file_name: str, model_name: Optional[str] = None
    ) -> str:
        """ Get the name of an output file of a workflow
        :param activity_template: Activity template of the workflow
        :param file_name: Name of the file
        :return: The name of the output file
        """
        name = activity_template['name']
        version = activity_template['version']
        return f"shimoku_generated_file_{name}_{version}_{(model_name+'_' if model_name else'')}{file_name}"

    @logging_before_and_after(logging_level=logger.debug)
    async def _check_model_exists(self, model_name: str):
        """ Check if a model exists
        :param model_name: Name of the model
        """
        app_files = await self._app.get_files()
        for file in app_files:
            if (file['name'] == "shimoku_generated_model_" + model_name and
               file['metadata']['name'] == model_name and
               'shimoku_generated' in file['tags'] and 'ai_model' in file['tags']):
                return
        log_error(logger, f"The model {model_name} does not exist", WorkflowError)

    @logging_before_and_after(logging_level=logger.debug)
    async def _create_output_file(
        self, activity_template: ActivityTemplate, file_name: str, file: bytes, model_name: Optional[str] = None
    ) -> str:
        """ Create an output file of a workflow
        :param activity_template: Activity template of the workflow
        :param file: File to be uploaded
        :return: The uuid of the created file
        """
        complete_file_name = self._get_output_file_name(activity_template, file_name, model_name)

        if model_name is not None:
            await self._check_model_exists(model_name)

        file = await self._app.create_file(
            name=complete_file_name, file_object=file,
            tags=['shimoku_generated', 'ai_output_file'],
            metadata={
                'file_name': file_name,
                'template': activity_template['name'],
                'template_version': activity_template['version'],
                'model': model_name
            }
        )
        logger.info(f'Created output file {file_name}')
        return file['id']

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def create_output_files(
        self, files: Dict[str, bytes], workflow: str,
        version: Optional[str] = None, model_name: Optional[str] = None
    ):
        """ Create output files of a workflow
        :param files: Files to be uploaded
        :param workflow: Name of the executed workflow
        :param version: Version of the executed workflow
        :param model_name: Name of the model used
        """
        activity_template = await self._get_activity_template(workflow, version)
        await self._get_activity_from_template(activity_template, create_if_not_exists=False)
        for file_name, file in files.items():
            await self._create_output_file(activity_template, file_name, file, model_name)

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def get_output_files(
        self, name: str, version: Optional[str] = None, model: Optional[str] = None
    ):
        """ Get output files for a generic workflow
        :param name: Name of the workflow to execute
        :param version: Version of the workflow to execute
        :param model: Name of the model to use
        :return: The output files if they exist
        """
        activity_template = await self._get_activity_template(name, version)
        await self._get_activity_from_template(activity_template, create_if_not_exists=False)
        app_files = await self._app.get_files()
        file_name_starts_with = self._get_output_file_name(activity_template, '', model)
        files = [file for file in app_files if file['name'].startswith(file_name_starts_with)]
        for file in files:
            if 'shimoku_generated' not in file['tags']:
                log_error(logger, f"File {file['name']} is not a shimoku generated file", WorkflowError)
            if 'ai_output_file' not in file['tags']:
                log_error(logger, f"File {file['name']} is not an output file", WorkflowError)
            if file['metadata']['template'] != activity_template['name']:
                log_error(logger, f"File {file['name']} is not from the workflow {name}", WorkflowError)
            if file['metadata']['template_version'] != activity_template['version']:
                log_error(logger, f"File {file['name']} is not from the version {version} of the workflow {name}", WorkflowError)
            if model and file['metadata']['model'] != model:
                log_error(logger, f"File {file['name']} is not from the model {model}", WorkflowError)
        file_objects = await asyncio.gather(*[self._app.get_file_object(file['id']) for file in files])
        return {file['metadata']['file_name']: file_object for file, file_object in zip(files, file_objects)}

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def show_available_models(self):
        """ Show available models """
        self._check_app_is_set()
        print()
        print("///////////////////////////////////////////////////")
        print("//////////////// Available models /////////////////")

        app_files = await self._app.get_files()
        for file in app_files:
            print()
            if 'shimoku_generated' not in file['tags']:
                continue
            if 'ai_model' not in file['tags']:
                continue
            print(f"- Model {file['metadata']['model_name']}")
            if file['metadata']:
                print(f"  Metadata:")
                for key, value in file['metadata'].items():
                    if key == 'model_name':
                        continue
                    print(f"    - {key}: {value}")
        print()
        print("///////////////////////////////////////////////////")
        print()

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def create_model(
        self, model_name: str, model: bytes, creator_workflow: str,
        workflow_version: Optional[str] = None, metadata: Optional[dict] = None
    ):
        """ Create a model
        :param model_name: Name of the model
        :param model: Model to be uploaded
        :param creator_workflow: Name of the workflow that created the model
        :param workflow_version: Version of the workflow that created the model
        :param metadata: Metadata of the model
        """
        # To check that the workflow exists
        activity_template = await self._get_activity_template(creator_workflow, workflow_version)
        await self._get_activity_from_template(activity_template, create_if_not_exists=False)

        self._check_app_is_set()
        file_name = f"shimoku_generated_model_{model_name}"

        await self._app.create_file(
            name=file_name, file_object=model,
            tags=['shimoku_generated', 'ai_model'],
            metadata={
                'model_name': model_name,
                'creator_workflow': creator_workflow,
                'creator_workflow_version': activity_template['version'],
                **(metadata or {})
            }
        )
        logger.info(f'Created model {model_name}')

    @logging_before_and_after(logging_level=logger.debug)
    async def _get_model_file(self, model_name: str) -> Optional[File]:
        """ Get a model
        :param model_name: Name of the model
        :return: The model if it exists
        """
        file_name = f"shimoku_generated_model_{model_name}"

        file = await self._app.get_file(name=file_name)
        if file and ('ai_model' not in file['tags'] or
                     'shimoku_generated' not in file['tags'] or
                     file['metadata']['model_name'] != model_name):
            log_error(logger, f"The file {model_name} is not a model", WorkflowError)
        return file

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def get_model(self, model_name: str) -> Tuple[bytes, dict]:
        """ Get a model
        :param model_name: Name of the model
        :return: The model if it exists
        """
        self._check_app_is_set()
        model_file: File = await self._get_model_file(model_name)
        if model_file is None:
            log_error(logger, f"The model {model_name} does not exist", WorkflowError)
        return await self._app.get_file_object(model_file['id']), model_file['metadata']

    @logging_before_and_after(logging_level=logger.debug)
    async def _check_params(self, activity_template: ActivityTemplate, params: dict):
        """ Check the parameters passed to the workflow, and create input files if necessary
        :param activity_template: Activity template of the workflow
        :param params: Parameters to be passed to the workflow
        """
        input_settings = activity_template['inputSettings']
        if any(param not in input_settings for param in params):
            log_error(
                logger,
                f"Unknown parameters for workflow {activity_template['name']}: "
                f"{[param for param in params if param not in input_settings]} \n"
                f"The possible parameters are: {list(input_settings.keys())}",
                WorkflowError
            )
        for param_name, definition in input_settings.items():
            if param_name not in params:
                if definition['mandatory']:
                    log_error(
                        logger,
                        f"Missing parameter {param_name} for activity template {activity_template['name']}, "
                        f"the description of the missing parameter is: {definition['description']}",
                        WorkflowError
                    )
                else:
                    continue
            param_value = params[param_name]
            param_definition_type = definition['datatype']
            if definition['datatype'] == 'file':
                if isinstance(param_value, str):
                    if not await self._app.get_file(uuid=param_value):
                        log_error(logger, f"File with uuid {param_value} does not exist", ShimokuFileError)
                else:
                    params[param_name] = await self._create_input_file(activity_template, param_value, param_name)
            elif str(type(param_value)) != f"<class '{param_definition_type}'>":
                log_error(
                    logger,
                    f"Wrong type for parameter {param_name} for activity template {activity_template['name']}"
                    f"the description of the missing parameter is: {definition['description']}\n"
                    f"Expected type: {param_definition_type}\n"
                    f"Provided type: {str(type(param_value))[8:-2]}",
                    WorkflowError
                )

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def get_last_executions_with_logs(
        self, name: str, version: Optional[str] = None, how_many: int = 1
    ):
        """ Get the logs of the executions of a workflow
        :param name: Name of the workflow to execute
        :param version: Version of the workflow to execute
        :param how_many: Number of executions to get
        :return: The logs of the workflow
        """
        activity_template: ActivityTemplate = await self._get_activity_template(name, version)
        activity: Activity = await self._get_activity_from_template(activity_template, create_if_not_exists=False)
        runs: List[Activity.Run] = await activity.get_runs(how_many)
        return [run.cascade_to_dict() for run in runs]

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def show_last_execution_logs(
        self, name: str, version: Optional[str] = None, how_many: int = 1
    ):
        """ Show the logs of the executions of a workflow
        :param name: Name of the workflow to execute
        :param version: Version of the workflow to execute
        :param how_many: Number of executions to get
        """
        activity_template: ActivityTemplate = await self._get_activity_template(name, version)
        activity: Activity = await self._get_activity_from_template(activity_template, create_if_not_exists=False)
        runs: List[Activity.Run] = await activity.get_runs(how_many)
        print()
        print("///////////////////////////////////////////////////")
        string_to_print = f' LOGS OF {name.upper()} '
        print(string_to_print.center(51, '/'))

        for run in runs:
            print()
            print(f' - Run {run["id"]}:')
            print(f'   Settings: {", ".join([f"{key}: {value}" for key, value in run["settings"].items()])}')
            print(f'   Logs:')
            for log in (await run.get_logs()):
                print(f'     - {log["severity"]} | {log["message"]}, at {log["dateTime"]}')

        print()
        print("///////////////////////////////////////////////////")
        print()

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def generic_execute(
        self, name: str, version: Optional[str] = None,
        universe_api_key: Optional[str] = None, **params
    ):
        """ Execute a generic workflow
        :param name: Name of the workflow to execute
        :param version: Version of the workflow to execute
        :param universe_api_key: API key of the universe
        :param params: Parameters to be passed to the workflow
        """
        activity_template: ActivityTemplate = await self._get_activity_template(name, version)
        activity: Activity = await self._get_activity_from_template(activity_template, universe_api_key)
        await self._check_params(activity_template, params)
        run: Activity.Run = await activity.create_run(settings=params)
        logger.info(f'Result of execution: {await run.trigger_webhook()}')

