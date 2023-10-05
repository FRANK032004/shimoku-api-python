import asyncio
from typing import Optional, List, Dict, Tuple

from copy import copy

from ..resources.app import App
from ..resources.universe import Universe
from ..resources.activity import Activity
from ..resources.file import File
from ..resources.activity_template import ActivityTemplate
from ..async_execution_pool import async_auto_call_manager, ExecutionPoolContext
from ..exceptions import WorkflowError, ShimokuFileError, ActivityError

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

    @staticmethod
    @logging_before_and_after(logging_level=logger.debug)
    def _get_output_file_name(
        activity_template: ActivityTemplate, file_name: str, run_id: str
    ) -> str:
        """ Get the name of an output file of a workflow
        :param activity_template: Activity template of the workflow
        :param file_name: Name of the file
        :return: The name of the output file
        """
        name = activity_template['name']
        version = activity_template['version']
        return f"shimoku_generated_file_{name}_{version}_{run_id}_{file_name}"

    @logging_before_and_after(logging_level=logger.debug)
    def _check_app_is_set(self):
        """ Check that the app is set """
        if self._app is None:
            log_error(logger, 'Menu path not set. Please use set_menu_path() method first.', AttributeError)

    @logging_before_and_after(logging_level=logger.debug)
    def _get_model_metadata(self, model: File) -> dict:
        """ Get the metadata of a model
        :param model: Model
        :return: The metadata of the model
        """
        metadata: dict = copy(model['metadata'])
        for tag in model['tags']:
            if tag.startswith('creator_workflow:'):
                metadata['creator_workflow'] = tag[len('creator_workflow:'):]
            elif tag.startswith('creator_workflow_version:'):
                metadata['creator_workflow_version'] = tag[len('creator_workflow_version:')]
        return metadata

    @logging_before_and_after(logging_level=logger.debug)
    def _get_output_file_metadata(self, file: File):
        """ Get the metadata of an output file
        :param file: File
        :return: The metadata of the output file
        """
        metadata: dict = copy(file['metadata'])
        for tag in file['tags']:
            if tag.startswith('creator_workflow:'):
                metadata['creator_workflow'] = tag[len('creator_workflow:'):]
            elif tag.startswith('creator_workflow_version:'):
                metadata['creator_workflow_version'] = tag[len('creator_workflow_version:'):]
            elif tag.startswith('model_name:'):
                metadata['model_name'] = tag[len('model_name:'):]
        return metadata

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
    async def _check_and_get_model(self, model_name: str) -> File:
        """ Check that a model exists and get it
        :param model_name: Name of the model
        """
        app_files = await self._app.get_files()
        for file in app_files:
            if 'shimoku_generated' not in file['tags'] or 'ai_model' not in file['tags']:
                continue
            metadata = self._get_model_metadata(file)
            if file['name'] == "shimoku_generated_model_" + model_name and metadata['model_name'] == model_name:
                return file
        log_error(logger, f"The model {model_name} does not exist", WorkflowError)

    @async_auto_call_manager(execute=True)
    async def _check_run_id_exists(self, activity_template: ActivityTemplate, run_id: str):
        """ Check if a run ID exists for the activity of a workflow
        :param activity_template: Activity template to check
        :param run_id: ID of the run to check
        """
        activity: Activity = await self._get_activity_from_template(activity_template, create_if_not_exists=False)
        if not activity.get_run(run_id):
            log_error(logger, f"Run {run_id} does not exist for activity {activity_template['name']}", ActivityError)

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

    @logging_before_and_after(logging_level=logger.debug)
    async def _create_output_file(
        self, activity_template: ActivityTemplate, file_name: str,
        file: bytes, run_id: str, model_name: str
    ) -> str:
        """ Create an output file of a workflow
        :param activity_template: Activity template of the workflow
        :param file: File to be uploaded
        :param run_id: Id of the run
        :return: The uuid of the created file
        """
        complete_file_name = self._get_output_file_name(activity_template, file_name, run_id)

        file = await self._app.create_file(
            name=complete_file_name, file_object=file,
            tags=['shimoku_generated', 'ai_output_file',
                  f'creator_workflow:{activity_template["name"]}',
                  f'creator_workflow_version:{activity_template["version"]}' 
                  f'model_name:{model_name}]'],
            metadata={
                'file_name': file_name,
                'run_id': run_id,
            }
        )
        logger.info(f'Created output file {file_name}')
        return file['id']

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def show_available_workflows(self, show_input_parameters: bool = False):
        """ Show the available workflows
        :param show_input_parameters: Show the input parameters of each workflow
        """
        message = [
            "",
            "///////////////////////////////////////////////////",
            "/////////////// Available workflows ///////////////",
        ]
        templates = await self._universe.get_activity_templates()
        while len(templates) > 0:
            template = templates.pop(0)
            if not template['enabled']:
                continue
            versions = [template['version']]
            while len(templates) > 0 and templates[0]['name'] == template['name']:
                template = templates.pop(0)
                versions.append(template['version'])
            message.append("")
            message.append(f" \033[1m- AI function:\033[0m {template['name']} (v{', v'.join(versions)})")
            message.append(
                f"   \033[1mDescription:\033[0m {template['description']} "
                f"(wait time between runs: >{template['minRunInterval']}s)"
            )
            if show_input_parameters:
                message.append(f"   \033[1mInput parameters:\033[0m")
                for param_name, param in template['inputSettings'].items():
                    message.append(f"     \033[1m- {param_name}"
                                   f"{' (Optional)' if not param['mandatory'] else ''}:\033[0m {param['datatype']}")
                    if param['description']:
                        message.append(f"       {param['description']}")
        message.extend(["", "///////////////////////////////////////////////////", ""])
        print('\n'.join(message))

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def show_workflow_parameters(self, name: str):
        """ Show the parameters of a workflow
        :param name: Name of the workflow
        """
        template = await self._get_activity_template(name, None)
        message = [
            "",
            "///////////////////////////////////////////////////",
            f" {name} parameters ".center(51, '/'),
            "",
            f"\033[1mDescription:\033[0m {template['description']} "
            f"(wait time between runs: >{template['minRunInterval']}s)",
            f"\033[1mInput parameters:\033[0m"
        ]
        for param_name, param in template['inputSettings'].items():
            message.append(f"  \033[1m- {param_name}"
                           f"{' (Optional)' if not param['mandatory'] else ''}:\033[0m {param['datatype']}")
            if param['description']:
                message.append(f"    {param['description']}")

        message.extend(["", "///////////////////////////////////////////////////", ""])
        print('\n'.join(message))

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def show_available_models(self):
        """ Show the available models """
        self._check_app_is_set()
        message = [
            "",
            "///////////////////////////////////////////////////",
            "//////////////// Available models /////////////////",
        ]
        app_files: List[File] = await self._app.get_files()
        for file in app_files:
            if 'shimoku_generated' not in file['tags']:
                continue
            if 'ai_model' not in file['tags']:
                continue
            message.extend([
                '',
                f" \033[1m- Model name:\033[0m {file['metadata']['model_name']}",
                f"   \033[1mMetadata:\033[0m"
            ])
            for key, value in self._get_model_metadata(file).items():
                if key == 'model_name':
                    continue
                message.append(f"     \033[1m- {key}:\033[0m {value}")

        message.extend(["", "///////////////////////////////////////////////////", ""])
        print('\n'.join(message))

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
        message = [
            '',
            '///////////////////////////////////////////////////',
            f' LOGS OF {name.upper()} '.center(51, '/')
        ]

        for run in runs:
            message.extend([
                '',
                f' - Run {run["id"]}:',
                f'   Settings: {", ".join([f"{key}: {value}" for key, value in run["settings"].items()])}',
                f'   Logs:',
                *[f'     - {log["severity"]} | {log["message"]}, at {log["dateTime"]}'
                  for log in (await run.get_logs())]
            ])

        message.extend(['', "///////////////////////////////////////////////////", ''])

    @logging_before_and_after(logging_level=logger.debug)
    async def _check_and_store_output_file(
        self, model_metadata: dict, file: File, files_by_run_id: Dict[str, Dict[str, File]],
        run_id: Optional[str] = None, file_name: Optional[str] = None, get_objects: bool = False
    ):
        """ Check if a file is an output file then add it to the list of output files
        :param model_metadata: Metadata of the model used
        :param file: File to check
        :param files_by_run_id: Dictionary of output files by run ID
        :param run_id: ID of the run to check
        :param file_name: Name of the file to check
        :param get_objects: Get the objects of the files instead of their IDs
        """
        if 'shimoku_generated' not in file['tags'] or 'ai_output_file' not in file['tags']:
            return
        output_file_metadata = self._get_output_file_metadata(file)
        if (output_file_metadata['model_name'] != model_metadata['model_name'] or
                (file_name and file_name != output_file_metadata['file_name'])):
            return
        output_file_run_id: str = output_file_metadata['run_id']
        if run_id and output_file_run_id != run_id:
            return
        if run_id not in files_by_run_id:
            files_by_run_id[output_file_run_id] = {}
        result = file['id'] if not get_objects else await self._app.get_file_object(file['id'])
        files_by_run_id[output_file_run_id][output_file_metadata['file_name']] = result

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def get_output_files_by_model(
        self, model_name: str, run_id: Optional[str] = None,
        file_name: Optional[str] = None, get_objects: bool = False,
    ):
        """ Get output files for the executions of workflows with a given model
        :param model_name: Name of the model to use
        :param run_id: ID of the executed run
        :param file_name: Name of the file to get
        :param get_objects: Get the file objects instead of the file IDs
        :return: Dictionary of output files by the execution identifier
        """
        app_files: List[File] = await self._app.get_files()
        model_file: File = await self._check_and_get_model(model_name)
        model_metadata = self._get_model_metadata(model_file)

        files_by_run_id: Dict[str, Dict[str, File]] = {}
        await asyncio.gather(*[
            self._check_and_store_output_file(model_metadata, file, files_by_run_id, run_id, file_name, get_objects)
            for file in app_files
        ])

        return files_by_run_id if run_id is None else files_by_run_id[run_id]

    # TODO: Ask if it is wanted
    # @async_auto_call_manager(execute=True)
    # @logging_before_and_after(logging_level=logger.info)
    # async def get_output_files_by_workflow(
    #     self, name: str, version: Optional[str] = None
    # ):
    #     """ Get output files for a generic workflow
    #     :param name: Name of the workflow to execute
    #     :param version: Version of the workflow to execute
    #     :return: The output files if they exist
    #     """
    #     pass

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def create_model(
        self, model_name: str, model: bytes, creator_workflow: str,
        workflow_version: str, run_id: str, metadata: Optional[dict] = None
    ):
        """ Create a model
        :param model_name: Name of the model
        :param model: Model to be uploaded
        :param creator_workflow: Name of the workflow that created the model
        :param workflow_version: Version of the workflow that created the model
        :param run_id: ID of the run that created the model
        :param metadata: Metadata of the model
        """
        if not model_name or not isinstance(model_name, str):
            log_error(logger, 'Model name has to be a non-empty string', ValueError)

        activity_template: ActivityTemplate = await self._get_activity_template(creator_workflow, workflow_version)

        await self._check_run_id_exists(activity_template, run_id)
        self._check_app_is_set()

        file_name = f"shimoku_generated_model_{model_name}"

        await self._app.create_file(
            name=file_name, file_object=model,
            tags=[
                'shimoku_generated', 'ai_model',
                f"creator_workflow:{creator_workflow}",
                f"creator_workflow_version:{activity_template['version']}"
            ],
            metadata={
                'model_name': model_name,
                **(metadata or {})
            }
        )
        logger.info(f'Created model {model_name}')

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def create_output_files(
        self, files: Dict[str, bytes], creator_workflow: str, workflow_version: str,
        run_id: str, model_name: Optional[str] = None
    ):
        """ Create output files of a workflow
        :param files: Files to be uploaded
        :param run_id: ID of the executed run
        :param creator_workflow: Name of the executed workflow
        :param workflow_version: Version of the executed workflow
        :param model_name: Name of the model used
        """
        activity_template = await self._get_activity_template(creator_workflow, workflow_version)

        # await self._check_run_id_exists(activity_template, run_id)
        if model_name is not None:
            await self._check_and_get_model(model_name)
        else:
            model_name = ''

        await asyncio.gather(*[self._create_output_file(activity_template, file_name, file, run_id, model_name)
                               for file_name, file in files.items()])

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def get_model(self, model_name: str) -> Tuple[bytes, dict]:
        """ Get a model
        :param model_name: Name of the model
        :return: The model if it exists
        """
        self._check_app_is_set()
        model_file: File = await self._check_and_get_model(model_name)
        if model_file is None:
            log_error(logger, f"The model {model_name} does not exist", WorkflowError)
        return await self._app.get_file_object(model_file['id']), self._get_model_metadata(model_file)

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def delete_model(self, model_name: str):
        """ Delete a model
        :param model_name: Name of the model
        """
        self._check_app_is_set()
        model_file: File = await self._check_and_get_model(model_name)
        if model_file is None:
            log_error(logger, f"The model {model_name} does not exist", WorkflowError)
        await self._app.delete_file(model_file['id'])
        logger.info(f'Deleted model {model_name}')

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

