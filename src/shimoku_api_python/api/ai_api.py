from typing import Optional, List, Dict
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
        templates = await self._universe.get_activity_templates()
        print()
        print("///////////////////////////////////////////////////")
        print("/////////////// Available workflows ///////////////")

        for template in templates*3:
            if not template['enabled']:
                continue
            print()
            print(f" \033[1m- AI function:\033[0m {template['name']}")
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
                return (await self._universe.create_universe_api_key())['id']
            return universe_api_keys[0]['id']
        return universe_api_key

    @logging_before_and_after(logging_level=logger.debug)
    async def _get_activity_template(self, name: str) -> ActivityTemplate:
        """ Get an activity template
        :param name: Name of the activity template
        :return: Activity template
        """
        activity_template = await self._universe.get_activity_template(name=name)
        if activity_template is None:
            log_error(logger, f"The workflow {name} does not exist", WorkflowError)
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

        if self._app is None:
            log_error(logger, 'Menu path not set. Please use set_menu_path() method first.', AttributeError)

        name = activity_template['name']
        activity_name = 'shimoku_generated_activity_' + name

        activity: Optional[Activity] = await self._app.get_activity(name=activity_name)
        if activity is None:
            if not create_if_not_exists:
                log_error(logger, f"The activity {activity_name} does not exist", WorkflowError)
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

    @logging_before_and_after(logging_level=logger.debug)
    async def _create_output_file(self, activity_template: ActivityTemplate, param_name: str, file: bytes) -> str:
        """ Create an output file of a workflow
        :param activity_template: Activity template of the workflow
        :param file: File to be uploaded
        :return: The uuid of the created file
        """
        file_name = f"shimoku_generated_file_{param_name}"
        file = await self._app.create_file(
            name=file_name, file_object=file,
            tags=['shimoku_generated', 'ai_output_file'],
            metadata={'template': activity_template['name']}
        )
        logger.info(f'Created output file {file_name}')
        return file['id']

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def create_output_files(self, name: str, files: Dict[str, bytes], model: Optional[str] = None):
        """ Create output files of a workflow
        :param name: Name of the executed workflow
        :param files: Files to be uploaded
        :param model: Name of the model used
        """

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def get_output_files(self, name: str, model: Optional[str] = None):
        """ Get output files for a generic workflow
        :param name: Name of the workflow to execute
        :param model: Name of the model to use
        :return: The output files if they exist
        """

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def create_model(self, name: str, model: bytes, metadata: Optional[dict] = None):
        """ Create a model
        :param name: Name of the model
        :param model: Model to be uploaded
        :param metadata: Metadata of the model
        """
        file_name = f"shimoku_generated_model_{name}"

        await self._app.create_file(
            name=file_name, file_object=model,
            tags=['shimoku_generated', 'ai_model'],
            metadata={'name': name, **(metadata or {})}
        )
        logger.info(f'Created model {name}')

    @logging_before_and_after(logging_level=logger.debug)
    async def _get_model_file(self, name: str) -> Optional[File]:
        """ Get a model
        :param name: Name of the model
        :return: The model if it exists
        """
        file_name = f"shimoku_generated_model_{name}"
        file = await self._app.get_file(name=file_name)
        if file and ('ai_model' not in file['tags'] or
                     'shimoku_generated' not in file['tags'] or
                     file['metadata']['name'] != name):
            log_error(logger, f"The file {name} is not a model", WorkflowError)
        return file

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def get_model(self, name: str):
        """ Get a model
        :param name: Name of the model
        :return: The model if it exists
        """
        model_file: File = await self._get_model_file(name)
        if model_file is None:
            log_error(logger, f"The model {name} does not exist", WorkflowError)
        return await self._app.get_file_object(model_file['id'])

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
    async def get_last_executions_with_logs(self, name: str, how_many: int = 1):
        """ Get the logs of the executions of a workflow
        :param name: Name of the workflow to execute
        :param how_many: Number of executions to get
        :return: The logs of the workflow
        """
        activity_template: ActivityTemplate = await self._get_activity_template(name)
        activity: Activity = await self._get_activity_from_template(activity_template, create_if_not_exists=False)
        runs: List[Activity.Run] = await activity.get_runs(how_many)
        return [run.cascade_to_dict() for run in runs]

    @async_auto_call_manager(execute=True)
    @logging_before_and_after(logging_level=logger.info)
    async def show_last_execution_logs(self, name: str, how_many: int = 1):
        """ Show the logs of the executions of a workflow
        :param name: Name of the workflow to execute
        :param how_many: Number of executions to get
        """
        activity_template: ActivityTemplate = await self._get_activity_template(name)
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
    async def generic_execute(self, name: str, universe_api_key: Optional[str] = None, **params):
        """ Execute a generic workflow
        :param name: Name of the workflow to execute
        :param universe_api_key: API key of the universe
        :param params: Parameters to be passed to the workflow
        """
        activity_template: ActivityTemplate = await self._get_activity_template(name)
        activity: Activity = await self._get_activity_from_template(activity_template, universe_api_key)
        await self._check_params(activity_template, params)
        run: Activity.Run = await activity.create_run(settings=params)
        logger.info(f'Result of execution: {await run.trigger_webhook()}')

