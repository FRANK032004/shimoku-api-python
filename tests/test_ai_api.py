""""""
from os import getenv

import shimoku_api_python as shimoku

access_token: str = getenv('API_TOKEN')
universe_id: str = getenv('UNIVERSE_ID')
business_id: str = getenv('BUSINESS_ID')
verbosity: str = getenv('VERBOSITY')

s = shimoku.Client(
    access_token=access_token,
    universe_id=universe_id,
    verbosity=verbosity,
)
s.set_workspace(uuid=business_id)

workflow_to_test = 'WORKFLOW_TEST'
menu_path = 'test ai'
model_name = 'test_model'
s.set_menu_path(name=menu_path)
s.ai.show_available_workflows()
s.ai.show_available_models()

s.menu_paths.delete_all_menu_path_files(name=menu_path, with_shimoku_generated=True)


def test_execute_workflow():
    s.ai.generic_execute(workflow_to_test, text_mandatory='test', text_optional='test')
    s.ai.show_last_execution_logs(workflow_to_test)
    run = s.ai.get_last_executions_with_logs(workflow_to_test)[0]
    assert len(run['logs']) == 1


def test_create_model():
    len_files = len(s.menu_paths.get_menu_path_files(name=menu_path))
    s.ai.create_model(model_name, b'', {'workflow': workflow_to_test})
    model, metadata = s.ai.get_model(model_name)
    assert metadata['model_name'] == model_name
    assert metadata['workflow'] == workflow_to_test
    assert model == b''
    assert len(s.menu_paths.get_menu_path_files(name=menu_path)) == len_files
    assert len(s.menu_paths.get_menu_path_files(name=menu_path, with_shimoku_generated=True)) == len_files + 1


def test_create_output_file():
    len_files = len(s.menu_paths.get_menu_path_files(name=menu_path))
    s.ai.create_output_files(files=[{'file1': b'', 'file2': b''}], creator_workflow=workflow_to_test, model_name=model_name)
    assert len(s.menu_paths.get_menu_path_files(name=menu_path)) == len_files
    assert len(s.menu_paths.get_menu_path_files(name=menu_path, with_shimoku_generated=True)) == len_files + 2


test_execute_workflow()
# test_create_model()
# test_create_output_file()

s.run()
