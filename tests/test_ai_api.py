""""""
from os import getenv
import time
import shimoku_api_python as shimoku

access_token: str = getenv('API_TOKEN')
universe_id: str = getenv('UNIVERSE_ID')
business_id: str = getenv('BUSINESS_ID')
verbosity: str = getenv('VERBOSITY')
environment: str = getenv('ENVIRONMENT')

s = shimoku.Client(
    access_token=access_token,
    universe_id=universe_id,
    verbosity=verbosity,
    environment=environment,
)
s.set_workspace(uuid=business_id)

workflow_to_test = 'TEST WORKFLOW'
menu_path = 'test-ai'
model_name = 'test_model'
s.set_menu_path(name=menu_path)
s.ai.show_available_workflows()
s.ai.show_available_models()
s.ai.show_available_input_files()
s.ai.show_last_execution_logs_by_workflow(workflow_to_test)

# s.menu_paths.delete_all_menu_path_files(name=menu_path, with_shimoku_generated=True)


def test_create_input_files():
    input_files = s.ai.get_available_input_files()
    if 'file1' in [input_file['file_name'] for input_file in input_files]:
        s.ai.delete_input_file(file_name='file1')
    if 'file2' in [input_file['file_name'] for input_file in input_files]:
        s.ai.delete_input_file(file_name='file2')
    len_files = len(s.menu_paths.get_menu_path_files(name=menu_path))
    len_input_files = len(s.ai.get_available_input_files())
    s.ai.create_input_files({'file1': b'', 'file2': (b'', {'metadata_key': 'metadata_value'})})
    assert len(s.menu_paths.get_menu_path_files(name=menu_path)) == len_files
    input_files = s.ai.get_available_input_files()
    assert len(input_files) == len_input_files + 2
    assert 'file1' in [input_file['file_name'] for input_file in input_files]
    assert 'file2' in [input_file['file_name'] for input_file in input_files]
    assert (('metadata_key', 'metadata_value') in
            [input_file_metadata.items()
                for input_file_metadata in input_files if input_file_metadata['file_name'] == 'file2'][0])
    s.ai.delete_input_file(file_name='file1')
    s.ai.delete_input_file(file_name='file2')
    assert len(s.menu_paths.get_menu_path_files(name=menu_path, with_shimoku_generated=True)) == len_files + 2
    assert len(s.ai.get_available_input_files()) == len_input_files


def test_execute_workflow():
    s.ai.create_input_files(input_files={f'Great Content': (
        b'//////////////////////////// CONTENT OF THE INPUT FILE ////////////////////////////\n\n'
        b'Great Content\n\n///////////////////////////////////////////////////////////////////////////////////',
        {'Great metadata entry': 'important file info'})}, force_overwrite=True)

    run_id = s.ai.generic_execute(
        workflow_to_test,
        input_data='Great Content',
        text_mandatory='Important text',
        text_optional='Less important text',
        add_timestamp=True
    )
    first_time = time.time()
    activity_ended = False
    last_log = ''
    s.disable_caching()
    while time.time() - first_time < 30:
        run = s.ai.get_last_executions_with_logs(workflow_to_test)[0]
        if last_log != run['logs'][-1]['message']:
            last_log = run['logs'][-1]['message']
            print(last_log)
            if last_log == 'The Activity has ended':
                s.enable_caching()
                activity_ended = True
                break
    if not activity_ended:
        raise Exception('Workflow did not finish')


test_create_input_files()
# test_execute_workflow()

s.run()
