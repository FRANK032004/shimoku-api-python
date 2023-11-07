""""""
from os import getenv
import unittest
import time
import shimoku_api_python as shimoku
import json
import datetime as dt
from shimoku_api_python.exceptions import ShimokuFileError

access_token: str = getenv('UNIVERSE_API_TOKEN')
universe_id: str = getenv('UNIVERSE_ID')
business_id: str = getenv('BUSINESS_ID')
verbosity: str = getenv('VERBOSITY')
environment: str = getenv('ENVIRONMENT')

last_execution_time: float = 0


def execute_ai_function(wf: str) -> str:
    global last_execution_time
    tics = 0
    while last_execution_time is not None and time.time() - last_execution_time < 32:
        print('\rWaiting for min run interval'+('.'*(tics % 4))+'      ', end='')
        tics += 1
        time.sleep(0.5)
    print()
    s.ai.create_input_files(
        input_files={
            f'Great Content': (
                b'//////////////////////////// CONTENT OF THE INPUT FILE ////////////////////////////\n\n'
                b'Great Content\n\n///////////////////////////////////////////////////////////////////////////////////',
                {'Great metadata entry': 'important file info'}
            )
        },
        force_overwrite=True
    )
    last_execution_time = time.time()
    return s.ai.generic_execute(
        wf,
        input_data='Great Content',
        text_mandatory='Important text',
        text_optional='Less important text',
        add_timestamp=True
    )


s = shimoku.Client(
    access_token=access_token,
    universe_id=universe_id,
    verbosity=verbosity,
    environment=environment,
)
s.set_workspace(uuid=business_id)

ai_function_to_test = 'TEST WORKFLOW'
ai_function_id_to_test = '355427f5-6fb1-45ea-82e9-a2755005b8aa'
menu_path = 'test-ai'
model_name = 'test-model'

s.menu_paths.delete_all_menu_path_files(name=menu_path, with_shimoku_generated=True)
s.menu_paths.delete_all_menu_path_activities(name=menu_path, with_linked_to_templates=True)
s.menu_paths.delete_menu_path(name=menu_path)

s.set_menu_path(name=menu_path)


def test_private_methods():
    run_id_for_model = execute_ai_function(ai_function_to_test)
    s.ai.check_for_private_access(
        ai_function_id='355427f5-6fb1-45ea-82e9-a2755005b8aa', run_id=run_id_for_model
    )
    ai_function_methods = s.ai.get_private_ai_function_methods()
    ai_function_methods.create_model(
        model_name='test-model', model=b'',
        metadata={'other_field': 'other_value'}
    )
    model, metadata = ai_function_methods.get_model(model_name='test-model')
    assert model == b''
    assert metadata == {
        'creator_ai_function': ai_function_to_test,
        'creator_ai_function_id': ai_function_id_to_test,
        'creator_ai_function_version': '',
        'model_name': model_name,
        'other_field': 'other_value',
        'run_id': run_id_for_model
    }
    s.ai.delete_model(model_name='test-model')

    class ModelDoesntExist(unittest.TestCase):
        def test_model_doesnt_exist(self):
            with self.assertRaises(ShimokuFileError):
                ai_function_methods.get_model(model_name='test-model')

    t = ModelDoesntExist()
    t.test_model_doesnt_exist()

    # For the following tests the model must exist
    ai_function_methods.create_model(
        model_name='test-model', model=b'',
        metadata={'other_field': 'other_value'}
    )


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


def test_execute_ai_function():
    run_id = execute_ai_function(ai_function_to_test)
    first_time = time.time()
    activity_ended = False
    last_log = ''
    s.disable_caching()
    while time.time() - first_time < 60:
        run = s.ai.get_last_executions_with_logs(ai_function_to_test)[0]
        if last_log != run['logs'][-1]['message']:
            last_log = run['logs'][-1]['message']
            print(last_log)
            if last_log == 'The Activity has ended':
                activity_ended = True
                break
    if not activity_ended:
        raise Exception('AI function did not finish')
    output_dict = s.ai.get_output_file_objects(run_id=run_id)
    s.enable_caching()
    file_name, file_obj = list(output_dict.items())[0]
    decoded_file = file_obj.decode('utf-8')
    assert 'Important text' in decoded_file
    assert 'Less important text' in decoded_file
    assert 'Great Content' in decoded_file
    assert dt.datetime.now().strftime('%Y-%m-%d') in file_name

    def assert_output_dict(_output_dict: dict):
        if 'model_name' in _output_dict:
            assert _output_dict['model_name'] == model_name
        if 'ai_function_name' in _output_dict:
            assert _output_dict['ai_function_name'] == ai_function_to_test
            assert _output_dict['ai_function_version'] == ""

        assert _output_dict['input']['args'] == {
            'add_timestamp': True, 'text_mandatory': 'Important text', 'text_optional': 'Less important text'
        }
        assert _output_dict['input']['files'] == {
            'input_data': {
                'file_name': 'Great Content', 'Great metadata entry': 'important file info'
            }
        }
        assert _output_dict['output_files'][file_name]['model_name'] == model_name

    output_dict_by_ai_function = [
        o_dict for o_dict in s.ai.get_output_files_by_ai_function(ai_function_to_test) if o_dict['run_id'] == run_id
    ][0]
    assert_output_dict(output_dict_by_ai_function)
    output_dict_by_model = [
        o_dict for o_dict in s.ai.get_output_files_by_model(model_name) if o_dict['run_id'] == run_id
    ][0]
    assert_output_dict(output_dict_by_model)


test_private_methods()
test_create_input_files()
test_execute_ai_function()

s.ai.show_available_ai_functions()
s.ai.show_ai_function_parameters(ai_function_to_test)
s.ai.show_available_models()
s.ai.show_available_input_files()
print(json.dumps(s.ai.get_output_files_by_ai_function(ai_function_to_test), indent=4))
s.ai.show_last_execution_logs_by_ai_function(ai_function_to_test)
s.ai.show_last_execution_logs_by_model(model_name)
