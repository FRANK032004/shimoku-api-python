import unittest
from test_plot_api import main, shimoku_client
import tempfile
import subprocess
import shutil
import json


def get_report_data_set_info(r_hash: str, report_data_set: dict, data_sets: dict[str, str]) -> dict:
    report_data_set.pop('id')
    report_id = report_data_set.pop('reportId')
    report_data_set['dataSetId'] = data_sets[report_data_set['dataSetId']].replace(report_id, r_hash)
    return report_data_set


def get_report_data_set_info_dict(r_hash: str, _dict: dict, report_data_sets: dict[str, dict], data_sets: dict[str, str]):
    for k, v in _dict.items():
        if isinstance(v, dict):
            get_report_data_set_info_dict(r_hash, v, report_data_sets, data_sets)
        elif isinstance(v, list):
            get_report_data_set_info_list(r_hash, v, report_data_sets, data_sets)
        elif isinstance(v, str) and v.startswith('#{') and v.endswith('}'):
            uuid = v[2:-1]
            _dict[k] = get_report_data_set_info(r_hash, report_data_sets[uuid], data_sets)


def get_report_data_set_info_list(r_hash: str, _list: list, report_data_sets: dict[str, dict], data_sets: dict[str, str]):
    for i, v in enumerate(_list):
        if isinstance(v, dict):
            get_report_data_set_info_dict(r_hash, v, report_data_sets, data_sets)
        elif isinstance(v, list):
            get_report_data_set_info_list(r_hash, v, report_data_sets, data_sets)
        elif isinstance(v, str) and v.startswith('#{') and v.endswith('}'):
            uuid = v[2:-1]
            _list[i] = get_report_data_set_info(r_hash, report_data_sets[uuid], data_sets)


def clear_workspace():
    for menu_path in shimoku_client.workspaces.get_workspace_menu_paths():
        shimoku_client.menu_paths.delete_all_menu_path_activities(uuid=menu_path['id'])
    shimoku_client.workspaces.delete_all_workspace_menu_paths()
    shimoku_client.workspaces.delete_all_workspace_boards()


def get_workspace_contents():
    boards = sorted(shimoku_client.workspaces.get_workspace_boards(), key=lambda x: x['order'])
    menu_paths = sorted(shimoku_client.workspaces.get_workspace_menu_paths(), key=lambda x: x['order'])
    components = []
    data_sets = {}
    for menu_path in menu_paths:
        components.extend(sorted(shimoku_client.menu_paths.get_menu_path_components(menu_path['id']),
                                 key=lambda x: x['properties']['hash']))
        for data_set in shimoku_client.menu_paths.get_menu_path_data_sets(menu_path['id']):
            data_sets[data_set['id']] = data_set['name']
    for component in components:
        data_set_links = shimoku_client.components.get_component_data_set_links(component['id'])
        get_report_data_set_info_dict(
            component['properties']['hash'], component['properties'],
            {rds['id']: rds for rds in data_set_links}, data_sets
        )
    _all = [*boards, *menu_paths, *components]
    for element in _all:
        element.pop('id')
    return boards, menu_paths, components


class TestCodeGen(unittest.TestCase):

    def test_code_gen(self):
        if not shimoku_client.playground:
            return
        shimoku_client.set_workspace()
        clear_workspace()
        main()
        shimoku_client.pop_out_of_dashboard()
        shimoku_client.disable_caching()
        boards, menu_paths, components = get_workspace_contents()
        temp_dir = tempfile.mkdtemp()
        shimoku_client.generate_code(temp_dir)
        clear_workspace()
        subprocess.run(['python', f'{temp_dir}/execute_workspace_local.py'], check=True)
        first_generation_boards, first_generation_menu_paths, first_generation_components = get_workspace_contents()
        shutil.rmtree(temp_dir)
        temp_dir = tempfile.mkdtemp()
        shimoku_client.generate_code(temp_dir)
        subprocess.run(['python', f'{temp_dir}/execute_workspace_local.py'], check=True)
        second_generation_boards, second_generation_menu_paths, second_generation_components = get_workspace_contents()
        clear_workspace()
        shutil.rmtree(temp_dir)
        results = {
            'orig-first': (boards == first_generation_boards, menu_paths == first_generation_menu_paths, components == first_generation_components),
            'orig-second': (boards == second_generation_boards, menu_paths == second_generation_menu_paths, components == second_generation_components),
            'first-second': (first_generation_boards == second_generation_boards, first_generation_menu_paths == second_generation_menu_paths, first_generation_components == second_generation_components),
        }
        print(json.dumps(components[1], indent=4))
        print(json.dumps(first_generation_components[1], indent=4))
        print(json.dumps(results, indent=4))
        self.assertTrue(all(all(v) for v in results.values()))
