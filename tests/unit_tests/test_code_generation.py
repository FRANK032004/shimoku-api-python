import unittest
from test_plot_api import main, shimoku_client
import tempfile
import subprocess
import shutil
import json


def get_report_data_set_info(
    r_hash: str, report_data_set: dict, data_sets_mapping: dict[str, str]
) -> dict:
    report_data_set.pop('id')
    report_id = report_data_set.pop('reportId')
    report_data_set['dataSetId'] = data_sets_mapping[report_data_set['dataSetId']].replace(report_id, r_hash)
    return report_data_set


def get_report_data_set_info_dict(
    r_hash: str, _dict: dict, report_data_sets: dict[str, dict], data_sets_mapping: dict[str, str]
):
    for k, v in _dict.items():
        if isinstance(v, dict):
            get_report_data_set_info_dict(r_hash, v, report_data_sets, data_sets_mapping)
        elif isinstance(v, list):
            get_report_data_set_info_list(r_hash, v, report_data_sets, data_sets_mapping)
        elif isinstance(v, str) and v.startswith('#{') and v.endswith('}'):
            uuid = v[2:-1]
            _dict[k] = get_report_data_set_info(r_hash, report_data_sets[uuid], data_sets_mapping)


def get_report_data_set_info_list(
    r_hash: str, _list: list, report_data_sets: dict[str, dict], data_sets_mapping: dict[str, str]
):
    for i, v in enumerate(_list):
        if isinstance(v, dict):
            get_report_data_set_info_dict(r_hash, v, report_data_sets, data_sets_mapping)
        elif isinstance(v, list):
            get_report_data_set_info_list(r_hash, v, report_data_sets, data_sets_mapping)
        elif isinstance(v, str) and v.startswith('#{') and v.endswith('}'):
            uuid = v[2:-1]
            _list[i] = get_report_data_set_info(r_hash, report_data_sets[uuid], data_sets_mapping)


def clear_workspace():
    for menu_path in shimoku_client.workspaces.get_workspace_menu_paths():
        shimoku_client.menu_paths.delete_all_menu_path_activities(uuid=menu_path['id'])
    shimoku_client.workspaces.delete_all_workspace_menu_paths()
    shimoku_client.workspaces.delete_all_workspace_boards()


def change_ids_in_tabs_or_modal(component, components_by_id):
    if 'tabs' in component['properties']:
        for tab_name, tab_dict in component['properties']['tabs'].items():
            component['properties']['tabs'][tab_name]['reportIds'] = sorted([
                components_by_id[r_id]['properties']['hash'] for r_id in tab_dict['reportIds']
            ])
    if 'reportIds' in component['properties']:
        component['properties']['reportIds'] = sorted([
            components_by_id[r_id]['properties']['hash'] for r_id in component['properties']['reportIds']
            ])


def handle_events(events, components_by_id):
    on_click_events = events.get('onClick', [])
    for event in on_click_events:
        if 'modalId' in event['params']:
            event['params']['modalId'] = components_by_id[event['params']['modalId']]['properties']['hash']


def handle_bentobox(component, seen_bentobox):
    # Order of creation is not important for bentoboxes, so we get the first one always
    if component['bentobox']['bentoboxId'] in seen_bentobox:
        component['bentobox'] = seen_bentobox[component['bentobox']['bentoboxId']]
    else:
        bentobox = component['bentobox']
        prev_bentobox_id = bentobox['bentoboxId']
        bentobox.update({'bentoboxId': f'_{component["order"]}', 'bentoboxOrder': component['order']})
        seen_bentobox[prev_bentobox_id] = bentobox


def handle_component(component, mpath_components_by_id, seen_bentobox, data_sets_mapping):
    data_set_links = shimoku_client.components.get_component_data_set_links(component['id'])
    get_report_data_set_info_dict(
        component['properties']['hash'], component['properties'],
        {rds['id']: rds for rds in data_set_links}, data_sets_mapping
    )
    if component.get('bentobox'):
        handle_bentobox(component, seen_bentobox)
    if 'events' in component['properties']:
        handle_events(component['properties']['events'], mpath_components_by_id)
    if component['reportType'] in ['MODAL', 'TABS']:
        change_ids_in_tabs_or_modal(component, mpath_components_by_id)
    elif component['reportType'] == 'FILTERDATASET':
        fds_mapping = component['properties']['mapping']
        fds_mapping[0]['id'] = data_sets_mapping[fds_mapping[0]['id']]


def handle_menu_path(menu_path, components, data_sets_mapping):
    mpath_components = sorted(shimoku_client.menu_paths.get_menu_path_components(menu_path['id']),
                              key=lambda x: x['properties']['hash'])
    components.extend(mpath_components)
    for data_set in shimoku_client.menu_paths.get_menu_path_data_sets(menu_path['id']):
        data_sets_mapping[data_set['id']] = data_set['name']
    shimoku_client.set_menu_path(menu_path['name'], dont_add_to_dashboard=True)
    mpath_components_by_id = {c['id']: c for c in mpath_components}
    seen_bentobox = {}
    for component in mpath_components:
        handle_component(component, mpath_components_by_id, seen_bentobox, data_sets_mapping)


def get_diff_percentage(items1, items2):
    return sum(c1 != c2 for c1, c2 in zip(items1, items2))/len(items1)


def get_workspace_contents():
    boards = sorted(shimoku_client.workspaces.get_workspace_boards(), key=lambda x: x['order'])
    menu_paths = sorted(shimoku_client.workspaces.get_workspace_menu_paths(), key=lambda x: x['order'])

    components = []
    data_sets_mapping = {}
    for menu_path in menu_paths:
        handle_menu_path(menu_path, components, data_sets_mapping)

    shimoku_client.pop_out_of_menu_path()
    _all = [*boards, *menu_paths, *components]
    for element in _all:
        element.pop('id')
    return boards, menu_paths, components


class TestCodeGen(unittest.TestCase):

    def test_code_gen(self):
        if not shimoku_client.playground:
            # It would take too long to complete the test
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
        shimoku_client.run()
        shutil.rmtree(temp_dir)
        results = {
            'orig-first': (get_diff_percentage(boards, first_generation_boards),
                           get_diff_percentage(menu_paths, first_generation_menu_paths),
                           get_diff_percentage(components, first_generation_components)),
            'orig-second': (get_diff_percentage(boards, second_generation_boards),
                            get_diff_percentage(menu_paths, second_generation_menu_paths),
                            get_diff_percentage(components, second_generation_components)),
            'first-second': (get_diff_percentage(first_generation_boards, second_generation_boards),
                             get_diff_percentage(first_generation_menu_paths, second_generation_menu_paths),
                             get_diff_percentage(first_generation_components, second_generation_components))
        }
        print(json.dumps(results, indent=4))
        self.assertTrue(all(all([value == 0 for value in results_list]) for results_list in results.values()))
