import asyncio
import os
import json
from copy import copy, deepcopy
from typing import Optional, List, Tuple, Dict
import shimoku_api_python as shimoku
import pandas as pd

from bs4 import BeautifulSoup

from shimoku_api_python.api.plot_api import PlotApi
from shimoku_api_python.resources.data_set import DataSet
from shimoku_api_python.resources.report import Report
from shimoku_api_python.resources.reports.modal import Modal
from shimoku_api_python.resources.reports.tabs_group import TabsGroup
from shimoku_api_python.async_execution_pool import async_auto_call_manager
from shimoku_api_python.utils import revert_uuids_from_dict

import logging
from shimoku_api_python.execution_logger import logging_before_and_after, log_error

logger = logging.getLogger(__name__)


shared_data_sets = []
output_path = ''

async def tree_from_tabs_group(
        self: PlotApi, tree: list, tabs_group: TabsGroup, seen_reports: set, parent_tabs_index: Tuple[str, str] = None
):
    """ Recursively build a tree of reports from a tabs group.
    :param self: PlotApi instance
    :param tree: list to append to
    :param tabs_group: tabs group to build tree from
    :param seen_reports: set of report ids that have already been seen
    :param parent_tabs_index: tuple of (tabs_group hash, tab name) of parent tabs group
    """
    tabs_group_dict = {'tabs_group': tabs_group, 'tabs': {}, 'order': tabs_group['order'],
                       'parent_tabs_index': parent_tabs_index}
    tree.append(tabs_group_dict)
    tabs = sorted(tabs_group['properties']['tabs'].items(), key=lambda x: x[1]['order'])
    for tab, tab_data in tabs:
        report_ids = tab_data['reportIds']
        tab_dict = {'tabs': [], 'other': []}
        tabs_group_dict['tabs'][tab] = tab_dict
        for child_id in report_ids:
            seen_reports.add(child_id)
            child_report = await self._app.get_report(child_id)
            if child_report['reportType'] == 'TABS':
                await tree_from_tabs_group(
                    self, tab_dict['tabs'], child_report, seen_reports, (tabs_group['properties']['hash'], tab))
            else:
                tab_dict['other'].append(child_report)
        tab_dict['tabs'] = sorted(tab_dict['tabs'], key=lambda x: x['tabs_group']['order'])
        tab_dict['other'] = sorted(tab_dict['other'], key=lambda x: x['order'])


async def tree_from_modal(self: PlotApi, tree: list, modal: Modal, seen_reports: set):
    """ Recursively build a tree of reports from a modal.
    :param self: PlotApi instance
    :param tree: list to append to
    :param modal: modal to build tree from
    :param seen_reports: set of report ids that have already been seen
    """
    modal_dict = {'modal': modal, 'tabs': [], 'other': []}
    tree.append(modal_dict)
    for child_id in modal['properties']['reportIds']:
        seen_reports.add(child_id)
        child_report = await self._app.get_report(child_id)
        if child_report['reportType'] == 'TABS':
            await tree_from_tabs_group(self, modal_dict['tabs'], child_report, seen_reports)
        else:
            modal_dict['other'].append(child_report)
    modal_dict['tabs'] = sorted(modal_dict['tabs'], key=lambda x: x['tabs_group']['order'])
    modal_dict['other'] = sorted(modal_dict['other'], key=lambda x: x['order'])


async def generate_tree(self: PlotApi, reports: list[Report]) -> dict:
    """ Generate a tree of reports from a list of reports.
    :param self: PlotApi instance
    :param reports: list of reports to build tree from
    :return: tree of reports
    """
    reports_tree = {}
    seen_reports = set()
    for report in reports:
        if report['id'] in seen_reports:
            continue

        if report['path'] not in reports_tree:
            reports_tree[report['path']] = {'modals': [], 'tabs': [], 'other': []}

        if report['reportType'] == 'MODAL':
            await tree_from_modal(self, reports_tree[report['path']]['modals'], report, seen_reports)
        elif report['reportType'] == 'TABS':
            await tree_from_tabs_group(self, reports_tree[report['path']]['tabs'], report, seen_reports)
        else:
            reports_tree[report['path']]['other'].append(report)

    for path in reports_tree:
        reports_tree[path]['other'] = sorted(reports_tree[path]['other'], key=lambda x: x['order'])
        reports_tree[path]['tabs'] = sorted(reports_tree[path]['tabs'], key=lambda x: x['tabs_group']['order'])
    # Todo: Solve path ordering
    # reports_tree = {k: v for k, v in sorted(reports.items(), key=lambda item: }

    return reports_tree


@logging_before_and_after(logger.debug)
async def check_for_shared_data_sets(self: PlotApi, report: Report, seen_data_sets: set):
    """ Check for shared data sets in a report.
    :param report: report to check
    :param seen_data_sets: set of data sets already seen
    """
    report_data_sets: List[Report.ReportDataSet] = await report.get_report_data_sets()
    data_sets_from_rds = set([rds['dataSetId'] for rds in report_data_sets])

    for ds_id in data_sets_from_rds:
        if ds_id in shared_data_sets:
            continue
        if ds_id in seen_data_sets:
            shared_data_sets.append(ds_id)
        else:
            seen_data_sets.add(ds_id)


@logging_before_and_after(logger.debug)
async def get_data_sets(self: PlotApi):
    """ Create files for the data sets.
    """
    reports = await self._app.get_reports()

    # To store the data sets in the cache for the reports to have faster access to them
    await self._app.get_data_sets()

    individual_data_sets: List[Tuple[DataSet, Report]] = []
    seen_data_sets = set()

    await asyncio.gather(*[check_for_shared_data_sets(self, report, seen_data_sets) for report in reports])

    for ds in shared_data_sets:
        await create_data_set_file(await self._app.get_data_set(ds))
    for ds, report in individual_data_sets:
        await create_data_set_file(ds, report)


@logging_before_and_after(logger.debug)
def change_data_set_name_with_report(data_set: DataSet, report: Report):
    """ Change the name of a data set to include the report name.
    :param data_set: data set to change name for
    :param report: report to change name for
    """
    return data_set["name"].replace(report['id'], report['properties']['hash'])


def print_list(l, deep=0):
    print(' ' * deep, '[')
    deep += 2
    for element in l:
        if isinstance(element, dict):
            print_dict(element, deep)
        elif isinstance(element, list):
            print_list(element, deep)
        else:
            print(' ' * deep, f'{str(element)},')
    deep -= 2
    print(' ' * deep, '],')


def print_dict(d, deep=0):
    print(' ' * deep, '{')
    deep += 2
    for k, v in d.items():
        if isinstance(v, (dict, list)):
            print(' ' * deep, f'{k}:')
            if isinstance(v, dict):
                print_dict(v, deep)
            elif isinstance(v, list):
                print_list(v, deep)
        else:
            print(' ' * deep, f'{k}:', f'{str(v)},')
    deep -= 2
    print(' ' * deep, '},')


def code_gen_from_list(l, deep=0):
    code_lines = [' ' * deep + '[']
    deep += 4
    for element in l:
        if isinstance(element, dict):
            code_lines.extend(code_gen_from_dict(element, deep))
        elif isinstance(element, list):
            code_lines.extend(code_gen_from_list(element, deep))
        else:
            code_lines.append(' ' * deep + f'"{str(element)}",')
    deep -= 4
    code_lines.append(' ' * deep + '],')
    return code_lines


def code_gen_from_dict(d, deep=0):
    code_lines = [' ' * deep + '{']
    deep += 4
    for k, v in d.items():
        if isinstance(v, (dict, list)):
            code_lines.append(' ' * deep + f'"{k}":')
            if isinstance(v, dict):
                code_lines.extend(code_gen_from_dict(v, deep))
            elif isinstance(v, list):
                code_lines.extend(code_gen_from_list(v, deep))
        else:
            code_lines.append(' ' * deep + f'"{k}": ' + f'"{str(v)}",')
    deep -= 4
    code_lines.append(' ' * deep + '},')
    return code_lines


def delete_default_properties(properties: dict, default_properties: dict) -> dict:
    """ Delete default properties from a report.
    :param properties: properties of a report
    :param default_properties: default properties of a report
    :return: properties without default properties
    """
    properties = copy(properties)
    for key, value in default_properties.items():
        if properties[key] == value:
            del properties[key]
        if isinstance(value, dict):
            if key not in properties:
                continue
            properties[key] = delete_default_properties(properties[key], value)
            if len(properties[key]) == 0:
                del properties[key]
    return properties


async def get_linked_data_set_info(
        self: PlotApi, report: Report
) -> Tuple[Dict[str, DataSet], List[Tuple[str, str]]]:
    rds: List[Report.ReportDataSet] = await report.get_report_data_sets()
    referenced_data_sets = {d_id: await self._app.get_data_set(d_id) for d_id in set([rd['dataSetId'] for rd in rds])}
    mappings = [(rd['dataSetId'], rd['properties']['mapping']) for rd in rds]
    return referenced_data_sets, mappings


async def code_gen_from_indicator(
        self: PlotApi, report: Report, report_params: List[str], properties: dict
) -> List[str]:
    """ Generate code for an indicator report.
    :param report_params: parameters of the report
    :param properties: properties of the report
    :return: list of code lines
    """
    return [
        's.plt.indicator(',
        *report_params,
        '    data=dict(',
        *[f'        {k}="{v}",' for k, v in properties.items() if v is not None],
        '    )',
        ')'
    ]


async def code_gen_from_echarts(
        self: PlotApi, report: Report, report_params: List[str], properties: dict
) -> List[str]:
    """ Generate code for an echarts report.
    :param report_params: parameters of the report
    :param properties: properties of the report
    :return: list of code lines
    """
    referenced_data_sets, mappings = await get_linked_data_set_info(self, report)
    echart_options = deepcopy(properties['option'])
    revert_uuids_from_dict(echart_options)
    if len(referenced_data_sets) > 1:
        log_error(logger,
                  'Only one data set is supported for the current implementation of the echarts component.',
                  RuntimeError)
    fields = [mapping[1] for mapping in mappings]
    data_set_id, data_set = list(referenced_data_sets.items())[0]

    data_arg = f'pd.read_csv("{output_path}/data/{change_data_set_name_with_report(data_set, report)}.csv")'
    if data_set_id in shared_data_sets:
        data_arg = f'"{data_set["name"]}"'
    return [
        's.plt.free_echarts(',
        *report_params,
        f'    data={data_arg},',
        f'    fields={fields},',
        '    options=',
        *code_gen_from_dict(echart_options, 4),
        ')'
    ]


async def code_gen_from_annotated_echart(
        self: PlotApi, report: Report, report_params: List[str], properties: dict
) -> List[str]:
    """ Generate code for an echarts report.
    :param report_params: parameters of the report
    :param properties: properties of the report
    :return: list of code lines
    """
    return [
        's.plt.annotated_chart(',
        *report_params,
        ')'
    ]


async def code_gen_from_table(
        self: PlotApi, report: Report, report_params: List[str], properties: dict
) -> List[str]:
    """ Generate code for a table report.
    :param report_params: parameters of the report
    :param properties: properties of the report
    :return: list of code lines
    """
    return [
        's.plt.table(',
        *report_params,
        ')'
    ]


async def code_gen_from_form(
        self: PlotApi, report: Report, report_params: List[str], properties: dict
) -> List[str]:
    """ Generate code for a form report.
    :param report_params: parameters of the report
    :param properties: properties of the report
    :return: list of code lines
    """
    return [
        's.plt.input_form(',
        *report_params,
        # "    options=",
        # *code_gen_from_dict(properties['options'], 4),
        ')'
    ]


async def code_gen_from_html(
        self: PlotApi, report: Report, report_params: List[str], chartData: dict
) -> List[str]:
    """ Generate code for an html report.
    :param report_params: parameters of the report
    :param chartData: chartData of the report where the html is stored
    :return: list of code lines
    """
    html = BeautifulSoup(chartData[0]["value"].replace("'", "\\'").replace('"', '\\"'),
                         "html.parser").prettify().replace("\n", "'\n" + " " * 8 + "'")
    return [
        's.plt.html(',
        *report_params,
        f"    html=(",
        f"        '{html}'",
        f"    )",
        ')'
    ]


async def code_gen_from_iframe(
        self: PlotApi, report: Report
) -> List[str]:
    """ Generate code for an iframe report.
    :param report: report to generate code from
    :return: list of code lines
    """
    code_lines = [
        's.plt.iframe(',
        f'    order="{report["order"]}",',
        f'    url="{report["dataFields"]["url"]}",',
    ]
    if report['dataFields']['height'] != 640:
        code_lines.append(f'    height={report["dataFields"]["height"]},')
    if report['sizeColumns']:
        code_lines.append(f'    cols_size={report["sizeColumns"]},')
    if report['sizePadding']:
        code_lines.append(f'    padding="{report["sizePadding"]}",')
    code_lines.append(')')
    return code_lines


async def code_gen_from_other(
        self: PlotApi, report: Report
) -> List[str]:
    """ Generate code for a report that is not a tabs group.
    :param report: report to generate code from
    :return: list of code lines
    """
    code_lines = []

    properties = delete_default_properties(report['properties'], report.default_properties)
    del properties['hash']

    report_params_to_get = {
        'order': 'order', 'title': 'title',
        'sizeColumns': 'cols_size', 'sizeRows': 'rows_size',
        'padding': 'padding',
    }
    report_params = [f'    {report_params_to_get[k]}=' + (f'"{(report[k])}",'
                                                          if isinstance(report[k], str) else f'{report[k]},')
                     for k in report if k in report_params_to_get]

    if report['reportType'] == 'INDICATOR':
        code_lines.extend(await code_gen_from_indicator(self, report, report_params, properties))
    elif report['reportType'] == 'ECHARTS2':
        code_lines.extend(await code_gen_from_echarts(self, report, report_params, properties))
    elif report['reportType'] == 'TABLE':
        code_lines.extend(await code_gen_from_table(self, report, report_params, properties))
    elif report['reportType'] == 'FORM':
        code_lines.extend(await code_gen_from_form(self, report, report_params, properties))
    elif report['reportType'] == 'HTML':
        code_lines.extend(await code_gen_from_html(self, report, report_params, report['chartData']))
    elif report['reportType'] == 'IFRAME':
        code_lines.extend(await code_gen_from_iframe(self, report))
    elif report['reportType'] == 'ANNOTATED_ECHART':
        code_lines.extend(await code_gen_from_annotated_echart(self, report, report_params, properties))
    else:
        code_lines.extend([f"s.add_report({report['reportType']}, order={report['order']}, data=dict())"])
    return code_lines


async def code_gen_from_tabs_group(self: PlotApi, tree: dict) -> List[str]:
    """ Generate code for a tabs group.
    :param tree: tree of reports
    :param code_lines: list of code lines
    """
    code_lines = []
    tabs_group: TabsGroup = tree['tabs_group']
    code_lines.extend([
        '',
        's.plt.set_tabs_index(',
        f'    tabs_index=("{tabs_group["properties"]["hash"]}", "{list(tree["tabs"].keys())[0]}"), order={tabs_group["order"]}, ',
    ])
    if tree['parent_tabs_index']:
        code_lines.extend([f'    parent_tabs_index={tree["parent_tabs_index"]}'])
    code_lines.extend([')'])

    for tab in tree['tabs']:
        code_lines.extend(['', f's.plt.change_current_tab("{tab}")'])
        code_lines.extend(await code_gen_tabs_and_other(self, tree['tabs'][tab]))

    return code_lines


async def code_gen_tabs_and_other(self: PlotApi, tree: dict) -> List[str]:
    """ Generate code for tabs and other components.
    :param tree: tree of reports
    :return: list of code lines
    """
    code_lines: List[str] = []
    components_ordered = sorted(tree['other'] + tree['tabs'], key=lambda x: x['order'])
    for component in components_ordered:
        if isinstance(component, dict):
            code_lines.extend(await code_gen_from_tabs_group(self, component))
        else:
            code_lines.extend(await code_gen_from_other(self, component))
    return code_lines


async def code_gen_from_modal(self: PlotApi, tree: dict) -> List[str]:
    """ Generate code for a modal.
    :param tree: tree of reports
    :return: list of code lines
    """
    code_lines = []
    modal: Modal = tree['modal']
    code_lines.extend(['', f's.plt.set_modal("{modal["properties"]["hash"]}")'])
    code_lines.extend(await code_gen_tabs_and_other(self, tree))
    return code_lines


async def code_gen_from_reports_tree(self: PlotApi, tree: dict, path: str) -> List[str]:
    code_lines = []
    for modal in tree[path]['modals']:
        code_lines.extend(await code_gen_from_modal(self, modal))
    if len(tree[path]['modals']) > 0:
        code_lines.extend(['', 's.plt.pop_out_of_modal()', ''])
    code_lines.extend(await code_gen_tabs_and_other(self, tree[path]))
    return code_lines


async def create_data_set_file(data_set: DataSet, report: Optional[Report] = None):
    """ Create a file for a data set.
    :param data_set: data set to create file for
    :param report: report to create file for
    """
    data: List[dict] = [{k: v for k, v in dp.cascade_to_dict().items()
                         if k not in ['id', 'dataSetId'] and v is not None}
                         for dp in await data_set.get_data_points()]
    data_as_df = pd.DataFrame(data)
    if not os.path.exists(output_path + '/data'):
        os.makedirs(output_path + '/data')

    output_name = data_set["name"] if report is None else change_data_set_name_with_report(data_set, report)
    data_as_df.to_csv(os.path.join(output_path + '/data', f'{output_name}.csv'), index=False)


async def code_gen_shared_data_sets(self: PlotApi) -> List[str]:
    """ Generate code for data sets that are shared between reports.
    :return: list of code lines
    """
    code_lines = []
    dfs: List[DataSet] = []
    custom: List[DataSet] = []
    for ds_id in shared_data_sets:
        ds = await self._app.get_data_set(ds_id)
        df = pd.read_csv(f'{output_path}/data/{ds["name"]}.csv')
        if 'customField1' in df.columns:
            custom.append(ds)
        else:
            dfs.append(ds)
    if len(dfs) > 0 or len(custom) > 0:
        code_lines.append("s.plt.set_shared_data(")

    if len(dfs) > 0:
        code_lines.extend([
            "    dfs={",
            *[f'        "{ds["name"]}": pd.read_csv("{output_path}/data/{ds["name"]}.csv"),' for ds in dfs],
            "    },",
        ])
    if len(custom) > 0:
        code_lines.extend([
            "    custom_data={",
            *[f'        "{ds["name"]}": pd.read_csv("{output_path}/data/{ds["name"]}.csv")["customField1"],'
              for ds in custom],
            "    },",
        ])

    if len(dfs) > 0 or len(custom) > 0:
        code_lines.append(")")

    return code_lines


@async_auto_call_manager(execute=True)
@logging_before_and_after(logger.info)
async def generate_code(self: PlotApi, file_name: Optional[str] = None):
    """ Use the resources in the API to generate code_lines for the SDK. Create a file in the specified path with the
    generated code_lines.
    :param file_name: The name of the file to create. If not specified, the name of the menu path will be used.
    """
    reports = sorted(
        await self._app.get_reports(),
        key=lambda x:
        '0' if x['reportType'] == 'MODAL' else
        '1' if x['reportType'] == 'TABS' else
        '_' + x['properties']['hash']
    )

    # print(str(self._app), [(t, self._app[t]) for t in self._app])
    reports_tree = await generate_tree(self, reports)
    await get_data_sets(self)

    # reports_per_type = {}
    # for report in reports:
    #     if report['reportType'] not in reports_per_type:
    #         reports_per_type[report['reportType']] = []
    #     reports_per_type[report['reportType']].append(report)
    #     print(str(report), [(t, report[t]) for t in report])
    # print_dict(reports_tree)
    print('CODE:')
    code_lines: List[str] = []
    # TODO: Put on external function
    code_lines = [
        'import shimoku_api_python as shimoku',
        'import pandas as pd',
        '',
        's = shimoku.Client(',
        '    access_token="",',
        '    universe_id="",',
        '    verbosity="INFO",',
        ')',
        '',
        's.set_workspace()',
        '',
        's.set_menu_path()',
        '',

    ]
    code_lines.extend(await code_gen_shared_data_sets(self))
    for path in reports_tree:
        code_lines.extend(
            ['', f's.set_menu_path("{self._app["name"]}"' + (f', "{path}")' if path is not None else ')')])
        code_lines.extend(await code_gen_from_reports_tree(self, reports_tree, path))

        # print_dict(reports_tree[path])

    # print('\n'.join(code_lines), '\n')
    if file_name is None:
        file_name = self._app['name']

    with open(os.path.join(output_path, file_name + '.py'), 'w') as f:
        f.write('\n'.join(code_lines))


s = shimoku.Client(
    access_token='',
    universe_id='5c22ba15-c32d-4f4f-9f3d-7c2d331a87a4',
    verbosity='INFO',
    environment='local',
    async_execution=True,
    local_port=8080,
)
s.set_workspace('34b9c913-ba02-47cf-a9cf-cdefb17f8b03')
print([app['name'] for app in s.workspaces.get_workspace_menu_paths(s.workspace_id)])
s.set_menu_path('test')

output_path = 'generated_code'
generate_code(s.plt)
