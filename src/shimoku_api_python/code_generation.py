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
from shimoku_api_python.utils import revert_uuids_from_dict, create_normalized_name

import logging
from shimoku_api_python.execution_logger import logging_before_and_after, log_error

logger = logging.getLogger(__name__)


shared_data_sets = []
custom_data_sets_with_data = {}
output_path = ''
actual_bentobox: Optional[Dict] = None


def create_function_name(name: Optional[str]) -> str:
    """ Create a valid function name from a string
    :param name: string to create function name from
    :return: valid function name
    """
    if name is None:
        return 'no_path'
    return create_normalized_name(name).replace('-', '_')


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
async def check_for_shared_data_sets(
    self: PlotApi, report: Report, seen_data_sets: set, individual_data_sets: Dict[str, Report]
):
    """ Check for shared data sets in a report.
    :param report: report to check
    :param seen_data_sets: set of data sets already seen
    :param individual_data_sets: list of data sets to append to
    """
    report_data_sets: List[Report.ReportDataSet] = await report.get_report_data_sets()
    data_sets_from_rds = set([rds['dataSetId'] for rds in report_data_sets])

    for ds_id in data_sets_from_rds:
        if ds_id in shared_data_sets:
            continue
        if ds_id in seen_data_sets:
            shared_data_sets.append(ds_id)
            del individual_data_sets[ds_id]
        else:
            individual_data_sets[ds_id] = report
            seen_data_sets.add(ds_id)


@logging_before_and_after(logger.debug)
async def get_data_sets(self: PlotApi):
    """ Create files for the data sets.
    """
    reports = await self._app.get_reports()

    # To store the data sets in the cache for the reports to have faster access to them
    await self._app.get_data_sets()

    individual_data_sets: Dict[str, Report] = {}
    seen_data_sets = set()

    await asyncio.gather(*[check_for_shared_data_sets(self, report, seen_data_sets, individual_data_sets)
                           for report in reports])

    for ds_id in shared_data_sets:
        await create_data_set_file(await self._app.get_data_set(ds_id))
    for ds_id, report in individual_data_sets.items():
        await create_data_set_file(await self._app.get_data_set(ds_id), report)


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


def code_gen_value(v):
    return v if not isinstance(v, str) else f'"{v}"'


def code_gen_from_list(l, deep=0):
    code_lines = [' ' * deep + '[']
    deep += 4
    for element in l:
        if isinstance(element, dict):
            code_lines.extend(code_gen_from_dict(element, deep))
        elif isinstance(element, list):
            code_lines.extend(code_gen_from_list(element, deep))
        else:
            code_lines.append(' ' * deep + f'{code_gen_value(element)},')
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
            code_lines.append(' ' * deep + f'"{k}": ' + f'{code_gen_value(v)},')
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
        self: PlotApi, report: Report, rds_ids_in_order: List[str]
) -> Tuple[Dict[str, DataSet], List[Tuple[str, str]]]:
    unordered_rds: List[Report.ReportDataSet] = await report.get_report_data_sets()
    rds: List[Report.ReportDataSet] = []
    for rds_id in rds_ids_in_order:
        rds.append(next(rd for rd in unordered_rds if rd['id'] == rds_id))
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
        'shimoku_client.plt.indicator(',
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
    echart_options = deepcopy(properties['option'])
    rds_ids_in_order = revert_uuids_from_dict(echart_options)
    referenced_data_sets, mappings = await get_linked_data_set_info(self, report, rds_ids_in_order)
    if len(referenced_data_sets) > 1:
        log_error(logger,
                  'Only one data set is supported for the current implementation of the echarts component.',
                  RuntimeError)
    fields = [mapping[1] for mapping in mappings]
    data_set_id, data_set = list(referenced_data_sets.items())[0]

    if data_set_id in shared_data_sets:
        if data_set_id in custom_data_sets_with_data:
            return []
        data_arg = [f'"{data_set["name"]}",']
    elif data_set_id in custom_data_sets_with_data:
        val = custom_data_sets_with_data[data_set_id]
        data_arg = code_gen_from_dict(val, 4) if isinstance(val, dict) else code_gen_from_list(val, 4)
        data_arg[0] = data_arg[0][4:]
        data_arg += ['    data_is_not_df=True,']
        fields = '["data"]'
    else:
        data_arg = [f'pd.read_csv("data/{change_data_set_name_with_report(data_set, report)}.csv"),']

    options_code = code_gen_from_dict(echart_options, 4)

    return [
        'shimoku_client.plt.free_echarts(',
        *report_params,
        f'    data={data_arg[0]}',
        *data_arg[1:],
        f'    fields={fields},',
        f'    options={options_code[0][4:]}',
        *options_code[1:],
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
    return []
    return [
        'shimoku_client.plt.annotated_chart(',
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
    return []
    return [
        'shimoku_client.plt.table(',
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
    return []
    return [
        'shimoku_client.plt.input_form(',
        *report_params,
        # "    options=",
        # *code_gen_from_dict(properties['options'], 4),
        ')'
    ]


async def code_gen_from_html(
        self: PlotApi, report: Report
) -> List[str]:
    """ Generate code for an html report.
    :param report_params: parameters of the report
    :param chartData: chartData of the report where the html is stored
    :return: list of code lines
    """
    html = BeautifulSoup(report['chartData'][0]["value"].replace("'", "\\'").replace('"', '\\"'),
                         "html.parser").prettify().replace("\n", "'\n" + " " * 9 + "'")
    code_lines = [
        'shimoku_client.plt.html(',
        f'    order={report["order"]},',
        f"    html='{html}',",
    ]
    if report['sizeColumns'] != 12:
        code_lines.append(f'    cols_size={report["sizeColumns"]},')
    if report['sizeRows']:
        code_lines.append(f'    rows_size={report["sizeRows"]},')
    if report['sizePadding'] != '0,0,0,0':
        code_lines.append(f'    padding="{report["sizePadding"]}",')
    code_lines.append(')')

    return code_lines


async def code_gen_from_button(
        self: PlotApi, report: Report
) -> List[str]:
    """ Generate code for a button report.
    :param report: report to generate code from
    :return: list of code lines
    """
    return []


async def code_gen_from_iframe(
        self: PlotApi, report: Report
) -> List[str]:
    """ Generate code for an iframe report.
    :param report: report to generate code from
    :return: list of code lines
    """
    code_lines = [
        'shimoku_client.plt.iframe(',
        f'    order={report["order"]},',
        f'    url="{report["dataFields"]["url"]}",',
    ]
    if report['dataFields']['height'] != 640:
        code_lines.append(f'    height={report["dataFields"]["height"]},')
    if report['sizeColumns'] != 12:
        code_lines.append(f'    cols_size={report["sizeColumns"]},')
    if report['sizePadding'] != '0,0,0,0':
        code_lines.append(f'    padding="{report["sizePadding"]}",')
    code_lines.append(')')
    return code_lines


async def code_gen_from_other(
        self: PlotApi, report: Report, is_last: bool
) -> List[str]:
    """ Generate code for a report that is not a tabs group.
    :param report: report to generate code from
    :return: list of code lines
    """
    global actual_bentobox
    code_lines = []

    properties = delete_default_properties(report['properties'], report.default_properties)
    del properties['hash']

    report_params_to_get = {
        'order': 'order', 'title': 'title',
        'sizeColumns': 'cols_size', 'sizeRows': 'rows_size',
        'sizePadding': 'padding',
    }
    report_params = [f'    {report_params_to_get[k]}=' + (f'"{(report[k])}",'
                                                          if isinstance(report[k], str) else f'{report[k]},')
                     for k in report if k in report_params_to_get]

    if len(report['bentobox']):
        if actual_bentobox is None or actual_bentobox['bentoboxId'] != report['bentobox']['bentoboxId']:
            actual_bentobox = report['bentobox']
            cols_size = actual_bentobox['bentoboxSizeColumns']
            rows_size = actual_bentobox['bentoboxSizeRows']
            code_lines.extend([
                '',
                f'shimoku_client.plt.set_bentobox(cols_size={cols_size}, rows_size={rows_size})'
            ])
    elif actual_bentobox is not None:
        actual_bentobox = None
        code_lines.append('shimoku_client.plt.pop_out_of_bentobox()')

    if report['reportType'] == 'INDICATOR':
        code_lines.extend(await code_gen_from_indicator(self, report, report_params, properties))
    elif report['reportType'] == 'ECHARTS2':
        code_lines.extend(await code_gen_from_echarts(self, report, report_params, properties))
    elif report['reportType'] == 'TABLE':
        code_lines.extend(await code_gen_from_table(self, report, report_params, properties))
    elif report['reportType'] == 'FORM':
        code_lines.extend(await code_gen_from_form(self, report, report_params, properties))
    elif report['reportType'] == 'HTML':
        code_lines.extend(await code_gen_from_html(self, report))
    elif report['reportType'] == 'IFRAME':
        code_lines.extend(await code_gen_from_iframe(self, report))
    elif report['reportType'] == 'ANNOTATED_ECHART':
        code_lines.extend(await code_gen_from_annotated_echart(self, report, report_params, properties))
    elif report['reportType'] == 'BUTTON':
        code_lines.extend(await code_gen_from_button(self, report))
    else:
        code_lines.extend([f"shimoku_client.add_report({report['reportType']}, order={report['order']}, data=dict())"])

    if is_last and actual_bentobox is not None:
        actual_bentobox = None
        code_lines.append('shimoku_client.plt.pop_out_of_bentobox()')

    return code_lines


async def code_gen_from_tabs_group(
    self: PlotApi, tree: dict, is_last: bool = False
) -> List[str]:
    """ Generate code for a tabs group.
    :param tree: tree of reports
    :param is_last: whether the tabs group is the last one
    :return: list of code lines
    """
    code_lines = []
    tabs_group: TabsGroup = tree['tabs_group']
    tabs_index = (tabs_group['properties']['hash'], list(tree['tabs'].keys())[0])
    parent_tabs_index = tree['parent_tabs_index']
    properties = delete_default_properties(tabs_group['properties'], TabsGroup.default_properties)
    del properties['hash']
    if 'tabs' in properties:
        del properties['tabs']
    if 'variant' in properties:
        properties['just_labels'] = True
        del properties['variant']

    code_lines.extend([
        '',
        'shimoku_client.plt.set_tabs_index(',
        f'    tabs_index=("{tabs_index[0]}", "{tabs_index[1]}"), order={tabs_group["order"]}, ',
    ])
    if parent_tabs_index:
        code_lines.extend([f'    parent_tabs_index={parent_tabs_index},'])
    code_lines.extend([f'    {k}={code_gen_value(v)},' for k, v in properties.items()])
    code_lines.extend([')'])

    for tab in tree['tabs']:
        code_lines.extend(['', f'shimoku_client.plt.change_current_tab("{tab}")'])
        code_lines.extend(await code_gen_tabs_and_other(self, tree['tabs'][tab],
                                                        last_tab=tab == list(tree['tabs'])[-1]))

    if parent_tabs_index:
        if not is_last:
            code_lines.extend([
                '',
                f'shimoku_client.plt.set_tabs_index(("{parent_tabs_index[0]}", "{parent_tabs_index[1]}"))'
            ])

    else:
        code_lines.extend(['', 'shimoku_client.plt.pop_out_of_tabs_group()'])

    return code_lines


async def code_gen_tabs_and_other(
    self: PlotApi, tree: dict, last_tab: bool = False
) -> List[str]:
    """ Generate code for tabs and other components.
    :param tree: tree of reports
    :param parent_tabs_index: parent tabs index
    :return: list of code lines
    """
    code_lines: List[str] = []
    components_ordered = sorted(tree['other'] + tree['tabs'], key=lambda x: x['order'])
    for i, component in enumerate(components_ordered):
        if isinstance(component, dict):
            code_lines.extend(
                await code_gen_from_tabs_group(
                    self, component, is_last=last_tab and i == len(components_ordered) - 1
                )
            )
        else:
            code_lines.extend(await code_gen_from_other(self, component, is_last=i == len(components_ordered) - 1))
    return code_lines


async def code_gen_from_modal(self: PlotApi, tree: dict) -> List[str]:
    """ Generate code for a modal.
    :param tree: tree of reports
    :return: list of code lines
    """
    code_lines = []
    modal: Modal = tree['modal']
    properties = delete_default_properties(modal['properties'], Modal.default_properties)
    properties['modal_name'] = properties['hash']
    del properties['hash']
    if 'reportIds' in properties:
        del properties['reportIds']
    if 'open' in properties:
        del properties['open']
        properties['open_by_default'] = True
    code_lines.extend([
        'shimoku_client.plt.set_modal(',
        *[f'    {k}={code_gen_value(v)},' for k, v in properties.items()],
        ')',
    ])
    code_lines.extend(await code_gen_tabs_and_other(self, tree))
    return code_lines


async def code_gen_from_reports_tree(self: PlotApi, tree: dict, path: str) -> List[str]:
    code_lines = []
    for modal in tree[path]['modals']:
        modal_code_lines = await code_gen_from_modal(self, modal)
        code_lines.extend([
            '',
            f'def modal_{create_function_name(modal["modal"]["properties"]["hash"])}():',
            *['    ' + line for line in modal_code_lines]
        ])
    for modal in tree[path]['modals']:
        code_lines.extend(['', f'modal_{create_function_name(modal["modal"]["properties"]["hash"])}()'])
    if len(tree[path]['modals']) > 0:
        code_lines.extend(['', 'shimoku_client.plt.pop_out_of_modal()', ''])
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

    if not os.path.exists(output_path + '/data'):
        os.makedirs(output_path + '/data')

    if len(data) == 0:
        return

    if len(data) > 1 or 'customField1' not in data[0]:
        data_as_df = pd.DataFrame(data)
        output_name = data_set["name"] if report is None else change_data_set_name_with_report(data_set, report)
        data_as_df.to_csv(os.path.join(output_path + '/data', f'{output_name}.csv'), index=False)
    else:
        custom_data_sets_with_data[data_set['id']] = data[0]['customField1']


async def code_gen_shared_data_sets(self: PlotApi) -> List[str]:
    """ Generate code for data sets that are shared between reports.
    :return: list of code lines
    """
    code_lines = []
    dfs: List[DataSet] = []
    custom: List[DataSet] = []
    for ds_id in shared_data_sets:
        ds = await self._app.get_data_set(ds_id)
        if ds_id in custom_data_sets_with_data:
            custom.append(ds)
        else:
            dfs.append(ds)
    if len(dfs) > 0 or len(custom) > 0:
        code_lines.append("shimoku_client.plt.set_shared_data(")

    if len(dfs) > 0:
        code_lines.extend([
            "    dfs={",
            *[f'        "{ds["name"]}": pd.read_csv("data/{ds["name"]}.csv"),' for ds in dfs],
            "    },",
        ])
    if len(custom) > 0:
        code_lines.append('    custom_data={')
        for ds in custom:
            custom_data = custom_data_sets_with_data[ds["id"]]
            if isinstance(custom_data, dict):
                custom_data = code_gen_from_dict(custom_data, 8)
            else:
                custom_data = code_gen_from_list(custom_data, 8)

            code_lines.extend([
                f'        "{ds["name"]}": {custom_data[0][8:]}',
                *custom_data[1:],
            ])
        code_lines.append('    },')

    if len(dfs) > 0 or len(custom) > 0:
        code_lines.append(")")
        code_lines = [''] + code_lines

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
    ]
    shared_data_sets_code_lines = await code_gen_shared_data_sets(self)
    for path in reports_tree:
        code_lines.extend([
            '',
            '',
            f'def {create_function_name(path)}(shimoku_client: shimoku.Client):',
        ])
        function_code_lines = []
        function_code_lines.extend(
            [f'shimoku_client.set_menu_path("{self._app["name"]}"' + (f', "{path}")' if path is not None else ')')])
        function_code_lines.extend(await code_gen_from_reports_tree(self, reports_tree, path))
        code_lines.extend(['    ' + line for line in function_code_lines])
        # print_dict(reports_tree[path])

    function_calls_code_lines = [f'{create_function_name(path)}(shimoku_client)' for path in reports_tree]
    main_code_lines = [
        'shimoku_client = shimoku.Client(',
        '    access_token="/",',
        '    universe_id="5c22ba15-c32d-4f4f-9f3d-7c2d331a87a4",',
        '    async_execution=True,',
        '    verbosity="INFO",',
        ')',
        'shimoku_client.set_workspace("34b9c913-ba02-47cf-a9cf-cdefb17f8b03")',
        f'shimoku_client.set_menu_path("{self._app["name"]}")',
        *shared_data_sets_code_lines,
        '',
        'shimoku_client.plt.clear_menu_path()',
        '',
        *function_calls_code_lines,
        '',
        'shimoku_client.run()',
    ]
    code_lines.extend([
        '',
        '',
        'def main():',
        *['    ' + line for line in main_code_lines],
        '',
        '',
        'if __name__ == "__main__":',
        '    main()',
        ''
    ])

    # print('\n'.join(code_lines), '\n')
    if file_name is None:
        file_name = self._app['name']

    with open(os.path.join(output_path, file_name + '.py'), 'w') as f:
        f.write('\n'.join(code_lines))


s = shimoku.Client(
    access_token='/',
    universe_id='5c22ba15-c32d-4f4f-9f3d-7c2d331a87a4',
    verbosity='INFO',
    environment='local',
    async_execution=True,
    local_port=8080,
)
s.set_workspace('34b9c913-ba02-47cf-a9cf-cdefb17f8b03')
print([app['name'] for app in s.workspaces.get_workspace_menu_paths(s.workspace_id)])
s.set_menu_path('Modal Test')

output_path = 'generated_code'
generate_code(s.plt)
