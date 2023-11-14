import asyncio
import os
import json
from copy import copy, deepcopy
from typing import Optional, List, Tuple, Dict
import shimoku_api_python as shimoku
import pandas as pd
import subprocess

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
all_tab_groups: Dict[str, List[dict]] = {}
imports_code_lines = [
    'import shimoku_api_python as shimoku',
]


def check_correct_character(character: str) -> bool:
    """ Check if a character is valid for a function name
    :param character: character to check
    :return: True if character is valid, False otherwise
    """
    return character.isalnum() or character in ['_', '-', ' ']


def create_function_name(name: Optional[str]) -> str:
    """ Create a valid function name from a string
    :param name: string to create function name from
    :return: valid function name
    """
    if name is None:
        return 'no_path'
    #Change Uppercase to '_' + lowercase if previous character is in abecedary
    name = ''.join(['_' + c.lower() if c.isupper() and i > 0 and name[i - 1].isalpha() else c
                    for i, c in enumerate(name) if check_correct_character(c)])
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

    path = tabs_group['path']
    if path not in all_tab_groups:
        all_tab_groups[path] = []
    all_tab_groups[path].insert(0, tabs_group_dict)

    tree.append(tabs_group_dict)
    tabs = sorted(tabs_group['properties']['tabs'].items(), key=lambda x: x[1]['order'])
    for tab, tab_data in tabs:
        report_ids = tab_data['reportIds']
        tab_dict = {'tab_groups': [], 'other': []}
        tabs_group_dict['tabs'][tab] = tab_dict
        for child_id in report_ids:
            seen_reports.add(child_id)
            child_report = await self._app.get_report(child_id)
            if child_report['reportType'] == 'TABS':
                await tree_from_tabs_group(
                    self, tab_dict['tab_groups'], child_report, seen_reports,
                    (tabs_group['properties']['hash'], tab))
            else:
                tab_dict['other'].append(child_report)
        tab_dict['tab_groups'] = sorted(tab_dict['tab_groups'], key=lambda x: x['tabs_group']['order'])
        tab_dict['other'] = sorted(tab_dict['other'], key=lambda x: x['order'])


async def tree_from_modal(self: PlotApi, tree: list, modal: Modal, seen_reports: set):
    """ Recursively build a tree of reports from a modal.
    :param self: PlotApi instance
    :param tree: list to append to
    :param modal: modal to build tree from
    :param seen_reports: set of report ids that have already been seen
    """
    modal_dict = {'modal': modal, 'tab_groups': [], 'other': []}
    tree.append(modal_dict)
    for child_id in modal['properties']['reportIds']:
        seen_reports.add(child_id)
        child_report = await self._app.get_report(child_id)
        if child_report['reportType'] == 'TABS':
            await tree_from_tabs_group(self, modal_dict['tab_groups'], child_report, seen_reports)
        else:
            modal_dict['other'].append(child_report)
    modal_dict['tab_groups'] = sorted(modal_dict['tab_groups'], key=lambda x: x['tabs_group']['order'])
    modal_dict['other'] = sorted(modal_dict['other'], key=lambda x: x['order'])


async def generate_tree(self: PlotApi, reports: list[Report]) -> dict:
    """ Generate a tree of reports from a list of reports.
    :param self: PlotApi instance
    :param reports: list of reports to build tree from
    :return: tree of reports
    """
    global all_tab_groups
    reports_tree = {}
    seen_reports = set()
    for report in reports:

        if report['id'] in seen_reports:
            continue

        if report['path'] not in reports_tree:
            reports_tree[report['path']] = {'modals': [], 'tab_groups': [], 'other': []}

        if report['reportType'] == 'MODAL':
            await tree_from_modal(self, reports_tree[report['path']]['modals'], report, seen_reports)
        elif report['reportType'] == 'TABS':
            await tree_from_tabs_group(self, reports_tree[report['path']]['tab_groups'], report, seen_reports)
        else:
            reports_tree[report['path']]['other'].append(report)

    for path in reports_tree:
        reports_tree[path]['other'] = sorted(reports_tree[path]['other'], key=lambda x: x['order'])
        reports_tree[path]['tab_groups'] = sorted(reports_tree[path]['tab_groups'],
            key=lambda x: x['tabs_group']['order'])

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
        await create_data_set_file(self, await self._app.get_data_set(ds_id))
    for ds_id, report in individual_data_sets.items():
        await create_data_set_file(self, await self._app.get_data_set(ds_id), report)


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
    if isinstance(v, str):
        special_chars = [
            '"', "'", '\\', '\n', '\t', '\r', '\b', '\f', '\v',
            '\a', '\0', '\1', '\2', '\3', '\4', '\5', '\6', '\7'
        ]
        replacement_chars = [
            '\"', "\'", '\\\\', '\\n', '\\t', '\\r', '\\b', '\\f', '\\v',
            '\\a', '\\0', '\\1', '\\2', '\\3', '\\4', '\\5', '\\6', '\\7'
        ]
        result = ''
        for char in v:
            if char in special_chars:
                result += replacement_chars[special_chars.index(char)]
            else:
                result += char
        print(result) if any(char in special_chars for char in v) else None
        return f'"{result}"'
    return v


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


async def code_gen_read_csv_from_data_set(self: PlotApi, data_set: DataSet, name: str) -> str:
    """ Generate code for reading a csv file from a data set.
    :param data_set: data set to generate code from
    :param name: name of the data set
    :return: code line
    """
    data_point = (await data_set.get_one_data_point()).cascade_to_dict()
    parse_dates = []
    for key, value in data_point.items():
        if 'date' in key and value is not None:
            parse_dates.append(key)
    return f'pd.read_csv("data/{name}.csv"{f", parse_dates={parse_dates}" if parse_dates else ""}).fillna("")'


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
    data_set_id, data_set = list(referenced_data_sets.items())[0] if len(referenced_data_sets) > 0 else (None, None)

    data_arg = ['[{}],']
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
    elif data_set is not None:
        data_arg = [
            (await code_gen_read_csv_from_data_set(self, data_set, change_data_set_name_with_report(data_set, report)))
            + ','
        ]

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
    return ['pass']
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
    report_data_set: Report.ReportDataSet = (await report.get_report_data_sets())[0]
    data_set_id = report_data_set['dataSetId']
    data_set = await self._app.get_data_set(data_set_id)
    data_arg = await code_gen_read_csv_from_data_set(self, data_set, change_data_set_name_with_report(data_set, report))
    if data_set_id in shared_data_sets:
        data_arg = f'"{data_set["name"]}",'
    print_dict(properties)
    table_params = []
    # TODO: This will need to have the correct names for the columns
    # TODO: Chips
    mapping = properties['rows']['mapping']
    if mapping:
        table_params.append(f'    columns={list(mapping.values())},')
    if properties['pagination']['pageSize'] != 10:
        table_params.append(f'    page_size={properties["pagination"]["pageSize"]},')
    if not properties['columnsButton']:
        table_params.append(f'    columns_button=False,')
    if not properties['filtersButton']:
        table_params.append(f'    filters=False,')
    if not properties['exportButton']:
        table_params.append(f'    export_to_csv=False,')
    if not properties['search']:
        table_params.append(f'    search=False,')
    if properties.get('sort'):
        sort_field = properties['sort']['field']
        sort_direction = properties['sort']['direction']
        table_params.append(f'    initial_sort_column="{sort_field}",')
        if sort_direction != 'asc':
            table_params.append(f'    sort_descending=True,')

    categorical_columns = [mapping[col_dict['field']]
                           for col_dict in properties['columns'] if col_dict.get('type') == 'singleSelect']
    if categorical_columns:
        table_params.append(f'    categorical_columns={categorical_columns},')

    return [
        'shimoku_client.plt.table(',
        f'    data={data_arg},',
        *report_params,
        *table_params,
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
    return ['pass']
    return [
        'shimoku_client.plt.input_form(',
        *report_params,
        # "    options=",
        # *code_gen_from_dict(properties['options'], 4),
        ')'
    ]


def code_gen_from_html_string(html_string: str):
    """ Generate code for an html string.
    :param html_string: html string to generate code from
    :return: list of code lines
    """
    code_lines = []
    current_line = ""
    for c in html_string:
        if c in ['\n', '\r']:
            code_lines.append(current_line)
            current_line = ""
        elif c == '<':
            code_lines.append(current_line)
            current_line = '<'
        elif c == ';' and len(current_line) > 60:
            code_lines.append(current_line+';')
            current_line = ""
        else:
            current_line += c

    return [f'"{line}"' for line in code_lines if line]


async def code_gen_from_html(
        self: PlotApi, report: Report
) -> List[str]:
    """ Generate code for an html report.
    :param report_params: parameters of the report
    :param chartData: chartData of the report where the html is stored
    :return: list of code lines
    """

    html = report['chartData'][0]["value"].replace("'", "\\'").replace('"', '\\"')
    html_lines = ['    ' + line for line in code_gen_from_html_string(html)]
    if not html_lines:
        return ['pass']
    html_lines[-1] += ','
    code_lines = [
        'shimoku_client.plt.html(',
        f'    order={report["order"]},',
    ]
    if report['sizeColumns'] != 12:
        code_lines.append(f'    cols_size={report["sizeColumns"]},')
    if report['sizeRows'] != 1:
        code_lines.append(f'    rows_size={report["sizeRows"]},')
    if report['sizePadding'] != '0,0,0,0':
        code_lines.append(f'    padding="{report["sizePadding"]}",')

    code_lines.extend([f'    html={html_lines[0][4:]}', *html_lines[1:], ')'])

    return code_lines


async def code_gen_from_button_modal(
        self: PlotApi, report: Report, report_params: List[str]
) -> List[str]:
    modal_id = report['properties']['events']['onClick'][0]['params']['modalId']
    modal = await self._app.get_report(modal_id)
    return [
        'shimoku_client.plt.modal_button(',
        f'    modal="{modal["properties"]["hash"]}",',
        *report_params,
        ')'
    ]


async def code_gen_from_button_activity(
        self: PlotApi, report: Report, report_params: List[str]
) -> List[str]:
    activity_id = report['properties']['events']['onClick'][0]['params']['activityId']
    activity = await self._app.get_activity(activity_id)
    return [
        'shimoku_client.plt.activity_button(',
        f'    activity_name="{activity["name"]}",',
        *report_params,
        ')'
    ]


async def code_gen_from_button_generic(
        self: PlotApi, report: Report, report_params: List[str]
) -> List[str]:
    events_code = code_gen_from_dict(report['properties']['events'], 4)
    return [
        'shimoku_client.plt.button(',
        *report_params,
        f'    on_click_events={events_code[0][4:]}',
        *events_code[1:],
        ')'
    ]


async def code_gen_from_button(
        self: PlotApi, report: Report
) -> List[str]:
    """ Generate code for a button report.
    :param report: report to generate code from
    :return: list of code lines
    """
    report_params = [
        f'    label="{report["properties"]["text"]}",',
        f'    order={report["order"]},',
    ]
    if report['sizeColumns'] != 12:
        report_params.append(f'    cols_size={report["sizeColumns"]},')
    if report['sizeRows'] != 1:
        report_params.append(f'    rows_size={report["sizeRows"]},')
    if report['sizePadding'] != '0,0,0,0':
        report_params.append(f'    padding="{report["sizePadding"]}",')
    if report['properties']['align'] != 'stretch':
        report_params.append(f'    align="{report["properties"]["align"]}",')

    if report['properties']['events']['onClick'][0]['action'] == 'openModal':
        return await code_gen_from_button_modal(self, report, report_params)
    elif report['properties']['events']['onClick'][0]['action'] == 'openActivity':
        return await code_gen_from_button_activity(self, report, report_params)
    else:
        return await code_gen_from_button_generic(self, report, report_params)


async def code_gen_from_filter(
        self: PlotApi, report: Report
) -> List[str]:
    """ Generate code for a filter report.
    :param report: report to generate code from
    :return: list of code lines
    """
    filter = report['properties']['filter'][0]
    field_name = filter['field']
    mapping = report['properties']['mapping'][0]
    field = mapping[field_name]
    data_set = await self._app.get_data_set(mapping['id'])
    report_params = [
        f'    order={report["order"]},',
        f'    data="{data_set["name"]}",',
        f'    field="{field}",',
    ]
    if report['sizeColumns'] != 4:
        report_params.append(f'    cols_size={report["sizeColumns"]},')
    if report['sizeRows'] != 1:
        report_params.append(f'    rows_size={report["sizeRows"]},')
    if report['sizePadding'] != '0,0,0,0':
        report_params.append(f'    padding="{report["sizePadding"]}",')
    if filter['inputType'] == 'CATEGORICAL_MULTI':
        report_params.append(f'    multi_select=True,')
    return [
        'shimoku_client.plt.filter(',
        *report_params,
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
    elif report['reportType'] == 'FILTERDATASET':
        code_lines.extend(await code_gen_from_filter(self, report))
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

    for tab in tree['tabs']:
        code_lines.extend(['', f'def tab_{create_function_name(tabs_index[0])}_{create_function_name(tab)}():'])
        # tab_code = await code_gen_tabs_functions(self, tree['tabs'][tab]['tab_groups'])
        tab_code = await code_gen_tabs_and_other(self, tree['tabs'][tab])
        code_lines.extend([f'    {line}' for line in tab_code])

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
        code_lines.extend([f'tab_{create_function_name(tabs_index[0])}_{create_function_name(tab)}()'])

    if parent_tabs_index:
        if not is_last:
            code_lines.extend([
                '',
                f'shimoku_client.plt.set_tabs_index(("{parent_tabs_index[0]}", "{parent_tabs_index[1]}"))'
            ])

    else:
        code_lines.extend(['', 'shimoku_client.plt.pop_out_of_tabs_group()'])

    return code_lines


async def code_gen_tabs_functions(self: PlotApi, tab_groups: List[dict]) -> List[str]:
    code_lines = []
    for tabs_group in tab_groups:
        tab_code_lines = await code_gen_from_tabs_group(self, tabs_group)
        code_lines.extend([
            '',
            f'def tabs_group_{create_function_name(tabs_group["tabs_group"]["properties"]["hash"])}():',
            *['    ' + line for line in tab_code_lines]
        ])
    return code_lines


async def code_gen_tabs_and_other(
    self: PlotApi, tree: dict
) -> List[str]:
    """ Generate code for tabs and other components.
    :param tree: tree of reports
    :param parent_tabs_index: parent tabs index
    :return: list of code lines
    """
    code_lines: List[str] = []
    components_ordered = sorted(tree['other'] + tree['tab_groups'], key=lambda x: x['order'])
    for i, component in enumerate(components_ordered):
        if isinstance(component, dict):
            code_lines.extend([
                '',
                f'tabs_group_{create_function_name(component["tabs_group"]["properties"]["hash"])}()']
            )
        else:
            code_lines.extend(await code_gen_from_other(self, component, is_last=i == len(components_ordered) - 1))
    return code_lines


async def code_gen_from_modal(self: PlotApi, tree: dict) -> List[str]:
    """ Generate code for a modal.
    :param tree: tree of reports
    :return: list of code lines
    """
    # code_lines = (await code_gen_tabs_functions(self, tree['tab_groups']))
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
    code_lines.extend(['', 'shimoku_client.plt.pop_out_of_modal()'])
    return code_lines


async def code_gen_modals_functions(self: PlotApi, modals: List[dict]) -> List[str]:
    code_lines = []
    for modal in modals:
        modal_code_lines = await code_gen_from_modal(self, modal)
        code_lines.extend([
            '',
            f'def modal_{create_function_name(modal["modal"]["properties"]["hash"])}():',
            *['    ' + line for line in modal_code_lines]
        ])
    return code_lines


async def code_gen_from_reports_tree(self: PlotApi, tree: dict, path: str) -> List[str]:
    code_lines = [
        *(await code_gen_tabs_functions(self, all_tab_groups[path]) if path in all_tab_groups else []),
        *(await code_gen_modals_functions(self, tree[path]['modals']) if path in tree else []),
    ]
    for modal in tree[path]['modals']:
        code_lines.extend(['', f'modal_{create_function_name(modal["modal"]["properties"]["hash"])}()'])
    # if len(tree[path]['modals']) > 0:
    #     code_lines.extend(['', 'shimoku_client.plt.pop_out_of_modal()', ''])
    code_lines.extend(['', *await code_gen_tabs_and_other(self, tree[path])])
    return code_lines


async def create_data_set_file(self: PlotApi, data_set: DataSet, report: Optional[Report] = None):
    """ Create a file for a data set.
    :param data_set: data set to create file for
    :param report: report to create file for
    """
    data: List[dict] = [
        {k: v for k, v in dp.cascade_to_dict().items()
         if k not in ['id', 'dataSetId'] and v is not None}
        for dp in await data_set.get_data_points()
    ]

    menu_path = create_function_name(self._app['name'])
    if not os.path.exists(f'{output_path}/{menu_path}/data'):
        os.makedirs(f'{output_path}/{menu_path}/data')

    if len(data) == 0:
        return

    if len(data) > 1 or 'customField1' not in data[0]:
        data_as_df = pd.DataFrame(data)
        for column in data_as_df:
            if 'date' in column:
                data_as_df[column] = pd.to_datetime(data_as_df[column]).dt.strftime('%Y-%m-%dT%H:%M:%S')
        output_name = data_set["name"] if report is None else change_data_set_name_with_report(data_set, report)
        data_as_df.to_csv(os.path.join(f'{output_path}/{menu_path}/data', f'{output_name}.csv'), index=False)
        if 'import pandas as pd' not in imports_code_lines:
            imports_code_lines.append('import pandas as pd')
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
            *[f'        "{ds["name"]}": {await code_gen_read_csv_from_data_set(self, ds, ds["name"])},' for ds in dfs],
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

    menu_path: str = create_function_name(self._app['name'])
    if not os.path.exists(f'{output_path}/{menu_path}'):
        os.makedirs(f'{output_path}/{menu_path}')

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
    code_lines: List[str] = []

    shared_data_sets_code_lines = await code_gen_shared_data_sets(self)
    for path in reports_tree:
        function_code_lines = await code_gen_from_reports_tree(self, reports_tree, path)

        script_code_lines = [
            *imports_code_lines,
            # *await code_gen_tabs_functions(self, all_tab_groups[path]),
            # *await code_gen_modals_functions(self, reports_tree[path]['modals']),
            '',
            '',
            f'def {create_function_name(path)}(shimoku_client: shimoku.Client):',
            *['    ' + line for line in function_code_lines],
            '',
        ]

        script_name = create_function_name(path)
        with open(os.path.join(output_path, menu_path, script_name + '.py'), 'w') as f:
            f.write('\n'.join(script_code_lines))

    function_calls_code_lines = []
    for path in reports_tree:
        script_name = path if path else 'no_path'
        function_calls_code_lines.extend([
            '',
            f'shimoku_client.set_menu_path("{self._app["name"]}"' + (f', "{path}")' if path is not None else ')'),
            f'{create_function_name(path)}(shimoku_client)'
        ])
        imports_code_lines.extend([f'from {create_function_name(script_name)} import {create_function_name(path)}'])

    main_code_lines = [
        'shimoku_client = shimoku.Client(',
        '    async_execution=True,',
        '    environment="develop",',
        '    verbosity="INFO",',
        ')',
        'shimoku_client.set_workspace()',
        f'shimoku_client.set_menu_path("{self._app["name"]}")',
        'shimoku_client.plt.clear_menu_path()',
        *shared_data_sets_code_lines,
        *function_calls_code_lines,
        '',
        'shimoku_client.run()',
    ]

    code_lines.extend([
        *imports_code_lines,
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

    with open(os.path.join(output_path, menu_path, 'main.py'), 'w') as f:
        f.write('\n'.join(code_lines))

    # apply black formatting
    # subprocess.run(["black", "-l", "80", os.path.join(output_path, menu_path)])


s = shimoku.Client(
    verbosity='INFO',
    environment='develop',
    async_execution=True
)
s.set_workspace()
print([app['name'] for app in s.workspaces.get_workspace_menu_paths(s.workspace_id)])
#TODO dont create nan values
s.set_menu_path('test-filters')

output_path = 'generated_code'
generate_code(s.plt)
