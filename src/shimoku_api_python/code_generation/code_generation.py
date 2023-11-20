import asyncio
import os
from copy import copy, deepcopy
from typing import Optional, List, Tuple, Dict
import subprocess

from shimoku_api_python.resources.business import Business
from shimoku_api_python.resources.app import App
from shimoku_api_python.resources.data_set import DataSet
from shimoku_api_python.resources.report import Report
from shimoku_api_python.resources.reports.modal import Modal
from shimoku_api_python.resources.reports.tabs_group import TabsGroup
from shimoku_api_python.async_execution_pool import ExecutionPoolContext
from shimoku_api_python.utils import revert_uuids_from_dict, create_function_name, change_data_set_name_with_report, \
    create_normalized_name

from shimoku_api_python.code_generation.tree_generation import CodeGenTree
from shimoku_api_python.code_generation.file_generator import CodeGenFileHandler

import logging
from shimoku_api_python.execution_logger import logging_before_and_after, log_error

logger = logging.getLogger(__name__)


class AppCodeGen:
    """ Class for generating code from a menu path. """

    def __init__(self, business_id: str, app: App, output_path: str, epc: ExecutionPoolContext):
        self.epc = epc
        self._app = app
        self.app_f_name = 'menu_path_' + create_function_name(app['name'])
        self._business_id = business_id
        self._output_path = (f'{output_path}/'
                             f'{self.app_f_name}')
        self._actual_bentobox: Optional[Dict] = None
        self._imports_code_lines = [
            'import os',
            'import shimoku_api_python as shimoku'
        ]
        self._file_generator: CodeGenFileHandler = CodeGenFileHandler(self._output_path)
        self._code_gen_tree: CodeGenTree = CodeGenTree(app, self._file_generator)

    @staticmethod
    def _code_gen_value(v):
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

    def _code_gen_from_list(self, l, deep=0):
        return [' ' * deep + str(l) + ',']
        # code_lines = [' ' * deep + '[']
        # deep += 4
        # for element in l:
        #     if isinstance(element, dict):
        #         code_lines.extend(self._code_gen_from_dict(element, deep))
        #     elif isinstance(element, list):
        #         code_lines.extend(self._code_gen_from_list(element, deep))
        #     else:
        #         code_lines.append(' ' * deep + f'{self._code_gen_value(element)},')
        # deep -= 4
        # code_lines.append(' ' * deep + '],')
        # return code_lines

    def _code_gen_from_dict(self, d, deep=0):
        return [' ' * deep + str(d) + ',']
        # code_lines = [' ' * deep + '{']
        # deep += 4
        # for k, v in d.items():
        #     if isinstance(v, (dict, list)):
        #         code_lines.append(' ' * deep + f'"{k}":')
        #         if isinstance(v, dict):
        #             code_lines.extend(self._code_gen_from_dict(v, deep))
        #         elif isinstance(v, list):
        #             code_lines.extend(self._code_gen_from_list(v, deep))
        #     else:
        #         code_lines.append(' ' * deep + f'"{k}": ' + f'{self._code_gen_value(v)},')
        # deep -= 4
        # code_lines.append(' ' * deep + '},')
        # return code_lines

    def _delete_default_properties(self, properties: dict, default_properties: dict) -> dict:
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
                properties[key] = self._delete_default_properties(properties[key], value)
                if len(properties[key]) == 0:
                    del properties[key]
        return properties

    async def get_linked_data_set_info(
            self, report: Report, rds_ids_in_order: List[str]
    ) -> Tuple[Dict[str, DataSet], List[Tuple[str, str]]]:
        unordered_rds: List[Report.ReportDataSet] = await report.get_report_data_sets()
        rds: List[Report.ReportDataSet] = []
        for rds_id in rds_ids_in_order:
            rds.append(next(rd for rd in unordered_rds if rd['id'] == rds_id))
        referenced_data_sets = {d_id: await self._app.get_data_set(d_id) for d_id in
                                set([rd['dataSetId'] for rd in rds])}
        mappings = [(rd['dataSetId'], rd['properties']['mapping']) for rd in rds]
        return referenced_data_sets, mappings

    @staticmethod
    async def _code_gen_read_csv_from_data_set(data_set: DataSet, name: str) -> str:
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
        return (f'pd.read_csv('
                f'f"{{data_folder_path}}/{name}.csv"{f", parse_dates={parse_dates}" if parse_dates else ""})'
                f'.fillna("")'
                )

    async def _code_gen_from_indicator(
            self, report_params: List[str], properties: dict
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

    async def _code_gen_from_echarts(
            self, report: Report, report_params: List[str], properties: dict
    ) -> List[str]:
        """ Generate code for an echarts report.
        :param report: report to generate code from
        :param report_params: parameters of the report
        :param properties: properties of the report
        :return: list of code lines
        """
        echart_options = deepcopy(properties['option'])
        rds_ids_in_order = revert_uuids_from_dict(echart_options)
        referenced_data_sets, mappings = await self.get_linked_data_set_info(report, rds_ids_in_order)
        if len(referenced_data_sets) > 1:
            log_error(logger,
                      'Only one data set is supported for the current implementation of the echarts component.',
                      RuntimeError)
        fields = [mapping[1] for mapping in mappings]
        data_set_id, data_set = list(referenced_data_sets.items())[0] if len(referenced_data_sets) > 0 else (None, None)

        data_arg = ['[{}],']
        if data_set_id in self._code_gen_tree.shared_data_sets:
            if data_set_id in self._code_gen_tree.custom_data_sets_with_data:
                return []
            data_arg = [f'"{data_set["name"]}",']
        elif data_set_id in self._code_gen_tree.custom_data_sets_with_data:
            val = self._code_gen_tree.custom_data_sets_with_data[data_set_id]
            data_arg = self._code_gen_from_dict(val, 4) \
                if isinstance(val, dict) else self._code_gen_from_list(val, 4)
            data_arg[0] = data_arg[0][4:]
            data_arg += ['    data_is_not_df=True,']
            fields = '["data"]'
        elif data_set is not None:
            data_arg = [
                (await self._code_gen_read_csv_from_data_set(
                    data_set, change_data_set_name_with_report(data_set, report)
                )) + ','
            ]

        options_code = self._code_gen_from_dict(echart_options, 4)

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

    # Todo needs to have the correct names for the columns
    async def _code_gen_from_annotated_echart(
            self, report: Report, report_params: List[str], properties: dict
    ) -> List[str]:
        """ Generate code for an echarts report.
        :param report: report to generate code from
        :param report_params: parameters of the report
        :param properties: properties of the report
        :return: list of code lines """
        report_data_sets: List[Report.ReportDataSet] = await report.get_report_data_sets()
        data_set_ids = [rds['dataSetId'] for rds in report_data_sets]
        data_sets = await asyncio.gather(*[self._app.get_data_set(ds_id) for ds_id in data_set_ids])
        data_set_names = [change_data_set_name_with_report(data_set, report) for data_set in data_sets]
        data_args = await asyncio.gather(*[self._code_gen_read_csv_from_data_set(data_set, name)
                                           for data_set, name in zip(data_sets, data_set_names)])
        slider_properties = properties.get('slider') or {}
        marks = slider_properties.pop('marks') if 'marks' in slider_properties else None

        slider_params = []
        if slider_properties:
            slider_params.append(f'    slider_config={slider_properties},')
        if marks:
            slider_params.append(f'    slider_marks={[(mark["label"], mark["value"]) for mark in marks]},')

        return [
            'shimoku_client.plt.annotated_chart(',
            f'    data=[{", ".join(data_args)}],',
            '    x="dateField1",',
            f'    y={["intField1"] * len(data_args)},',
            *slider_params,
            *report_params,
            ')'
        ]

    async def _code_gen_from_table(
            self, report: Report, report_params: List[str], properties: dict
    ) -> List[str]:
        """ Generate code for a table report.
        :param report: report to generate code from
        :param report_params: parameters of the report
        :param properties: properties of the report
        :return: list of code lines
        """
        report_data_set: Report.ReportDataSet = (await report.get_report_data_sets())[0]
        data_set_id = report_data_set['dataSetId']
        data_set = await self._app.get_data_set(data_set_id)
        data_arg = await self._code_gen_read_csv_from_data_set(
            data_set, change_data_set_name_with_report(data_set, report)
        )
        if data_set_id in self._code_gen_tree.shared_data_sets:
            data_arg = f'"{data_set["name"]}",'
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

    async def _code_gen_from_form(
            self, report: Report, properties: dict
    ) -> List[str]:
        """ Generate code for a form report.
        :param report: report to generate code from
        :param properties: properties of the report
        :return: list of code lines
        """
        rds = (await report.get_report_data_sets())[0]
        rds_properties = rds['properties']
        form_groups = {group['title']: group['fields'] for group in rds_properties['fields']}
        input_form_params = []
        if rds_properties['fields'][0].get('nextFormGroup'):
            input_form_params.append(f'    dynamic_sequential_show=True,')
        if rds_properties.get('variant') == 'autoSend':
            input_form_params.append(f'    auto_send=True,')

        events_on_submit = []
        for event in properties['events']['onSubmit']:
            if event['action'] == 'openModal':
                input_form_params.append(
                    f'    modal="{(await self._app.get_report(event["params"]["modalId"]))["properties"]["hash"]}",'
                )
            elif event['action'] == 'openActivity':
                input_form_params.append(
                    f'    activity_name="{(await self._app.get_activity(event["params"]["activityId"]))["name"]}",'
                )
            else:
                events_on_submit.append(event)
        if events_on_submit:
            code_gen_on_submit_events = self._code_gen_from_list(events_on_submit, 4)
            input_form_params.append(f'    on_submit_events={code_gen_on_submit_events[0][4:]}')
            input_form_params.extend(code_gen_on_submit_events[1:])

        report_params = [
            f'    order={report["order"]},',
        ]
        if report['sizeColumns'] != 12:
            report_params.append(f'    cols_size={report["sizeColumns"]},')
        if report['sizeRows'] != 1:
            report_params.append(f'    rows_size={report["sizeRows"]},')
        if report['sizePadding'] != '0,0,0,0':
            report_params.append(f'    padding="{report["sizePadding"]}",')
        return [
            'shimoku_client.plt.generate_input_form_groups(',
            f'    form_groups={form_groups},',
            *report_params,
            *input_form_params,
            ')'
        ]

    @staticmethod
    def _code_gen_from_html_string(html_string: str):
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
                code_lines.append(current_line + ';')
                current_line = ""
            else:
                current_line += c

        return [f'"{line}"' for line in code_lines if line]

    async def _code_gen_from_html(
            self, report: Report
    ) -> List[str]:
        """ Generate code for an html report.
        :param report: report to generate code from
        :return: list of code lines
        """

        html = report['chartData'][0]["value"].replace("'", "\\'").replace('"', '\\"')
        html_lines = ['    ' + line for line in self._code_gen_from_html_string(html)]
        if not html_lines:
            return ['pass']
        html_lines[-1] += ','
        code_lines = [
            'shimoku_client.plt.html(',
            f'    order={report["order"]},',
        ]
        if report['sizeColumns'] != 12:
            code_lines.append(f'    cols_size={report["sizeColumns"]},')
        if report['sizeRows']:
            code_lines.append(f'    rows_size={report["sizeRows"]},')
        if report['sizePadding'] != '0,0,0,0':
            code_lines.append(f'    padding="{report["sizePadding"]}",')

        code_lines.extend([f'    html={html_lines[0][4:]}', *html_lines[1:], ')'])

        return code_lines

    async def _code_gen_from_button_modal(
            self, report: Report, report_params: List[str]
    ) -> List[str]:
        modal_id = report['properties']['events']['onClick'][0]['params']['modalId']
        modal = await self._app.get_report(modal_id)
        return [
            'shimoku_client.plt.modal_button(',
            f'    modal="{modal["properties"]["hash"]}",',
            *report_params,
            ')'
        ]

    async def _code_gen_from_button_activity(
            self, report: Report, report_params: List[str]
    ) -> List[str]:
        activity_id = report['properties']['events']['onClick'][0]['params']['activityId']
        activity = await self._app.get_activity(activity_id)
        return [
            'shimoku_client.plt.activity_button(',
            f'    activity_name="{activity["name"]}",',
            *report_params,
            ')'
        ]

    async def _code_gen_from_button_generic(
            self, report: Report, report_params: List[str]
    ) -> List[str]:
        events_code = self._code_gen_from_dict(report['properties']['events'], 4)
        return [
            'shimoku_client.plt.button(',
            *report_params,
            f'    on_click_events={events_code[0][4:]}',
            *events_code[1:],
            ')'
        ]

    async def _code_gen_from_button(
            self, report: Report
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
            return await self._code_gen_from_button_modal(report, report_params)
        elif report['properties']['events']['onClick'][0]['action'] == 'openActivity':
            return await self._code_gen_from_button_activity(report, report_params)
        else:
            return await self._code_gen_from_button_generic(report, report_params)

    async def _code_gen_from_filter(
            self, report: Report
    ) -> List[str]:
        """ Generate code for a filter report.
        :param report: report to generate code from
        :return: list of code lines
        """
        filter_def = report['properties']['filter'][0]
        field_name = filter_def['field']
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
        if filter_def['inputType'] == 'CATEGORICAL_MULTI':
            report_params.append(f'    multi_select=True,')
        return [
            'shimoku_client.plt.filter(',
            *report_params,
            ')'
        ]

    async def _code_gen_from_iframe(
            self, report: Report
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

    async def _code_gen_from_other(
            self, report: Report, is_last: bool
    ) -> List[str]:
        """ Generate code for a report that is not a tabs group.
        :param report: report to generate code from
        :param is_last: whether the report is the last one
        :return: list of code lines """
        code_lines = []

        properties = self._delete_default_properties(report['properties'], report.default_properties)
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
            if self._actual_bentobox is None or self._actual_bentobox['bentoboxId'] != report['bentobox']['bentoboxId']:
                self._actual_bentobox = report['bentobox']

                cols_size = self._actual_bentobox['bentoboxSizeColumns']
                rows_size = self._actual_bentobox['bentoboxSizeRows']
                code_lines.extend([
                    '',
                    f'shimoku_client.plt.set_bentobox(cols_size={cols_size}, rows_size={rows_size})'
                ])
        elif self._actual_bentobox is not None:
            self._actual_bentobox = None
            code_lines.append('shimoku_client.plt.pop_out_of_bentobox()')

        if report['reportType'] == 'INDICATOR':
            code_lines.extend(await self._code_gen_from_indicator(report_params, properties))
        elif report['reportType'] == 'ECHARTS2':
            code_lines.extend(await self._code_gen_from_echarts(report, report_params, properties))
        elif report['reportType'] == 'TABLE':
            code_lines.extend(await self._code_gen_from_table(report, report_params, properties))
        elif report['reportType'] == 'FORM':
            code_lines.extend(await self._code_gen_from_form(report, properties))
        elif report['reportType'] == 'HTML':
            code_lines.extend(await self._code_gen_from_html(report))
        elif report['reportType'] == 'IFRAME':
            code_lines.extend(await self._code_gen_from_iframe(report))
        elif report['reportType'] == 'ANNOTATED_ECHART':
            code_lines.extend(await self._code_gen_from_annotated_echart(report, report_params, properties))
        elif report['reportType'] == 'BUTTON':
            code_lines.extend(await self._code_gen_from_button(report))
        elif report['reportType'] == 'FILTERDATASET':
            code_lines.extend(await self._code_gen_from_filter(report))
        else:
            code_lines.extend(
                [f"shimoku_client.add_report({report['reportType']}, order={report['order']}, data=dict())"])

        if is_last and self._actual_bentobox is not None:
            self._actual_bentobox = None
            code_lines.append('shimoku_client.plt.pop_out_of_bentobox()')

        return code_lines

    async def _code_gen_from_tabs_group(
            self, tree: dict, is_last: bool = False
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
        properties = self._delete_default_properties(tabs_group['properties'], TabsGroup.default_properties)
        del properties['hash']
        if 'tabs' in properties:
            del properties['tabs']
        if 'variant' in properties:
            properties['just_labels'] = True
            del properties['variant']

        for tab in tree['tabs']:
            code_lines.extend(['', f'def tab_{create_function_name(tabs_index[0])}_{create_function_name(tab)}():'])
            # tab_code = await code_gen_tabs_functions(tree['tabs'][tab]['tab_groups'])
            tab_code = await self._code_gen_tabs_and_other(tree['tabs'][tab])
            code_lines.extend([f'    {line}' for line in tab_code])

        code_lines.extend([
            '',
            'shimoku_client.plt.set_tabs_index(',
            f'    tabs_index=("{tabs_index[0]}", "{tabs_index[1]}"), order={tabs_group["order"]}, ',
        ])
        if tabs_group['sizeColumns']:
            code_lines.append(f'    cols_size={tabs_group["sizeColumns"]},')
        if tabs_group['sizeRows']:
            code_lines.append(f'    rows_size={tabs_group["sizeRows"]},')
        if tabs_group['sizePadding']:
            code_lines.append(f'    padding="{tabs_group["sizePadding"]}",')
        if parent_tabs_index:
            code_lines.extend([f'    parent_tabs_index={parent_tabs_index},'])
        code_lines.extend([f'    {k}={self._code_gen_value(v)},' for k, v in properties.items()])
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

    async def _code_gen_tabs_functions(self, path: str) -> List[str]:
        """ Generate code for tabs groups functions.
        :return: list of code lines
        """
        code_lines = []
        if path not in self._code_gen_tree.all_tab_groups:
            return code_lines
        for tabs_group in self._code_gen_tree.all_tab_groups[path]:
            tab_code_lines = await self._code_gen_from_tabs_group(tabs_group)
            code_lines.extend([
                '',
                f'def tabs_group_{create_function_name(tabs_group["tabs_group"]["properties"]["hash"])}'
                f'(shimoku_client: shimoku.Client):',
                *['    ' + line for line in tab_code_lines]
            ])
        return code_lines

    async def _code_gen_tabs_and_other(
            self, tree: dict
    ) -> List[str]:
        """ Generate code for tabs and other components.
        :param tree: tree of reports
        :return: list of code lines
        """
        code_lines: List[str] = []
        components_ordered = sorted(tree['other'] + tree['tab_groups'], key=lambda x: x['order'])
        for i, component in enumerate(components_ordered):
            if isinstance(component, dict):
                code_lines.extend([
                    '',
                    f'tabs_group_{create_function_name(component["tabs_group"]["properties"]["hash"])}(shimoku_client)']
                )
            else:
                code_lines.extend(await self._code_gen_from_other(component, is_last=i == len(components_ordered) - 1))
        return code_lines

    async def _code_gen_from_modal(self, tree: dict) -> List[str]:
        """ Generate code for a modal.
        :param tree: tree of reports
        :return: list of code lines
        """
        # code_lines = (await code_gen_tabs_functions(tree['tab_groups']))
        code_lines = []
        modal: Modal = tree['modal']
        properties = self._delete_default_properties(modal['properties'], Modal.default_properties)
        properties['modal_name'] = properties['hash']
        del properties['hash']
        if 'reportIds' in properties:
            del properties['reportIds']
        if 'open' in properties:
            del properties['open']
            properties['open_by_default'] = True
        code_lines.extend([
            'shimoku_client.plt.set_modal(',
            *[f'    {k}={self._code_gen_value(v)},' for k, v in properties.items()],
            ')',
        ])
        code_lines.extend(await self._code_gen_tabs_and_other(tree))
        code_lines.extend(['', 'shimoku_client.plt.pop_out_of_modal()'])
        return code_lines

    async def _code_gen_modals_functions(self, path: str) -> List[str]:
        code_lines = []
        for modal in self._code_gen_tree.tree[path]['modals']:
            modal_code_lines = await self._code_gen_from_modal(modal)
            code_lines.extend([
                '',
                f'def modal_{create_function_name(modal["modal"]["properties"]["hash"])}'
                f'(shimoku_client: shimoku.Client):',
                *['    ' + line for line in modal_code_lines]
            ])
        return code_lines

    async def _code_gen_from_reports_tree(self, path: str) -> List[str]:
        code_lines = [
            # *(await code_gen_tabs_functions(all_tab_groups[path]) if path in all_tab_groups else []),
            # *(await code_gen_modals_functions(tree[path]['modals']) if path in tree else []),
        ]
        tree = self._code_gen_tree.tree
        for modal in tree[path]['modals']:
            code_lines.extend([
                '',
                f'modal_{create_function_name(modal["modal"]["properties"]["hash"])}(shimoku_client)'
            ])
        # if len(tree[path]['modals']) > 0:
        #     code_lines.extend(['', 'shimoku_client.plt.pop_out_of_modal()', ''])
        code_lines.extend(['', *await self._code_gen_tabs_and_other(tree[path])])
        return code_lines

    async def _code_gen_shared_data_sets(self) -> List[str]:
        """ Generate code for data sets that are shared between reports.
        :return: list of code lines
        """
        code_lines = []
        dfs: List[DataSet] = []
        custom: List[DataSet] = []
        for ds_id in self._code_gen_tree.shared_data_sets:
            ds = await self._app.get_data_set(ds_id)
            if ds_id in self._code_gen_tree.custom_data_sets_with_data:
                custom.append(ds)
            else:
                dfs.append(ds)
        if len(dfs) > 0 or len(custom) > 0:
            code_lines.append("shimoku_client.plt.set_shared_data(")

        if len(dfs) > 0:
            code_lines.extend([
                "    dfs={",
                *[f'        "{ds["name"]}": {await self._code_gen_read_csv_from_data_set(ds, ds["name"])},'
                  for ds in dfs],
                "    },",
            ])
        if len(custom) > 0:
            code_lines.append('    custom_data={')
            for ds in custom:
                custom_data = self._code_gen_tree.custom_data_sets_with_data[ds["id"]]
                if isinstance(custom_data, dict):
                    custom_data = self._code_gen_from_dict(custom_data, 8)
                else:
                    custom_data = self._code_gen_from_list(custom_data, 8)

                code_lines.extend([
                    f'        "{ds["name"]}": {custom_data[0][8:]}',
                    *custom_data[1:],
                ])
            code_lines.append('    },')

        if len(dfs) > 0 or len(custom) > 0:
            code_lines.append(")")
            code_lines = [''] + code_lines

        return code_lines

    @logging_before_and_after(logger.debug)
    async def generate_code(self):
        """ Use the resources in the API to generate code_lines for the SDK. Create a file in
        the specified path with the generated code_lines.
        """
        await self._code_gen_tree.generate_tree()
        if self._code_gen_tree.needs_pandas:
            self._imports_code_lines.extend(['import pandas as pd'])

        code_lines: List[str] = []
        function_calls_code_lines: List[str] = []

        shared_data_sets_code_lines = await self._code_gen_shared_data_sets()
        scripts_imports = copy(self._imports_code_lines)
        for path in self._code_gen_tree.tree:
            function_code_lines = await self._code_gen_from_reports_tree(path)
            path_name = 'sub_path_' + create_function_name(path) if path else 'no_path'
            script_code_lines = [
                *scripts_imports,
                '',
                '',
                'data_folder_path = os.path.dirname(os.path.abspath(__file__)) + "/data"',
                *await self._code_gen_tabs_functions(path),
                *await self._code_gen_modals_functions(path),
                '',
                '',
                f'def {path_name}(shimoku_client: shimoku.Client):',
                *['    ' + line for line in function_code_lines],
                '',
            ]
            self._file_generator.generate_script_file(path_name, script_code_lines)
            self._imports_code_lines.append(
                f'from .{path_name} import {path_name}'
            )
            function_calls_code_lines.extend([
                '',
                f'shimoku_client.set_menu_path("{self._app["name"]}"' + (f', "{path}")' if path is not None else ')'),
                f'{path_name}(shimoku_client)'
            ])

        main_code_lines = [
            f'shimoku_client.set_menu_path("{self._app["name"]}")',
            'shimoku_client.plt.clear_menu_path()',
            *shared_data_sets_code_lines,
            *function_calls_code_lines,
        ]

        code_lines.extend([
            *self._imports_code_lines,
            '',
            '',
            'data_folder_path = os.path.dirname(os.path.abspath(__file__)) + "/data"',
            '',
            '',
            f'def {self.app_f_name}(shimoku_client: shimoku.Client):',
            *['    ' + line for line in main_code_lines],
            ''
        ])

        self._file_generator.generate_script_file('app', code_lines)
        # Create an __init__.py file for the imports to work
        self._file_generator.generate_script_file('__init__', [''])


class BusinessCodeGen:

    def __init__(self, business: Business, output_path: str, epc: ExecutionPoolContext):
        self._business = business
        self._output_path = f'{output_path}/{business["id"]}'
        self._file_generator = CodeGenFileHandler(self._output_path)
        self.epc = epc

    async def generate_code(
            self, environment: str,
            access_token: str,
            universe_id: str,
            business_id: str,
            menu_paths: Optional[List[str]] = None,
            use_black_formatter: bool = True
    ):
        """ Use the resources in the API to generate code_lines for the SDK. Create a file in
        the specified path with the generated code_lines.
        :param environment: environment to use
        :param access_token: access token to use
        :param universe_id: universe id to use
        :param business_id: business id to use
        :param menu_paths: list of menu paths to generate code for
        :param use_black_formatter: whether to use black formatter
        """
        import_code_lines: List[str] = [
            'import shimoku_api_python as shimoku'
        ]
        main_code_lines: List[str] = [
            'shimoku_client = shimoku.Client(',
        ]
        if access_token != 'local':
            main_code_lines.extend([
                f'    access_token="{access_token}",',
                f'    universe_id="{universe_id}",'
            ])
        main_code_lines.extend([
            f'    environment="{environment}",',
            f'    verbosity="INFO",',
            ')',
            f'shimoku_client.set_workspace("{business_id}")',
            '',
        ])
        exec_code_lines: List[str] = [
            '',
            'if __name__ == "__main__":',
            '    main()',
            ''
        ]
        if menu_paths:
            menu_paths = [create_normalized_name(menu_path) for menu_path in menu_paths]

        for app in await self._business.get_apps():
            if menu_paths is None or app['normalizedName'] in menu_paths:
                app_code_gen = AppCodeGen(business_id, app, self._output_path, self.epc)
                await app_code_gen.generate_code()
                import_code_lines.append(f'from .{app_code_gen.app_f_name}.app import {app_code_gen.app_f_name}')
                main_code_lines.append(f'{app_code_gen.app_f_name}(shimoku_client)')
        main_code_lines.extend(['', 'shimoku_client.run()'])
        self._file_generator.generate_script_file(
            'main',
            [
                *import_code_lines,
                '',
                '',
                'def main():',
                *['    ' + line for line in main_code_lines],
                '',
                *exec_code_lines
            ]
        )
        # Create an __init__.py file for the imports to work
        self._file_generator.generate_script_file('__init__', [''])

        if use_black_formatter:
            # apply black formatting
            subprocess.run(["black", "-l", "80", os.path.join(self._output_path)])
