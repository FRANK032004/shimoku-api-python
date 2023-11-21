from copy import copy
from typing import TYPE_CHECKING, List
if TYPE_CHECKING:
    from ...code_gen_from_apps import AppCodeGen
    from shimoku_api_python.resources.report import Report

from .code_gen_from_indicators import code_gen_from_indicator
from .code_gen_from_echarts import code_gen_from_echarts
from .code_gen_from_annotated_echart import code_gen_from_annotated_echart
from .code_gen_from_table import code_gen_from_table
from .code_gen_from_form import code_gen_from_form
from .code_gen_from_html import code_gen_from_html
from .code_gen_from_button import code_gen_from_button
from .code_gen_from_filter import code_gen_from_filter
from .code_gen_from_iframe import code_gen_from_iframe


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


async def code_gen_from_other_reports(
        self: 'AppCodeGen', report: 'Report', is_last: bool
) -> List[str]:
    """ Generate code for a report that is not a tabs group.
    :param report: report to generate code from
    :param is_last: whether the report is the last one
    :return: list of code lines """
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
        code_lines.extend(await code_gen_from_indicator(self, report_params, properties))
    elif report['reportType'] == 'ECHARTS2':
        code_lines.extend(await code_gen_from_echarts(self, report, report_params, properties))
    elif report['reportType'] == 'TABLE':
        code_lines.extend(await code_gen_from_table(self, report, report_params, properties))
    elif report['reportType'] == 'FORM':
        code_lines.extend(await code_gen_from_form(self, report, properties))
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
        code_lines.extend(
            [f"shimoku_client.add_report({report['reportType']}, order={report['order']}, data=dict())"])

    if is_last and self._actual_bentobox is not None:
        self._actual_bentobox = None
        code_lines.append('shimoku_client.plt.pop_out_of_bentobox()')

    return code_lines
