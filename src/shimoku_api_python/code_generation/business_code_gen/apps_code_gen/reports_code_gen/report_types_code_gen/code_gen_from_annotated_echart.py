from typing import TYPE_CHECKING, List
import asyncio
from shimoku_api_python.utils import change_data_set_name_with_report

from ...data_sets_code_gen.code_gen_from_data_sets import code_gen_read_csv_from_data_set
if TYPE_CHECKING:
    from ...code_gen_from_apps import AppCodeGen
    from shimoku_api_python.resources.report import Report


# Todo needs to have the correct names for the columns
async def code_gen_from_annotated_echart(
        self: 'AppCodeGen', report: 'Report', report_params: List[str], properties: dict
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
    data_args = await asyncio.gather(*[code_gen_read_csv_from_data_set(data_set, name)
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