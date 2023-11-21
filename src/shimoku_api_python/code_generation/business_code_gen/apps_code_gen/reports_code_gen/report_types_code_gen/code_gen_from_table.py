from typing import TYPE_CHECKING, List
from shimoku_api_python.utils import change_data_set_name_with_report
from ...data_sets_code_gen.code_gen_from_data_sets import code_gen_read_csv_from_data_set
if TYPE_CHECKING:
    from ...code_gen_from_apps import AppCodeGen
    from shimoku_api_python.resources.report import Report


async def code_gen_from_table(
        self: 'AppCodeGen', report: 'Report', report_params: List[str], properties: dict
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
    data_arg = await code_gen_read_csv_from_data_set(data_set, change_data_set_name_with_report(data_set, report))
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