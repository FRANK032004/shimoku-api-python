from typing import TYPE_CHECKING, List
from shimoku_api_python.utils import change_data_set_name_with_report
from ...data_sets_code_gen.code_gen_from_data_sets import code_gen_read_csv_from_data_set
if TYPE_CHECKING:
    from ...code_gen_from_apps import AppCodeGen
    from shimoku_api_python.resources.report import Report


def compact_labels_info(columns: list[dict], mapping: dict) -> dict:
    """ Compact label info.
    :return: compacted label info
    """
    label_columns = {}
    for column in columns:
        if 'chips' not in column:
            continue
        chips_dict = column['chips']
        variant = chips_dict['variant']
        mapping_from_field = mapping[column['field']]
        entry_key = mapping_from_field
        chips_options = chips_dict['options']
        if variant != 'filled':
            entry_key = (entry_key, variant)
        entry_value = {}
        if mapping_from_field.startswith('intField'):
            chips_options = sorted(chips_options, key=lambda x: int(x['value']))
            current_range = (0, chips_options[0]['value'])
            current_color = chips_options[0]['backgroundColor']
            first_index = 0
            i = 1
            for chip in chips_options[1:]:
                if chip['backgroundColor'] != current_color:
                    if i == first_index + 1:
                        entry_value[current_range[1]] = current_color
                    else:
                        entry_value[current_range] = current_color
                    first_index = i
                    current_range = (current_range[1], chip['value'])
                    current_color = chip['backgroundColor']
                else:
                    current_range = (current_range[0], chip['value'])
                i += 1
            if first_index == len(chips_options) - 1:
                entry_value[current_range[1]] = current_color
            else:
                entry_value[(current_range[0], int(current_range[1]) + 1)] = current_color
        else:
            entry_value = {chip['value']: chip['backgroundColor'] for chip in chips_options}
        first_color = chips_options[0]['backgroundColor']
        if all(v == first_color for v in entry_value.values()):
            entry_value = first_color
        label_columns[entry_key] = entry_value
    return label_columns


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
    label_columns = compact_labels_info(properties['columns'], mapping)
    if label_columns:
        label_columns_code_lines = self._code_gen_from_dict(label_columns, 4)
        table_params.extend([f'    label_columns={label_columns_code_lines[0][4:]}', *label_columns_code_lines[1:]])
    return [
        'shimoku_client.plt.table(',
        f'    data={data_arg},',
        *report_params,
        *table_params,
        ')'
    ]
