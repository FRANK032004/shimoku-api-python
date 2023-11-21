from typing import TYPE_CHECKING, List
if TYPE_CHECKING:
    from ...code_gen_from_apps import AppCodeGen


async def code_gen_from_indicator(
        self: 'AppCodeGen', report_params: List[str], properties: dict
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