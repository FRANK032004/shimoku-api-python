import os
from typing import Optional, List
import subprocess

from shimoku_api_python.resources.business import Business
from shimoku_api_python.async_execution_pool import ExecutionPoolContext
from shimoku_api_python.utils import create_function_name, create_normalized_name

from shimoku_api_python.code_generation.file_generator import CodeGenFileHandler
from .apps_code_gen.code_gen_from_apps import AppCodeGen


class BusinessCodeGen:

    def __init__(self, business: Business, output_path: str, epc: ExecutionPoolContext):
        self._business = business
        self._output_path = f'{output_path}/{create_function_name(business["name"])}'
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
                app_code_gen = AppCodeGen(app, self._output_path, self.epc)
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
