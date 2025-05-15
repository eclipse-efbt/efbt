# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#

import os
import ast
import argparse
from logger_factory import return_logger, Path


class TestCodeGenerator:
    """
    A class for generating test code for regulatory templates.

    This class provides functionality to generate test code for regulatory templates by creating
    import statements, test functions, and additional test functions based on input parameters.
    """

    @staticmethod
    def setup_environment():
        """
        Set up the Django environment by configuring the settings module.

        This method sets the DJANGO_SETTINGS_MODULE environment variable and imports Django settings.
        """
        os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'
        from django.conf import settings

    @staticmethod
    def parse_arguments():
        """
        Parse command line arguments for test code generation.

        Returns:
            argparse.Namespace: Parsed command line arguments.
        """
        parser = argparse.ArgumentParser(description='Generate test code for regulatory templates')
        parser.add_argument('--dp-value', type=int, default=83491250,
                          help='Datapoint value to test (default: 83491250)')
        parser.add_argument('--reg-tid', type=str, default="F_05_01_REF_FINREP_3_0",
                          help='Regulatory template ID (default: F_05_01_REF_FINREP_3_0)')
        parser.add_argument('--dp-suffix', type=str, default="152589_REF",
                          help='Suffix for datapoint and cell IDs (default: 152589_REF)')
        parser.add_argument('--scenario', type=str, default="base",
                          help='Scenario name (default: base)')

        return parser.parse_args()

    @staticmethod
    def create_import_statements(cell_class):
        """
        Create import statements for the test code.

        Args:
            cell_class (str): Name of the cell class to import.

        Returns:
            str: Generated import statements as a string.
        """
        module = ast.Module(
            body=[
                ast.Import(names=[ast.alias(name='os', asname=None)]),
                ast.Import(names=[ast.alias(name='logging', asname=None)]),
                ast.Assign(
                    targets=[
                        ast.Subscript(
                            value=ast.Attribute(
                                value=ast.Name(id='os', ctx=ast.Load()),
                                attr='environ',
                                ctx=ast.Load()
                            ),
                            slice=ast.Constant(value='DJANGO_SETTINGS_MODULE'),
                            ctx=ast.Store()
                        )
                    ],
                    value=ast.Constant(value='birds_nest.settings')
                ),
                ast.ImportFrom(
                    module='django.conf',
                    names=[ast.alias(name='settings', asname=None)],
                    level=0
                ),
                ast.Import(names=[ast.alias(name='pytest', asname=None)]),
                ast.ImportFrom(
                    module='django.core.wsgi',
                    names=[ast.alias(name='get_wsgi_application', asname=None)],
                    level=0
                ),
                ast.Assign(
                    targets=[ast.Name(id='application', ctx=ast.Store())],
                    value=ast.Call(
                        func=ast.Name(id='get_wsgi_application', ctx=ast.Load()),
                        args=[],
                        keywords=[]
                    )
                ),
                ast.ImportFrom(
                    module='pybirdai.entry_points.execute_datapoint',
                    names=[ast.alias(name='RunExecuteDataPoint', asname=None)],
                    level=0
                ),
                ast.ImportFrom(
                    module='pybirdai.process_steps.pybird.execute_datapoint',
                    names=[ast.alias(name='ExecuteDataPoint', asname=None)],
                    level=0
                ),
                ast.ImportFrom(
                    module='pybirdai.process_steps.filter_code.report_cells',
                    names=[ast.alias(name=cell_class, asname=None)],
                    level=0
                )
            ],
            type_ignores=[]
        )

        return ast.unparse(ast.fix_missing_locations(module))

    @staticmethod
    def create_test_functions(datapoint_value, datapoint_id):
        """
        Create test functions for testing datapoint execution and lineage deletion.

        Args:
            datapoint_value (int): Value of the datapoint to test.
            datapoint_id (str): ID of the datapoint to test.

        Returns:
            str: Generated test functions as a string.
        """
        test_functions = ast.Module(
            body=[
                ast.FunctionDef(
                    name='test_execute_datapoint',
                    args=ast.arguments(
                        posonlyargs=[],
                        args=[
                            ast.arg(arg='value', annotation=ast.Name(id='int', ctx=ast.Load()))
                        ],
                        kwonlyargs=[],
                        kw_defaults=[],
                        defaults=[ast.Constant(value=datapoint_value)]
                    ),
                    body=[
                        ast.Assign(
                            targets=[ast.Name(id='data_point_id', ctx=ast.Store())],
                            value=ast.Constant(value=datapoint_id)
                        ),
                        ast.Assign(
                            targets=[ast.Name(id='result', ctx=ast.Store())],
                            value=ast.Call(
                                func=ast.Attribute(
                                    value=ast.Name(id='RunExecuteDataPoint', ctx=ast.Load()),
                                    attr='run_execute_data_point',
                                    ctx=ast.Load()
                                ),
                                args=[ast.Name(id='data_point_id', ctx=ast.Load())],
                                keywords=[]
                            )
                        ),
                        ast.Expr(
                            value=ast.Call(
                                func=ast.Attribute(
                                    value=ast.Name(id='ExecuteDataPoint', ctx=ast.Load()),
                                    attr='delete_lineage_data',
                                    ctx=ast.Load()
                                ),
                                args=[],
                                keywords=[]
                            )
                        ),
                        ast.Assert(
                            test=ast.Compare(
                                left=ast.Name(id='result', ctx=ast.Load()),
                                ops=[ast.Eq()],
                                comparators=[
                                    ast.Call(
                                        func=ast.Name(id='str', ctx=ast.Load()),
                                        args=[ast.Name(id='value', ctx=ast.Load())],
                                        keywords=[]
                                    )
                                ]
                            )
                        )
                    ],
                    decorator_list=[]
                )
                
            ],
            type_ignores=[]
        )

        return ast.unparse(ast.fix_missing_locations(test_functions))

    @staticmethod
    def create_additional_test_functions(cell_class, regulatory_template_id):
        """
        Create additional test functions for testing cell metrics and filtering.

        Args:
            cell_class (str): Name of the cell class to test.
            regulatory_template_id (str): ID of the regulatory template.

        Returns:
            str: Generated additional test functions as a string.
        """
        test_functions_additional = ast.Module(
            body=[
                ast.FunctionDef(
                    name='test_cell_metric',
                    args=ast.arguments(
                        posonlyargs=[],
                        args=[],
                        kwonlyargs=[],
                        kw_defaults=[],
                        defaults=[]
                    ),
                    body=[
                        ast.Assign(
                            targets=[ast.Name(id='cell', ctx=ast.Store())],
                            value=ast.Call(
                                func=ast.Name(id=cell_class, ctx=ast.Load()),
                                args=[],
                                keywords=[]
                            )
                        ),
                        ast.Expr(
                            value=ast.Call(
                                func=ast.Attribute(
                                    value=ast.Name(id='cell', ctx=ast.Load()),
                                    attr='init',
                                    ctx=ast.Load()
                                ),
                                args=[],
                                keywords=[]
                            )
                        ),
                        ast.Assign(
                            targets=[ast.Name(id='result', ctx=ast.Store())],
                            value=ast.Call(
                                func=ast.Attribute(
                                    value=ast.Name(id='cell', ctx=ast.Load()),
                                    attr='metric_value',
                                    ctx=ast.Load()
                                ),
                                args=[],
                                keywords=[]
                            )
                        ),
                        ast.Expr(
                            value=ast.Call(
                                func=ast.Attribute(
                                    value=ast.Name(id='ExecuteDataPoint', ctx=ast.Load()),
                                    attr='delete_lineage_data',
                                    ctx=ast.Load()
                                ),
                                args=[],
                                keywords=[]
                            )
                        ),
                        ast.Assert(
                            test=ast.Call(
                                func=ast.Name(id='isinstance', ctx=ast.Load()),
                                args=[
                                    ast.Name(id='result', ctx=ast.Load()),
                                    ast.Tuple(
                                        elts=[
                                            ast.Name(id='int', ctx=ast.Load()),
                                            ast.Name(id='float', ctx=ast.Load())
                                        ],
                                        ctx=ast.Load()
                                    )
                                ],
                                keywords=[]
                            )
                        )
                    ],
                    decorator_list=[]
                ),
                ast.FunctionDef(
                    name='test_cell_filter',
                    args=ast.arguments(
                        posonlyargs=[],
                        args=[],
                        kwonlyargs=[],
                        kw_defaults=[],
                        defaults=[]
                    ),
                    body=[
                        ast.Assign(
                            targets=[ast.Name(id='cell', ctx=ast.Store())],
                            value=ast.Call(
                                func=ast.Name(id=cell_class, ctx=ast.Load()),
                                args=[],
                                keywords=[]
                            )
                        ),
                        ast.Expr(
                            value=ast.Call(
                                func=ast.Attribute(
                                    value=ast.Name(id='cell', ctx=ast.Load()),
                                    attr='init',
                                    ctx=ast.Load()
                                ),
                                args=[],
                                keywords=[]
                            )
                        ),
                        ast.Expr(
                            value=ast.Call(
                                func=ast.Attribute(
                                    value=ast.Name(id='ExecuteDataPoint', ctx=ast.Load()),
                                    attr='delete_lineage_data',
                                    ctx=ast.Load()
                                ),
                                args=[],
                                keywords=[]
                            )
                        ),
                        ast.Assert(
                            test=ast.Call(
                                func=ast.Name(id='isinstance', ctx=ast.Load()),
                                args=[
                                    ast.Attribute(
                                        value=ast.Name(id='cell', ctx=ast.Load()),
                                        attr=f'{regulatory_template_id}s',
                                        ctx=ast.Load()
                                    ),
                                    ast.Name(id='list', ctx=ast.Load())
                                ],
                                keywords=[]
                            )
                        )
                    ],
                    decorator_list=[]
                )
            ],
            type_ignores=[]
        )

        return ast.unparse(ast.fix_missing_locations(test_functions_additional))

    @staticmethod
    def save_generated_code(output_file, import_code, test_code, logger):
        """
        Save the generated code to an output file.

        Args:
            output_file (str): Path to the output file.
            import_code (str): Import statements code.
            test_code (str): Test functions code.
            logger (logging.Logger): Logger instance for logging debug messages.
        """
        with open(output_file, 'w') as f:
            f.write(import_code)
            f.write('\n\n')
            f.write(test_code)
            
        logger.debug(f"Saved generated code to {output_file}")

    @classmethod
    def generate_test_code(cls):
        """
        Generate test code based on command line arguments.

        This method parses command line arguments, initializes variables,
        generates code components, and saves the generated code to a file.
        """
        logger = return_logger(str(Path(__file__).resolve()).rsplit("/",1)[-1])

        # Parse command line arguments
        args = cls.parse_arguments()
        logger.debug(f"Running with arguments: {args}")

        # Initialize variables from arguments
        datapoint_value = args.dp_value
        regulatory_template_id = args.reg_tid
        datapoint_id = f"{args.reg_tid}_{args.dp_suffix}"
        cell_class = f"Cell_{args.reg_tid}_{args.dp_suffix}"
        scenario_name = args.scenario

        logger.debug(f"Generating test code for cell class: {cell_class}")

        # Generate code components
        import_code = cls.create_import_statements(cell_class)
        logger.debug("Generated import code")

        test_code = cls.create_test_functions(datapoint_value, datapoint_id)
        logger.debug("Generated test functions")

        #test_code_additional = cls.create_additional_test_functions(cell_class, regulatory_template_id)
        #logger.debug("Generated additional test functions")

        # Save generated code
        output_file = os.path.join('tests', f'test_{cell_class.lower()}__{scenario_name}.py')
        cls.save_generated_code(output_file, import_code, test_code, logger)


def main():
    """
    Main function to set up the environment and generate test code.
    """
    TestCodeGenerator.setup_environment()
    TestCodeGenerator.generate_test_code()


if __name__ == "__main__":
    main()
