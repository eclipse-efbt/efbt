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
"""
DPM Pipeline Post-Editor - Runs DPM steps after the output layer mapping editor.

This script runs Steps 4-6 of the DPM workflow, which occur AFTER the human
editing step (output layer mapping editor in Step 3):

4. Create Transformation Rules (filters + joins metadata)
5. Generate Python Code (executable filters + joins)
5.5. Generate DPM Template Execution Code (report_cells + logic files)
6. Validate Pipeline Results

Prerequisites:
- Steps 1-3 must be completed (DPM metadata imported, output layers created/edited)
- Output layer mappings should be reviewed/edited via the web UI before running this

Usage:
    python run_dpm_pipeline_post_editor.py [options]

Options:
    --frameworks FRAMEWORK [FRAMEWORK ...]  Frameworks to process (default: COREP)
    --tables TABLE [TABLE ...]              Tables to process (default: C_07.00.a)
    --skip-validation                       Skip validation step
    --skip-code-generation                  Skip Python code generation (Steps 5 and 5.5)
"""
import django
import os
import sys
import argparse
import cProfile
from django.conf import settings
import logging

# Create a logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DjangoSetup:
    _initialized = False

    @classmethod
    def configure_django(cls):
        """Configure Django settings without starting the application"""
        if cls._initialized:
            return

        try:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
            sys.path.insert(0, project_root)
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

            # This allows us to use Django models without running the server
            django.setup()

            logger.info("Django configured successfully with settings module: %s",
                       os.environ['DJANGO_SETTINGS_MODULE'])
            cls._initialized = True
        except Exception as e:
            logger.error(f"Django configuration failed: {str(e)}")
            raise


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Run DPM pipeline steps after the output layer mapping editor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--frameworks',
        nargs='+',
        default=['COREP'],
        help='Frameworks to process (default: COREP)'
    )
    parser.add_argument(
        '--tables',
        nargs='+',
        default=['C_07.00.a'],
        help='Tables to process (default: C_07.00.a)'
    )
    parser.add_argument(
        '--skip-validation',
        action='store_true',
        help='Skip validation step'
    )
    parser.add_argument(
        '--skip-code-generation',
        action='store_true',
        help='Skip Python code generation (Steps 5 and 5.5)'
    )
    parser.add_argument(
        '--version',
        type=str,
        default='4_0',
        help='Framework version string (default: 4_0 for DPM v4.0)'
    )
    return parser.parse_args()


def run_step_4_transformation_rules(framework, version, is_dpm=True):
    """Step 4: Create Transformation Rules (filters + joins metadata).

    For DPM workflows, this step is simplified as output layers are already
    created in Step 3. The FINREP-specific transformation rules are skipped.
    """
    logger.info("=" * 60)
    logger.info(f"Step 4: Create Transformation Rules for {framework}")
    logger.info("=" * 60)

    if is_dpm:
        # For DPM workflows, output layers were already created in Step 3
        # using RunDPMOutputLayerCreation. Skip FINREP-specific processing.
        logger.info("  DPM mode: Output layers already created in Step 3")
        logger.info("  DPM mode: Skipping FINREP-specific transformation rules")
        logger.info("Step 4 completed: DPM transformation rules (via Step 3)")
        return

    # FINREP-specific processing (not used for DPM)
    from pybirdai.context.sdd_context_django import SDDContext
    from pybirdai.context.context import Context
    from pybirdai.process_steps.report_filters.create_output_layers import CreateOutputLayers
    from pybirdai.process_steps.report_filters.create_report_filters import CreateReportFilters
    from pybirdai.process_steps.joins_meta_data.create_joins_meta_data import JoinsMetaDataCreator
    from pybirdai.process_steps.joins_meta_data.main_category_finder import MainCategoryFinder

    base_dir = settings.BASE_DIR
    sdd_context = SDDContext()
    sdd_context.file_directory = os.path.join(base_dir, 'resources')
    sdd_context.output_directory = os.path.join(base_dir, 'results')

    context = Context()
    context.file_directory = sdd_context.file_directory
    context.output_directory = sdd_context.output_directory

    # Create output layers and filters
    logger.info("  Creating output layers and filters...")
    CreateOutputLayers().create_filters(context, sdd_context, framework, version)
    CreateReportFilters().create_report_filters(context, sdd_context, framework, version)

    # Create joins metadata
    logger.info("  Creating joins metadata...")
    MainCategoryFinder().create_report_to_main_category_maps(
        context, sdd_context, framework, [version]
    )
    JoinsMetaDataCreator().generate_joins_meta_data(context, sdd_context, framework)

    logger.info("Step 4 completed: Transformation rules created")


def run_step_5_generate_python_code():
    """Step 5: Generate Python Code (executable filters + joins)."""
    logger.info("=" * 60)
    logger.info("Step 5: Generate Python Code")
    logger.info("=" * 60)

    from pybirdai.entry_points.run_create_executable_filters import RunCreateExecutableFilters
    from pybirdai.entry_points.create_executable_joins import RunCreateExecutableJoins

    logger.info("  Generating executable filters...")
    RunCreateExecutableFilters.run_create_executable_filters_from_db()

    logger.info("  Generating executable joins...")
    RunCreateExecutableJoins.create_python_joins_from_db()

    logger.info("Step 5 completed: Python code generated")


def run_step_5_5_generate_dpm_template_code(table_codes, framework, version):
    """Step 5.5: Generate DPM Template Execution Code (report_cells + logic files)."""
    logger.info("=" * 60)
    logger.info(f"Step 5.5: Generate DPM Template Execution Code for {framework}")
    logger.info("=" * 60)

    from pybirdai.process_steps.code_generation.dpm_report_cells_generator import DPMReportCellsGenerator

    generator = DPMReportCellsGenerator()
    result = generator.generate_for_framework(
        framework=framework,
        version=version,
        table_codes=table_codes
    )

    logger.info(f"  Generated {result.get('logic_files_count', 0)} logic files")
    logger.info(f"  Generated report_cells file: {result.get('report_cells_file', 'N/A')}")
    logger.info("Step 5.5 completed: DPM template code generated")
    return result


def run_step_6_validate(table_codes, framework):
    """Step 6: Validate Pipeline Results."""
    logger.info("=" * 60)
    logger.info("Step 6: Validate Pipeline Results")
    logger.info("=" * 60)

    from pybirdai.models.bird_meta_data_model import (
        CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM,
        COMBINATION, COMBINATION_ITEM, CUBE_TO_COMBINATION,
        TABLE
    )

    validation_results = {
        'tables': {},
        'overall_status': 'success'
    }

    for table_code in table_codes:
        table_result = {
            'table_exists': False,
            'cube_count': 0,
            'cube_structure_count': 0,
            'combination_count': 0,
            'status': 'unknown'
        }

        # Check if table exists (field is 'code', not 'table_code')
        tables = TABLE.objects.filter(code=table_code)
        if tables.exists():
            table_result['table_exists'] = True
            table = tables.first()

            # Count related objects - search by table code pattern
            safe_code = table_code.replace('.', '_')
            cubes = CUBE.objects.filter(name__icontains=safe_code)
            table_result['cube_count'] = cubes.count()

            if cubes.exists():
                cube_structures = CUBE_STRUCTURE.objects.filter(cube__in=cubes)
                table_result['cube_structure_count'] = cube_structures.count()

                # Count combinations via CUBE_TO_COMBINATION
                cube_to_combos = CUBE_TO_COMBINATION.objects.filter(cube__in=cubes)
                table_result['combination_count'] = cube_to_combos.count()

            # Determine status
            if table_result['cube_count'] > 0 and table_result['combination_count'] > 0:
                table_result['status'] = 'success'
            elif table_result['cube_count'] > 0:
                table_result['status'] = 'partial'
            else:
                table_result['status'] = 'no_output_layer'
        else:
            table_result['status'] = 'table_not_found'
            validation_results['overall_status'] = 'error'

        validation_results['tables'][table_code] = table_result
        logger.info(f"  {table_code}: {table_result['status']} "
                   f"(cubes={table_result['cube_count']}, "
                   f"combinations={table_result['combination_count']})")

    # Check for generated files
    base_dir = settings.BASE_DIR
    dpm_output_dir = os.path.join(base_dir, 'results', 'generated_python_dpm', framework)
    if os.path.exists(dpm_output_dir):
        logic_files = [f for f in os.listdir(dpm_output_dir) if f.endswith('_logic.py')]
        validation_results['logic_files_count'] = len(logic_files)
        logger.info(f"  Generated logic files: {len(logic_files)}")
    else:
        validation_results['logic_files_count'] = 0
        logger.info("  No generated DPM output directory found")

    logger.info(f"Step 6 completed: Validation status = {validation_results['overall_status']}")
    return validation_results


if __name__ == "__main__":
    args = parse_arguments()

    DjangoSetup.configure_django()

    logger.info("=" * 60)
    logger.info("DPM Pipeline Post-Editor - Starting Execution")
    logger.info("Running Steps 4-6 (after output layer mapping editor)")
    logger.info(f"Frameworks: {args.frameworks}")
    logger.info(f"Tables: {args.tables}")
    logger.info(f"Version: {args.version}")
    logger.info("=" * 60)

    try:
        # Use first framework for now
        framework = args.frameworks[0]

        # Step 4: Create Transformation Rules
        run_step_4_transformation_rules(framework, args.version)

        if not args.skip_code_generation:
            # Step 5: Generate Python Code
            run_step_5_generate_python_code()

            # Step 5.5: Generate DPM Template Execution Code
            run_step_5_5_generate_dpm_template_code(args.tables, framework, args.version)

        if not args.skip_validation:
            # Step 6: Validate Pipeline Results
            validation_results = run_step_6_validate(args.tables, framework)

        logger.info("=" * 60)
        logger.info("DPM Pipeline Post-Editor - Execution Complete")
        logger.info("=" * 60)

    except Exception as e:
        logger.error("=" * 60)
        logger.error("DPM PIPELINE POST-EDITOR FAILED")
        logger.error("=" * 60)
        logger.error(f"Error: {str(e)}", exc_info=True)
        sys.exit(1)
