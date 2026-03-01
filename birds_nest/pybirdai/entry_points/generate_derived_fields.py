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
Entry point for generating derived fields from ECB logical transformation rules.

This module provides functions to:
1. Download logical transformation rules from ECB website
2. Generate Python derivation files from the rules
3. Merge derived fields into the bird data model

Usage:
    from pybirdai.entry_points.generate_derived_fields import (
        run_download_transformation_rules,
        run_generate_derivation_files,
        run_full_derivation_pipeline
    )

    # Download rules from ECB
    run_download_transformation_rules()

    # Generate derivation files
    run_generate_derivation_files()

    # Or run the full pipeline
    run_full_derivation_pipeline()
"""

import os
import logging
from django.conf import settings


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Default paths
# Note: ECB API returns 'logical_transformation_rule.csv' (not sddlogicaltransformationrule.csv)
DEFAULT_TRANSFORMATION_RULES_CSV = "artefacts/smcubes_artefacts/logical_transformation_rule.csv"
DEFAULT_DERIVATION_CONFIG_CSV = "resources/derivation_files/derivation_config.csv"
DEFAULT_GENERATED_OUTPUT_DIR = "resources/derivation_files/generated_from_logical_transformation_rules"
DEFAULT_BIRD_DATA_MODEL = "pybirdai/models/bird_data_model.py"


def run_download_transformation_rules(output_dir: str = "artefacts/smcubes_artefacts") -> str:
    """
    Download logical transformation rules from the ECB BIRD website.

    Args:
        output_dir: Directory to save the downloaded CSV file.

    Returns:
        Path to the downloaded CSV file.
    """
    from pybirdai.utils.bird_ecb_website_fetcher import BirdEcbWebsiteClient

    logger.info("Downloading logical transformation rules from ECB...")

    client = BirdEcbWebsiteClient()
    csv_path = client.request_logical_transformation_rules(output_dir=output_dir)

    if os.path.exists(csv_path):
        logger.info(f"Successfully downloaded transformation rules to: {csv_path}")
    else:
        logger.warning(f"Expected CSV file not found at: {csv_path}")

    return csv_path


def run_generate_derivation_files(
    transformation_rules_csv: str = DEFAULT_TRANSFORMATION_RULES_CSV,
    output_dir: str = DEFAULT_GENERATED_OUTPUT_DIR
) -> dict:
    """
    Generate Python derivation files from ECB transformation rules.

    This function generates Python files for ALL DER-type rules in the CSV.
    The config file is NOT used here - it's only used during merge to
    determine which derived fields to include in the bird data model.

    Args:
        transformation_rules_csv: Path to the logical_transformation_rule.csv file.
        output_dir: Directory for generated Python files.

    Returns:
        Dictionary mapping class names to generated file paths.
    """
    from pybirdai.process_steps.derivation_generation import generate_all_derivation_files

    logger.info("Generating derivation files from transformation rules...")

    # Check if input file exists
    if not os.path.exists(transformation_rules_csv):
        logger.error(f"Transformation rules CSV not found: {transformation_rules_csv}")
        logger.info("Run run_download_transformation_rules() first to download the CSV.")
        return {}

    generated_files = generate_all_derivation_files(
        transformation_rules_csv=transformation_rules_csv,
        output_dir=output_dir
    )

    logger.info(f"Generated {len(generated_files)} derivation file(s)")
    return generated_files


def run_list_available_rules(
    transformation_rules_csv: str = DEFAULT_TRANSFORMATION_RULES_CSV
) -> dict:
    """
    List all available derived field rules from the transformation rules CSV.

    This is useful for discovering what fields are available to configure
    in derivation_config.csv.

    Args:
        transformation_rules_csv: Path to the sddlogicaltransformationrule.csv file.

    Returns:
        Dictionary mapping class names to lists of available field names.
    """
    from pybirdai.process_steps.derivation_generation import (
        get_available_derivation_rules
    )

    if not os.path.exists(transformation_rules_csv):
        logger.error(f"Transformation rules CSV not found: {transformation_rules_csv}")
        return {}

    available = get_available_derivation_rules(transformation_rules_csv)

    logger.info(f"Found {len(available)} classes with derived fields:")
    for class_name, fields in sorted(available.items()):
        logger.info(f"  {class_name}: {len(fields)} field(s)")
        for field in fields[:5]:  # Show first 5 fields
            logger.info(f"    - {field}")
        if len(fields) > 5:
            logger.info(f"    ... and {len(fields) - 5} more")

    return available


def run_merge_derived_fields(
    bird_data_model_path: str = DEFAULT_BIRD_DATA_MODEL
) -> bool:
    """
    Merge derived fields from all sources into the bird data model.

    This merges both manual implementations (derived_field_configuration.py)
    and auto-generated files (generated/*_derived.py) into bird_data_model.py.

    Manual implementations take precedence over auto-generated ones.

    Args:
        bird_data_model_path: Path to the bird_data_model.py file.

    Returns:
        True if modifications were made, False otherwise.
    """
    from pybirdai.utils.derived_fields_extractor import merge_all_derived_fields_into_model

    logger.info("Merging derived fields into bird data model...")

    result = merge_all_derived_fields_into_model(bird_data_model_path)

    if result:
        logger.info("Successfully merged derived fields")
    else:
        logger.info("No modifications made (file may already be modified)")

    return result


def run_full_derivation_pipeline(
    download: bool = True,
    generate: bool = True,
    merge: bool = False,
    output_dir: str = "artefacts/smcubes_artefacts",
    config_csv: str = DEFAULT_DERIVATION_CONFIG_CSV,
    generated_output_dir: str = DEFAULT_GENERATED_OUTPUT_DIR,
    bird_data_model_path: str = DEFAULT_BIRD_DATA_MODEL
) -> dict:
    """
    Run the complete derivation generation pipeline.

    Steps:
    1. Download transformation rules from ECB (optional)
    2. Generate derivation files (optional)
    3. Merge into bird data model (optional)

    Args:
        download: Whether to download transformation rules from ECB.
        generate: Whether to generate derivation files.
        merge: Whether to merge derived fields into bird data model.
        output_dir: Directory for downloaded CSV files.
        config_csv: Path to derivation configuration CSV.
        generated_output_dir: Directory for generated Python files.
        bird_data_model_path: Path to bird_data_model.py.

    Returns:
        Dictionary with results of each step.
    """
    results = {
        'download': None,
        'generate': None,
        'merge': None
    }

    logger.info("Starting derivation generation pipeline...")

    # Step 1: Download
    if download:
        try:
            csv_path = run_download_transformation_rules(output_dir=output_dir)
            results['download'] = csv_path
        except Exception as e:
            logger.error(f"Failed to download transformation rules: {e}")
            results['download'] = str(e)

    # Step 2: Generate
    if generate:
        try:
            transformation_rules_csv = os.path.join(
                output_dir, "logical_transformation_rule.csv"
            )
            generated = run_generate_derivation_files(
                transformation_rules_csv=transformation_rules_csv,
                output_dir=generated_output_dir
            )
            results['generate'] = generated
        except Exception as e:
            logger.error(f"Failed to generate derivation files: {e}")
            results['generate'] = str(e)

    # Step 3: Merge
    if merge:
        try:
            merged = run_merge_derived_fields(bird_data_model_path=bird_data_model_path)
            results['merge'] = merged
        except Exception as e:
            logger.error(f"Failed to merge derived fields: {e}")
            results['merge'] = str(e)

    logger.info("Pipeline completed")
    return results


def export_available_rules_to_config(
    transformation_rules_csv: str = DEFAULT_TRANSFORMATION_RULES_CSV,
    config_csv: str = DEFAULT_DERIVATION_CONFIG_CSV,
    enabled_by_default: bool = False
) -> str:
    """
    Export all available derivation rules to a config CSV file.

    This creates or updates the derivation_config.csv with all available
    derived fields from the transformation rules CSV.

    Args:
        transformation_rules_csv: Path to the sddlogicaltransformationrule.csv file.
        config_csv: Path to output derivation_config.csv file.
        enabled_by_default: Whether to enable all fields by default.

    Returns:
        Path to the created config file.
    """
    import csv
    from pybirdai.process_steps.derivation_generation import get_available_derivation_rules

    if not os.path.exists(transformation_rules_csv):
        logger.error(f"Transformation rules CSV not found: {transformation_rules_csv}")
        return ""

    available = get_available_derivation_rules(transformation_rules_csv)

    # Ensure directory exists
    os.makedirs(os.path.dirname(config_csv), exist_ok=True)

    # Write config CSV
    with open(config_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['class_name', 'field_name', 'enabled', 'notes'])

        # Add header comment
        writer.writerow([
            '# Configuration file for derived field generation from ECB logical transformation rules.',
            '', '', ''
        ])

        for class_name in sorted(available.keys()):
            for field_name in sorted(available[class_name]):
                enabled_str = 'true' if enabled_by_default else 'false'
                writer.writerow([class_name, field_name, enabled_str, ''])

    logger.info(f"Exported {sum(len(f) for f in available.values())} rules to: {config_csv}")
    return config_csv
