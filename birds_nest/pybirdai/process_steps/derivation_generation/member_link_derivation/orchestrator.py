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
Orchestrator for member link derivation generation.

This module provides the main entry point for generating derivation code
from member link CSV files. It coordinates the parsing, resolution, and
code generation steps.
"""

import logging
import os
from typing import Dict, List, Optional

from .generator import DerivationCodeGenerator
from .dataclasses import DerivationConfig, DerivationMapping
from .parser import MemberLinkParser, get_unique_source_variables
from .resolver import SpecificityResolver

logger = logging.getLogger(__name__)


def run_derivation_generation(
    csv_path: str = "resources/derivation_files/member_link_for_derivation.csv",
    output_dir: str = "resources/derivation_files/generated_from_member_links/",
    target_cube: str = "ANCRDT_INSTRMNT_C",
    target_variable: str = "TYP_INSTRMNT",
    output_variable: str = "TYP_INSTRMNT_ANCRDT",
    class_name: str = "INSTRMNT",
    verbose: bool = False
) -> str:
    """Generate derivation code from member link CSV.

    This is the main entry point for the derivation generation pipeline.
    It performs the following steps:
    1. Parse the member_link.csv file
    2. Filter entries for the target cube and variable
    3. Resolve specificity conflicts
    4. Generate Python derivation code
    5. Write the output file

    Args:
        csv_path: Path to the member_link.csv file
        output_dir: Directory to write generated code
        target_cube: The cube to filter for (e.g., ANCRDT_INSTRMNT_C)
        target_variable: The target variable to derive (e.g., TYP_INSTRMNT)
        output_variable: Name for the generated property (e.g., TYP_INSTRMNT_ANCRDT)
        class_name: Name of the class to generate the property for
        verbose: If True, print detailed progress information

    Returns:
        Path to the generated output file
    """
    # Check if cube_link derivations are allowed
    try:
        from pybirdai.context.context import Context
        if not Context.cube_link_derivations_allowed:
            logger.info("Cube link derivations are disabled (cube_link_derivations_allowed=False)")
            return ""
    except ImportError:
        pass  # If Context can't be imported, proceed anyway

    if verbose:
        logging.basicConfig(level=logging.INFO)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    # Create tmp file to ensure directory is tracked
    tmp_file = os.path.join(output_dir, 'tmp')
    if not os.path.exists(tmp_file):
        with open(tmp_file, 'w') as f:
            pass

    logger.info(f"Starting derivation generation for {output_variable}")
    logger.info(f"Source: {csv_path}")
    logger.info(f"Target cube: {target_cube}, Target variable: {target_variable}")

    # Create configuration
    config = DerivationConfig(
        csv_path=csv_path,
        output_dir=output_dir,
        target_cube=target_cube,
        target_variable=target_variable,
        output_variable_name=output_variable,
        class_name=class_name
    )

    # Step 1: Parse CSV
    logger.info("Step 1: Parsing member_link.csv...")
    parser = MemberLinkParser(csv_path)
    entries = parser.parse_filtered(
        target_cube=target_cube,
        target_variable=target_variable,
        only_active=True
    )
    logger.info(f"Found {len(entries)} matching entries")

    if not entries:
        logger.warning(f"No entries found for {target_cube}/{target_variable}")
        return ""

    # Log unique source variables found
    source_vars = get_unique_source_variables(entries)
    logger.info(f"Source variables: {source_vars}")

    # Step 2: Resolve specificity conflicts
    logger.info("Step 2: Resolving specificity conflicts...")
    resolver = SpecificityResolver()
    resolved_mappings = resolver.resolve_conflicts(entries)
    logger.info(f"Created {len(resolved_mappings)} derivation mappings")

    # Log any conflicts that were resolved
    if resolver.conflict_log:
        logger.info(f"Resolved {len(resolver.conflict_log)} conflicts:")
        for conflict in resolver.conflict_log[:10]:  # Show first 10
            logger.info(f"  {conflict}")
        if len(resolver.conflict_log) > 10:
            logger.info(f"  ... and {len(resolver.conflict_log) - 10} more")

    # Step 3: Merge mappings by source variable
    logger.info("Step 3: Merging mappings by source variable...")
    merged_mappings = resolver.merge_mappings_by_variable(resolved_mappings)
    logger.info(f"Merged into {len(merged_mappings)} variable mappings")

    # Step 4: Generate code
    logger.info("Step 4: Generating derivation code...")
    generator = DerivationCodeGenerator(config)
    output_path = os.path.join(
        output_dir,
        f"derived_{output_variable.lower()}.py"
    )
    code = generator.generate_file(merged_mappings, output_path)
    logger.info(f"Generated {len(code)} bytes of code")

    # Step 5: Write output
    logger.info(f"Output written to: {output_path}")

    return output_path


def run_derivation_generation_for_all_variables(
    csv_path: str = "resources/derivation_files/member_link_for_derivation.csv",
    output_dir: str = "resources/derivation_files/generated_from_member_links/",
    target_cube: str = "ANCRDT_INSTRMNT_C",
    verbose: bool = False
) -> List[str]:
    """Generate derivation code for all target variables in a cube.

    This function discovers all unique target variables in the cube
    and generates derivation code for each one.

    Args:
        csv_path: Path to the member_link.csv file
        output_dir: Directory to write generated code
        target_cube: The cube to process
        verbose: If True, print detailed progress information

    Returns:
        List of paths to generated output files
    """
    # Check if cube_link derivations are allowed
    try:
        from pybirdai.context.context import Context
        if not Context.cube_link_derivations_allowed:
            logger.info("Cube link derivations are disabled (cube_link_derivations_allowed=False)")
            return []
    except ImportError:
        pass  # If Context can't be imported, proceed anyway

    if verbose:
        logging.basicConfig(level=logging.INFO)

    logger.info(f"=== Member Link Derivation Generation ===")
    logger.info(f"CSV path: {csv_path}")
    logger.info(f"Output dir: {output_dir}")
    logger.info(f"Target cube: {target_cube}")

    # Check if CSV file exists
    if not os.path.exists(csv_path):
        logger.error(f"CSV file does not exist: {csv_path}")
        return []

    # Log file size
    file_size = os.path.getsize(csv_path)
    logger.info(f"CSV file size: {file_size} bytes")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    # Create tmp file to ensure directory is tracked
    tmp_file = os.path.join(output_dir, 'tmp')
    if not os.path.exists(tmp_file):
        with open(tmp_file, 'w') as f:
            pass

    logger.info(f"Discovering target variables for {target_cube}...")

    # Parse and find all unique target variables
    try:
        parser = MemberLinkParser(csv_path)
        entries = parser.parse_filtered(
            target_cube=target_cube,
            only_active=True
        )
        logger.info(f"Parsed {len(entries)} entries matching target_cube={target_cube}")
    except Exception as e:
        logger.error(f"Failed to parse CSV: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []

    if not entries:
        # Try parsing without target_cube filter to see what cubes exist
        all_entries = parser.parse_filtered(only_active=True)
        all_cubes = sorted(set(e.target_cube for e in all_entries if e.target_cube))
        logger.warning(f"No entries found for target_cube={target_cube}")
        logger.info(f"Available cubes in CSV: {all_cubes}")
        return []

    # Get unique target variables
    target_vars = sorted(set(e.target_variable for e in entries if e.target_variable))
    logger.info(f"Found {len(target_vars)} target variables: {target_vars}")

    # Derive class name from target cube (strip ANCRDT_ prefix and _C suffix)
    # e.g., ANCRDT_INSTRMNT_C -> INSTRMNT
    class_name = target_cube
    if class_name.startswith("ANCRDT_"):
        class_name = class_name[7:]  # Remove "ANCRDT_" prefix
    if class_name.endswith("_C"):
        class_name = class_name[:-2]  # Remove "_C" suffix
    logger.info(f"Derived class name: {class_name} (from target_cube={target_cube})")

    output_files = []
    for target_var in target_vars:
        # Output variable has _ANCRDT suffix to indicate ANCRDT-derived field
        output_var = f"{target_var}_ANCRDT"
        try:
            output_path = run_derivation_generation(
                csv_path=csv_path,
                output_dir=output_dir,
                target_cube=target_cube,
                target_variable=target_var,
                output_variable=output_var,
                class_name=class_name,
                verbose=verbose
            )
            if output_path:
                output_files.append(output_path)
        except Exception as e:
            logger.error(f"Failed to generate derivation for {target_var}: {e}")

    return output_files


def preview_derivation(
    csv_path: str = "results/ancrdt_csv/member_link.csv",
    target_cube: str = "ANCRDT_INSTRMNT_C",
    target_variable: str = "TYP_INSTRMNT"
) -> Dict[str, DerivationMapping]:
    """Preview derivation mappings without generating code.

    This is useful for inspecting what mappings would be generated
    before actually creating the output file.

    Args:
        csv_path: Path to the member_link.csv file
        target_cube: The cube to filter for
        target_variable: The target variable to derive

    Returns:
        Dictionary of merged DerivationMapping objects by source variable
    """
    parser = MemberLinkParser(csv_path)
    entries = parser.parse_filtered(
        target_cube=target_cube,
        target_variable=target_variable,
        only_active=True
    )

    resolver = SpecificityResolver()
    resolved_mappings = resolver.resolve_conflicts(entries)
    merged_mappings = resolver.merge_mappings_by_variable(resolved_mappings)

    return merged_mappings


def print_mapping_summary(mappings: Dict[str, DerivationMapping]):
    """Print a summary of derivation mappings.

    Args:
        mappings: Dictionary of source variable names to DerivationMapping
    """
    print(f"\n{'='*60}")
    print(f"Derivation Mapping Summary")
    print(f"{'='*60}")

    for var_name, mapping in sorted(mappings.items(), key=lambda x: -x[1].specificity_score):
        print(f"\n{var_name} (specificity: {mapping.specificity_score})")
        print(f"  Source table: {mapping.source_table}")
        print(f"  Target variable: {mapping.target_variable}")
        print(f"  Mappings: {len(mapping.mappings)}")

        # Show first 5 mappings
        for i, (inp, out) in enumerate(sorted(mapping.mappings.items())[:5]):
            print(f"    {inp} -> {out}")
        if len(mapping.mappings) > 5:
            print(f"    ... and {len(mapping.mappings) - 5} more")

    print(f"\n{'='*60}")


# CLI entry point
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate derivation code from member link CSV"
    )
    parser.add_argument(
        "--csv-path",
        default="results/ancrdt_csv/member_link.csv",
        help="Path to member_link.csv"
    )
    parser.add_argument(
        "--output-dir",
        default="resources/derivation_files/",
        help="Output directory for generated code"
    )
    parser.add_argument(
        "--target-cube",
        default="ANCRDT_INSTRMNT_C",
        help="Target cube to filter for"
    )
    parser.add_argument(
        "--target-variable",
        default="TYP_INSTRMNT",
        help="Target variable to derive"
    )
    parser.add_argument(
        "--output-variable",
        default="TYP_INSTRMNT_ANCRDT",
        help="Name for the generated property"
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Preview mappings without generating code"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )

    args = parser.parse_args()

    if args.preview:
        mappings = preview_derivation(
            csv_path=args.csv_path,
            target_cube=args.target_cube,
            target_variable=args.target_variable
        )
        print_mapping_summary(mappings)
    else:
        output_path = run_derivation_generation(
            csv_path=args.csv_path,
            output_dir=args.output_dir,
            target_cube=args.target_cube,
            target_variable=args.target_variable,
            output_variable=args.output_variable,
            verbose=args.verbose
        )
        print(f"Generated: {output_path}")
