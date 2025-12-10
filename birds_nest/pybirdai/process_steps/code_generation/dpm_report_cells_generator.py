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
DPM Report Cells Generator - Generates dpm_{framework}_report_cells.py files.

This generator creates Python code for DPM regulatory templates following
the FINREP pattern with inheritance to avoid code duplication.

Hierarchy:
- ReportTable (table-level base) - defines framework, table code, dimensions
- Submap (cells with identical combination_items) - defines filter logic
- Cell (only defines ROW/COL position) - minimal code, inherits filtering

This approach ensures that when cells share the same combination_items
(common in DPM templates), the filter logic is defined only once in
the Submap class.
"""

import os
import logging
import shutil
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

from .dpm_logic_generator import (
    DPMLogicGenerator, TableInfo, DimensionInfo,
    CombinationInfo, CombinationItemInfo
)

logger = logging.getLogger(__name__)


@dataclass
class CellPosition:
    """Position of a cell in the template grid."""
    row: str
    col: str
    combination_id: str


@dataclass
class SubmapInfo:
    """Information about a submap (group of cells with same combination structure)."""
    submap_id: int
    signature: str
    combination_items: List[CombinationItemInfo]
    cells: List[CellPosition] = field(default_factory=list)


class DPMReportCellsGenerator:
    """
    Generates dpm_{framework}_report_cells.py files for DPM templates.

    This generator:
    1. Generates *_logic.py files for each table using DPMLogicGenerator
    2. Generates a main dpm_{framework}_report_cells.py file that:
       - Imports all logic files
       - Defines table-level base classes
       - Defines submap classes (inherit from table base)
       - Defines cell classes (inherit from submap, only define position)
    """

    def __init__(self):
        self.logic_generator = DPMLogicGenerator()

    def generate_for_framework(
        self,
        framework: str,
        version: str,
        table_codes: List[str],
        output_base_dir: Optional[str] = None
    ) -> Dict:
        """
        Generate all code for a framework.

        Args:
            framework: Framework name (e.g., 'COREP')
            version: Version string (e.g., 'COREP_3')
            table_codes: List of table codes to process
            output_base_dir: Base directory for output (defaults to results/generated_python_dpm)

        Returns:
            Dict with generation results
        """
        from django.conf import settings

        if output_base_dir is None:
            output_base_dir = os.path.join(
                settings.BASE_DIR, 'results', 'generated_python_dpm'
            )

        framework_dir = os.path.join(output_base_dir, framework)
        logic_dir = os.path.join(framework_dir, 'logic')
        os.makedirs(logic_dir, exist_ok=True)

        results = {
            'status': 'success',
            'framework': framework,
            'version': version,
            'logic_files': [],
            'logic_files_count': 0,
            'report_cells_file': None,
            'errors': []
        }

        # Step 1: Generate logic files for each table
        table_infos = []
        for table_code in table_codes:
            try:
                logic_result = self.logic_generator.generate_for_table(
                    table_code=table_code,
                    framework=framework,
                    version=version,
                    output_dir=logic_dir
                )

                if logic_result.get('status') == 'success':
                    results['logic_files'].append(logic_result['file'])

                    # Fetch table info for report_cells generation
                    table_info = self.logic_generator._fetch_table_info(
                        table_code, framework, version
                    )
                    if table_info:
                        table_infos.append(table_info)
                else:
                    results['errors'].append({
                        'table_code': table_code,
                        'error': logic_result.get('message', 'Unknown error')
                    })

            except Exception as e:
                logger.error(f"Error generating logic for {table_code}: {e}")
                results['errors'].append({
                    'table_code': table_code,
                    'error': str(e)
                })

        results['logic_files_count'] = len(results['logic_files'])

        # Step 2: Generate the main report_cells file
        try:
            report_cells_file = self._generate_report_cells_file(
                framework=framework,
                version=version,
                table_infos=table_infos,
                output_dir=framework_dir
            )
            results['report_cells_file'] = report_cells_file

            # Step 3: Copy to filter_code directory for runtime use
            self._copy_to_filter_code(framework, framework_dir)

        except Exception as e:
            logger.error(f"Error generating report_cells: {e}")
            results['errors'].append({
                'component': 'report_cells',
                'error': str(e)
            })
            results['status'] = 'partial'

        if results['errors'] and not results['logic_files']:
            results['status'] = 'error'

        return results

    def _generate_report_cells_file(
        self,
        framework: str,
        version: str,
        table_infos: List[TableInfo],
        output_dir: str
    ) -> str:
        """Generate the main dpm_{framework}_report_cells.py file."""
        lines = []

        # File header
        lines.extend([
            "# coding=UTF-8",
            "# Auto-generated DPM report cells file",
            f"# Framework: {framework}",
            f"# Version: {version}",
            f"# Tables: {[t.table_code for t in table_infos]}",
            "",
            "from pybirdai.models.bird_data_model import *",
            "from pybirdai.process_steps.pybird.orchestration import Orchestration",
            "from pybirdai.annotations.decorators import lineage",
            "",
        ])

        # Import logic files
        lines.append("# Import table-specific logic files")
        for table_info in table_infos:
            safe_code = table_info.table_code.replace('.', '_').replace(' ', '_')
            module_name = f"{safe_code}_{framework}_{version}_logic"
            lines.append(f"from .logic.{module_name} import *")

        lines.append("")
        lines.append("")

        # Generate table-level base classes, submaps, and cells for each table
        for table_info in table_infos:
            lines.extend(self._generate_table_classes(table_info, framework, version))

        # Write file
        filename = f"dpm_{framework.lower()}_report_cells.py"
        filepath = os.path.join(output_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))

        logger.info(f"Generated report_cells file: {filepath}")
        return filepath

    def _generate_table_classes(
        self,
        table_info: TableInfo,
        framework: str,
        version: str
    ) -> List[str]:
        """Generate all classes for a single table."""
        lines = []
        safe_code = table_info.table_code.replace('.', '_').replace(' ', '_')
        class_prefix = f"{safe_code}_{framework}_{version}"

        # Section header
        lines.extend([
            "#" + "=" * 70,
            f"# Table: {table_info.table_code}",
            "#" + "=" * 70,
            ""
        ])

        # Generate ReportTable base class
        lines.extend(self._generate_report_table_class(table_info, class_prefix))

        # Group combinations by signature for submap generation
        submap_groups = self._group_combinations_by_signature(table_info.combinations)

        # Generate Submap classes (inherit from ReportTable)
        lines.extend(self._generate_submap_cell_classes(
            table_info, class_prefix, submap_groups
        ))

        lines.append("")
        return lines

    def _generate_report_table_class(
        self,
        table_info: TableInfo,
        class_prefix: str
    ) -> List[str]:
        """Generate the table-level base class."""
        lines = [
            f"class {class_prefix}_ReportTable:",
            f'\t"""',
            f"\tBase class for table {table_info.table_code}.",
            f"\tDefines framework, table code, and dimension structure.",
            f'\t"""',
            f"\tFRAMEWORK = '{table_info.framework}'",
            f"\tTABLE_CODE = '{table_info.table_code}'",
            f"\tVERSION = '{table_info.version}'",
            "",
        ]

        # Define dimensions
        if table_info.dimensions:
            lines.append("\t# Dimensions")
            lines.append("\tDIMENSIONS = [")
            for dim in table_info.dimensions:
                safe_name = self._safe_identifier(dim.variable_name)
                lines.append(f"\t\t'{safe_name}',  # {dim.variable_id}")
            lines.append("\t]")
            lines.append("")

        # Add reference to union table
        lines.extend([
            f"\t{class_prefix}_UnionTable_ref = None  # Reference to UnionTable",
            "",
            "\tdef get_union_items(self):",
            "\t\t'''Get all union items from the referenced table.'''",
            f"\t\tif self.{class_prefix}_UnionTable_ref is not None:",
            f"\t\t\treturn self.{class_prefix}_UnionTable_ref.{class_prefix}_UnionItems",
            "\t\treturn []",
            "",
            "\tdef init(self):",
            "\t\t'''Initialize with orchestration.'''",
            "\t\tOrchestration().init(self)",
            "\t\treturn None",
            ""
        ])

        lines.append("")
        return lines

    def _group_combinations_by_signature(
        self,
        combinations: List[CombinationInfo]
    ) -> Dict[str, SubmapInfo]:
        """Group combinations by their signature."""
        groups: Dict[str, SubmapInfo] = {}

        for i, combo in enumerate(combinations):
            signature = combo.get_signature()

            if signature not in groups:
                groups[signature] = SubmapInfo(
                    submap_id=len(groups),
                    signature=signature,
                    combination_items=combo.items.copy(),
                    cells=[]
                )

            # Add cell position (using combination_id as both row and col for now)
            groups[signature].cells.append(CellPosition(
                row=f"R{i:04d}",
                col="C0010",
                combination_id=combo.combination_id
            ))

        return groups

    def _generate_submap_cell_classes(
        self,
        table_info: TableInfo,
        class_prefix: str,
        submap_groups: Dict[str, SubmapInfo]
    ) -> List[str]:
        """Generate Submap classes and their Cell subclasses."""
        lines = []

        for signature, submap in submap_groups.items():
            submap_class_name = f"{class_prefix}_Submap_{submap.submap_id}"

            # Generate Submap class (inherits from ReportTable)
            lines.extend([
                f"class {submap_class_name}({class_prefix}_ReportTable):",
                f'\t"""',
                f"\tSubmap with {len(submap.cells)} cells sharing combination structure.",
                f'\t"""',
                "",
            ])

            # Define combination items
            if submap.combination_items:
                lines.append("\t# Shared combination items for all cells in this submap")
                lines.append("\tCOMBINATION_ITEMS = {")
                for item in submap.combination_items:
                    safe_var = self._safe_identifier(item.variable_name)
                    lines.append(f"\t\t'{safe_var}': '{item.member_code}',")
                lines.append("\t}")
                lines.append("")

            # Reference to filtered items
            lines.append(f"\t{class_prefix}_filtered_items = []")
            lines.append("")

            # Metric value method
            lines.extend([
                "\t@lineage(dependencies={})",
                "\tdef metric_value(self):",
                "\t\t'''Calculate the metric value for this cell.'''",
                "\t\ttotal = 0",
                f"\t\tfor item in self.{class_prefix}_filtered_items:",
                "\t\t\t# Sum the measure values",
                "\t\t\tif hasattr(item, 'MTRC'):",
                "\t\t\t\tval = item.MTRC()",
                "\t\t\t\tif isinstance(val, (int, float)):",
                "\t\t\t\t\ttotal += val",
                "\t\treturn total",
                ""
            ])

            # Filter method based on combination items
            lines.extend([
                "\t@lineage(dependencies={})",
                "\tdef calc_filtered_items(self):",
                "\t\t'''Filter items based on combination criteria.'''",
                "\t\titems = self.get_union_items()",
                "\t\tfiltered = []",
                "\t\tfor item in items:",
                "\t\t\tfilter_passed = True",
            ])

            # Generate filter conditions
            if submap.combination_items:
                for item in submap.combination_items:
                    safe_var = self._safe_identifier(item.variable_name)
                    lines.extend([
                        f"\t\t\t# Filter on {safe_var}",
                        f"\t\t\tif hasattr(item, '{safe_var}'):",
                        f"\t\t\t\tif item.{safe_var}() != '{item.member_code}':",
                        "\t\t\t\t\tfilter_passed = False",
                    ])

            lines.extend([
                "\t\t\tif filter_passed:",
                "\t\t\t\tfiltered.append(item)",
                f"\t\tself.{class_prefix}_filtered_items = filtered",
                ""
            ])

            # Init method
            lines.extend([
                "\tdef init(self):",
                "\t\t'''Initialize the submap with filtering.'''",
                "\t\tOrchestration().init(self)",
                f"\t\tself.{class_prefix}_filtered_items = []",
                "\t\tself.calc_filtered_items()",
                "\t\treturn None",
                ""
            ])

            lines.append("")

            # Generate Cell classes (inherit from Submap, minimal code)
            for cell in submap.cells:
                cell_class_name = f"Cell_{class_prefix}_{cell.combination_id}"
                # Sanitize class name
                cell_class_name = self._safe_identifier(cell_class_name)

                lines.extend([
                    f"class {cell_class_name}({submap_class_name}):",
                    f'\t"""Cell at position ({cell.row}, {cell.col})."""',
                    f"\tROW = '{cell.row}'",
                    f"\tCOL = '{cell.col}'",
                    f"\tCOMBINATION_ID = '{cell.combination_id}'",
                    ""
                ])

        return lines

    def _copy_to_filter_code(self, framework: str, source_dir: str):
        """Copy generated files to filter_code directory for runtime use."""
        from django.conf import settings

        filter_code_dir = os.path.join(
            settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code'
        )

        # Copy main report_cells file
        src_file = os.path.join(source_dir, f"dpm_{framework.lower()}_report_cells.py")
        if os.path.exists(src_file):
            dst_file = os.path.join(filter_code_dir, f"dpm_{framework.lower()}_report_cells.py")
            shutil.copy2(src_file, dst_file)
            logger.info(f"Copied to filter_code: {dst_file}")

        # Create logic subdirectory if needed
        logic_src_dir = os.path.join(source_dir, 'logic')
        if os.path.exists(logic_src_dir):
            # Create a dpm_logic_{framework} directory in filter_code
            dpm_logic_dir = os.path.join(filter_code_dir, f"dpm_logic_{framework.lower()}")
            os.makedirs(dpm_logic_dir, exist_ok=True)

            # Copy __init__.py
            init_content = f"# Auto-generated init for DPM {framework} logic files\n"
            with open(os.path.join(dpm_logic_dir, '__init__.py'), 'w') as f:
                f.write(init_content)

            # Copy logic files
            for filename in os.listdir(logic_src_dir):
                if filename.endswith('.py'):
                    src = os.path.join(logic_src_dir, filename)
                    dst = os.path.join(dpm_logic_dir, filename)
                    shutil.copy2(src, dst)

            logger.info(f"Copied logic files to: {dpm_logic_dir}")

    def _safe_identifier(self, name: str) -> str:
        """Convert a name to a safe Python identifier."""
        if not name:
            return "unknown"
        # Replace special characters
        safe = name.replace(' ', '_').replace('-', '_').replace('.', '_')
        # Remove any remaining invalid characters
        safe = ''.join(c for c in safe if c.isalnum() or c == '_')
        # Ensure it doesn't start with a digit
        if safe and safe[0].isdigit():
            safe = '_' + safe
        return safe or "unknown"
