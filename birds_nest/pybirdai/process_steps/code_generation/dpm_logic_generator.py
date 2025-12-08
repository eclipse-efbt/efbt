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
DPM Logic Generator - Generates *_logic.py files for DPM templates.

This generator creates Python code following the FINREP pattern with:
- Base class: Abstract methods for all dimensions/measures
- UnionItem class: Combines data from multiple sources with @lineage decorators
- Concrete source classes: Implement mappings to BIRD data model tables
- UnionTable class: Orchestrates execution with calc_* and init() methods

Inheritance is used to avoid code duplication when cells share the same
combination_items (submaps of a template).
"""

import os
import logging
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class DimensionInfo:
    """Information about a dimension/variable in the template."""
    variable_id: str
    variable_name: str
    domain_id: str
    is_measure: bool = False
    return_type: str = "str"


@dataclass
class CombinationItemInfo:
    """Information about a combination item (member value for a dimension)."""
    variable_id: str
    variable_name: str
    member_id: str
    member_code: str


@dataclass
class CombinationInfo:
    """Information about a combination (cell definition)."""
    combination_id: str
    items: List[CombinationItemInfo] = field(default_factory=list)

    def get_signature(self) -> str:
        """Get a signature string for grouping cells with same combination structure."""
        # Sort items by variable_id for consistent comparison
        sorted_items = sorted(self.items, key=lambda x: x.variable_id)
        return "|".join(f"{item.variable_id}:{item.member_code}" for item in sorted_items)


@dataclass
class TableInfo:
    """Information about a DPM table."""
    table_id: str
    table_code: str
    table_name: str
    framework: str
    version: str
    dimensions: List[DimensionInfo] = field(default_factory=list)
    combinations: List[CombinationInfo] = field(default_factory=list)


class DPMLogicGenerator:
    """
    Generates *_logic.py files for DPM templates following the FINREP pattern.

    The generated code uses inheritance to avoid duplication:
    - Base class defines the template structure
    - Submap classes group cells with identical combination_items
    - Cell classes only define position (ROW/COL)
    """

    def __init__(self):
        self.output_dir = None

    def generate_for_table(
        self,
        table_code: str,
        framework: str,
        version: str,
        output_dir: str
    ) -> Dict:
        """
        Generate a *_logic.py file for a specific table.

        Args:
            table_code: The table code (e.g., 'C_07.00.a')
            framework: The framework name (e.g., 'COREP')
            version: The version string (e.g., 'COREP_3')
            output_dir: Directory to write the generated file

        Returns:
            Dict with generation results
        """
        self.output_dir = output_dir

        # Fetch table info from database
        table_info = self._fetch_table_info(table_code, framework, version)
        if not table_info:
            return {'status': 'error', 'message': f'Table {table_code} not found'}

        # Generate the logic file content
        content = self._generate_logic_file_content(table_info)

        # Write to file
        safe_table_code = table_code.replace('.', '_').replace(' ', '_')
        filename = f"{safe_table_code}_{framework}_{version}_logic.py"
        filepath = os.path.join(output_dir, filename)

        os.makedirs(output_dir, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)

        logger.info(f"Generated logic file: {filepath}")

        return {
            'status': 'success',
            'file': filepath,
            'table_code': table_code,
            'dimensions_count': len(table_info.dimensions),
            'combinations_count': len(table_info.combinations)
        }

    def _fetch_table_info(
        self,
        table_code: str,
        framework: str,
        version: str
    ) -> Optional[TableInfo]:
        """Fetch table information from the database."""
        from pybirdai.models.bird_meta_data_model import (
            TABLE, CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM,
            COMBINATION, COMBINATION_ITEM, CUBE_TO_COMBINATION,
            VARIABLE, MEMBER
        )

        # Find the table
        tables = TABLE.objects.filter(table_code=table_code)
        if not tables.exists():
            logger.warning(f"Table not found: {table_code}")
            return None

        table = tables.first()

        # Find associated cube
        safe_code = table_code.replace('.', '_')
        cubes = CUBE.objects.filter(name__icontains=safe_code)
        if not cubes.exists():
            logger.warning(f"No cube found for table: {table_code}")
            return None

        cube = cubes.first()

        # Fetch dimensions from cube structure items
        dimensions = []
        cube_structures = CUBE_STRUCTURE.objects.filter(cube=cube)
        for cs in cube_structures:
            csi_list = CUBE_STRUCTURE_ITEM.objects.filter(cube_structure=cs)
            for csi in csi_list:
                if csi.variable:
                    dim_info = DimensionInfo(
                        variable_id=csi.variable.variable_id,
                        variable_name=csi.variable.name or csi.variable.variable_id,
                        domain_id=csi.variable.domain.domain_id if csi.variable.domain else '',
                        is_measure=csi.is_identifier == False if hasattr(csi, 'is_identifier') else False,
                        return_type='int' if csi.variable.domain and 'Float' in str(csi.variable.domain.domain_id) else 'str'
                    )
                    dimensions.append(dim_info)

        # Fetch combinations
        combinations = []
        cube_to_combos = CUBE_TO_COMBINATION.objects.filter(cube=cube)
        for ctc in cube_to_combos:
            if ctc.combination:
                combo = ctc.combination
                combo_info = CombinationInfo(combination_id=combo.combination_id)

                # Fetch combination items
                combo_items = COMBINATION_ITEM.objects.filter(combination=combo)
                for ci in combo_items:
                    if ci.variable and ci.member:
                        item_info = CombinationItemInfo(
                            variable_id=ci.variable.variable_id,
                            variable_name=ci.variable.name or ci.variable.variable_id,
                            member_id=ci.member.member_id,
                            member_code=ci.member.code or ci.member.member_id
                        )
                        combo_info.items.append(item_info)

                combinations.append(combo_info)

        return TableInfo(
            table_id=table.table_id if hasattr(table, 'table_id') else str(table.pk),
            table_code=table_code,
            table_name=table.table_name if hasattr(table, 'table_name') else table_code,
            framework=framework,
            version=version,
            dimensions=dimensions,
            combinations=combinations
        )

    def _generate_logic_file_content(self, table_info: TableInfo) -> str:
        """Generate the content of the logic file."""
        safe_table_code = table_info.table_code.replace('.', '_').replace(' ', '_')
        class_prefix = f"{safe_table_code}_{table_info.framework}_{table_info.version}"

        lines = []

        # File header
        lines.extend([
            "# coding=UTF-8",
            "# Auto-generated DPM logic file",
            f"# Table: {table_info.table_code}",
            f"# Framework: {table_info.framework}",
            f"# Version: {table_info.version}",
            "",
            "from pybirdai.bird_data_model import *",
            "from pybirdai.process_steps.pybird.orchestration import Orchestration",
            "from pybirdai.process_steps.pybird.csv_converter import CSVConverter",
            "from datetime import datetime",
            "from pybirdai.annotations.decorators import lineage",
            "",
            ""
        ])

        # Generate Base class
        lines.extend(self._generate_base_class(class_prefix, table_info.dimensions))

        # Generate UnionItem class
        lines.extend(self._generate_union_item_class(class_prefix, table_info.dimensions))

        # Group combinations by signature (for inheritance)
        submap_groups = self._group_combinations_by_signature(table_info.combinations)

        # Generate Submap base classes for groups with multiple cells
        lines.extend(self._generate_submap_classes(class_prefix, submap_groups, table_info.dimensions))

        # Generate UnionTable class
        lines.extend(self._generate_union_table_class(class_prefix, submap_groups))

        return "\n".join(lines)

    def _generate_base_class(
        self,
        class_prefix: str,
        dimensions: List[DimensionInfo]
    ) -> List[str]:
        """Generate the Base class with abstract methods."""
        lines = [
            f"class {class_prefix}_Base:",
            f'\t"""Base class defining the template structure for {class_prefix}."""',
            ""
        ]

        # Generate abstract method for each dimension
        for dim in dimensions:
            safe_name = self._safe_identifier(dim.variable_name)
            return_type = dim.return_type
            lines.extend([
                f"\tdef {safe_name}(self) -> {return_type}:",
                f"\t\t'''Return value for {dim.variable_name}'''",
                "\t\tpass",
                ""
            ])

        if not dimensions:
            lines.append("\tpass")
            lines.append("")

        lines.append("")
        return lines

    def _generate_union_item_class(
        self,
        class_prefix: str,
        dimensions: List[DimensionInfo]
    ) -> List[str]:
        """Generate the UnionItem class with @lineage decorators."""
        lines = [
            f"class {class_prefix}_UnionItem:",
            f'\t"""Combines data from multiple sources with lineage tracking."""',
            f"\tbase = None  # {class_prefix}_Base",
            ""
        ]

        for dim in dimensions:
            safe_name = self._safe_identifier(dim.variable_name)
            return_type = dim.return_type
            lines.extend([
                f'\t@lineage(dependencies={{"base.{safe_name}"}})',
                f"\tdef {safe_name}(self) -> {return_type}:",
                f"\t\treturn self.base.{safe_name}()",
                ""
            ])

        if not dimensions:
            lines.append("\tpass")
            lines.append("")

        lines.append("")
        return lines

    def _group_combinations_by_signature(
        self,
        combinations: List[CombinationInfo]
    ) -> Dict[str, List[CombinationInfo]]:
        """Group combinations by their signature (same combination items = same submap)."""
        groups = defaultdict(list)
        for combo in combinations:
            signature = combo.get_signature()
            groups[signature].append(combo)
        return dict(groups)

    def _generate_submap_classes(
        self,
        class_prefix: str,
        submap_groups: Dict[str, List[CombinationInfo]],
        dimensions: List[DimensionInfo]
    ) -> List[str]:
        """Generate Submap base classes for cells with identical combination structures."""
        lines = []

        for i, (signature, combos) in enumerate(submap_groups.items()):
            submap_name = f"{class_prefix}_Submap_{i}"

            # Generate the submap base class
            lines.extend([
                f"class {submap_name}({class_prefix}_Base):",
                f'\t"""',
                f"\tSubmap grouping {len(combos)} cells with identical combination structure.",
                f"\tSignature: {signature[:80]}..." if len(signature) > 80 else f"\tSignature: {signature}",
                f'\t"""',
                ""
            ])

            # Add combination_items as class attribute
            if combos:
                first_combo = combos[0]
                lines.append("\t# Combination items (shared by all cells in this submap)")
                lines.append(f"\tCOMBINATION_ITEMS = {{")
                for item in first_combo.items:
                    safe_var = self._safe_identifier(item.variable_name)
                    lines.append(f"\t\t'{safe_var}': '{item.member_code}',")
                lines.append("\t}")
                lines.append("")

            # Generate filter method based on combination items
            lines.extend([
                "\tdef matches_filter(self, data_item) -> bool:",
                "\t\t'''Check if a data item matches this submap's filter criteria.'''",
            ])

            if combos and combos[0].items:
                for item in combos[0].items:
                    safe_var = self._safe_identifier(item.variable_name)
                    lines.append(f"\t\tif hasattr(data_item, '{safe_var}') and data_item.{safe_var} != '{item.member_code}':")
                    lines.append("\t\t\treturn False")
                lines.append("\t\treturn True")
            else:
                lines.append("\t\treturn True")

            lines.append("")
            lines.append("")

        return lines

    def _generate_union_table_class(
        self,
        class_prefix: str,
        submap_groups: Dict[str, List[CombinationInfo]]
    ) -> List[str]:
        """Generate the UnionTable class for orchestration."""
        lines = [
            f"class {class_prefix}_UnionTable:",
            f'\t"""Orchestrates data collection and union from all submaps."""',
            f"\t{class_prefix}_UnionItems = []  # {class_prefix}_UnionItem[]",
            ""
        ]

        # Add submap table references
        for i in range(len(submap_groups)):
            lines.append(f"\t{class_prefix}_Submap_{i}_Table = None")

        lines.append("")

        # Generate calc method
        lines.extend([
            f"\tdef calc_{class_prefix}_UnionItems(self) -> list:",
            "\t\t'''Calculate and collect all union items from submaps.'''",
            "\t\titems = []",
            ""
        ])

        for i in range(len(submap_groups)):
            submap_var = f"self.{class_prefix}_Submap_{i}_Table"
            lines.extend([
                f"\t\tif {submap_var} is not None:",
                f"\t\t\tfor item in {submap_var}.items:",
                f"\t\t\t\tnew_item = {class_prefix}_UnionItem()",
                "\t\t\t\tnew_item.base = item",
                "\t\t\t\titems.append(new_item)",
                ""
            ])

        lines.extend([
            "\t\treturn items",
            ""
        ])

        # Generate init method
        lines.extend([
            "\tdef init(self):",
            "\t\t'''Initialize the union table with orchestration.'''",
            "\t\tOrchestration().init(self)",
            f"\t\tself.{class_prefix}_UnionItems = []",
            f"\t\tself.{class_prefix}_UnionItems.extend(self.calc_{class_prefix}_UnionItems())",
            "\t\tCSVConverter.persist_object_as_csv(self, True)",
            "\t\treturn None",
            ""
        ])

        lines.append("")
        return lines

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
