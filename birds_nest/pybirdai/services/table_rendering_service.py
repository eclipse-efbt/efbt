"""
Table Rendering Service

Provides business logic for rendering table structures with hierarchical headers.
Used by the Interactive Report Viewer to display regulatory templates.
"""

from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
import logging
import re

from pybirdai.models import TABLE, AXIS, AXIS_ORDINATE, TABLE_CELL, CELL_POSITION, ORDINATE_ITEM

logger = logging.getLogger(__name__)


class TableRenderingService:
    """Service for rendering table structures for display."""

    @staticmethod
    def render_table(table_id: str) -> Dict[str, Any]:
        """
        Generate a renderable table structure with hierarchical headers.

        Args:
            table_id: The ID of the TABLE to render

        Returns:
            Dictionary with table structure for frontend rendering
        """
        try:
            table = TABLE.objects.get(table_id=table_id)
        except TABLE.DoesNotExist:
            return {
                'success': False,
                'error': f"Table not found: {table_id}"
            }

        # Get axes for this table
        axes = AXIS.objects.filter(table_id=table).order_by('orientation', 'order')

        # Separate row and column axes
        row_axes = [a for a in axes if a.orientation in ('Y', '2')]
        col_axes = [a for a in axes if a.orientation in ('X', '1')]

        # Build ordinate trees
        row_tree = TableRenderingService._build_ordinate_tree(row_axes)
        col_tree = TableRenderingService._build_ordinate_tree(col_axes)

        # Get leaf ordinates (for actual data rows/columns)
        row_leaves = TableRenderingService._get_leaf_ordinates(row_tree)
        col_leaves = TableRenderingService._get_leaf_ordinates(col_tree)

        # Get cells and build lookup
        cells = TABLE_CELL.objects.filter(table_id=table)
        positions = CELL_POSITION.objects.filter(
            cell_id__in=cells
        ).select_related('cell_id', 'axis_ordinate_id', 'axis_ordinate_id__axis_id')

        cell_lookup = TableRenderingService._build_cell_lookup(positions)

        # Build column headers with colspan
        column_headers = TableRenderingService._build_column_headers(col_tree)

        # Build row headers with rowspan
        row_headers = TableRenderingService._build_row_headers(row_tree)

        # Build data rows
        rows = []
        for i, row_ord in enumerate(row_leaves):
            row_cells = []
            for j, col_ord in enumerate(col_leaves):
                cell = cell_lookup.get((row_ord.axis_ordinate_id, col_ord.axis_ordinate_id))
                # Build the full datapoint ID from table code + combination ID
                datapoint_id = TableRenderingService._build_datapoint_id(cell, table) if cell else None
                row_cells.append({
                    'cell_id': cell.cell_id if cell else None,
                    'row_ordinate_id': row_ord.axis_ordinate_id,
                    'column_ordinate_id': col_ord.axis_ordinate_id,
                    'row_index': i,
                    'column_index': j,
                    'is_shaded': cell.is_shaded if cell else True,
                    'is_executable': TableRenderingService._is_cell_executable(cell) if cell else False,
                    'datapoint_id': datapoint_id,
                    'name': cell.name if cell else None,
                })
            rows.append({
                'row_ordinate_id': row_ord.axis_ordinate_id,
                'row_index': i,
                'cells': row_cells
            })

        # Count statistics
        total_cells = len(cells)
        executable_cells = sum(1 for c in cells if TableRenderingService._is_cell_executable(c))
        shaded_cells = sum(1 for c in cells if c.is_shaded)

        return {
            'success': True,
            'table_id': table.table_id,
            'name': table.name or '',
            'code': table.code or '',
            'description': table.description or '',
            'column_headers': column_headers,
            'row_headers': row_headers,
            'rows': rows,
            'metadata': {
                'total_cells': total_cells,
                'executable_cells': executable_cells,
                'shaded_cells': shaded_cells,
                'row_count': len(row_leaves),
                'column_count': len(col_leaves),
            }
        }

    @staticmethod
    def _build_ordinate_tree(axes: List[AXIS]) -> List[Dict]:
        """
        Build a hierarchical tree structure from axes and their ordinates.

        Args:
            axes: List of AXIS objects

        Returns:
            List of tree nodes with nested children
        """
        result = []

        for axis in axes:
            ordinates = AXIS_ORDINATE.objects.filter(
                axis_id=axis
            ).order_by('level', 'order')

            # Build lookup and children map
            ordinate_list = list(ordinates)
            children_map = defaultdict(list)
            roots = []

            for o in ordinate_list:
                if o.parent_axis_ordinate_id:
                    children_map[o.parent_axis_ordinate_id.axis_ordinate_id].append(o)
                else:
                    roots.append(o)

            # Get ordinate items for each ordinate
            ordinate_items = ORDINATE_ITEM.objects.filter(
                axis_ordinate_id__in=ordinate_list
            ).select_related('variable_id', 'member_id')

            items_map = defaultdict(list)
            for item in ordinate_items:
                items_map[item.axis_ordinate_id.axis_ordinate_id].append({
                    'variable_id': item.variable_id.variable_id if item.variable_id else None,
                    'variable_name': item.variable_id.name if item.variable_id else None,
                    'member_id': item.member_id.member_id if item.member_id else None,
                    'member_name': item.member_id.name if item.member_id else None,
                })

            def build_node(ord_obj):
                children = [build_node(c) for c in children_map.get(ord_obj.axis_ordinate_id, [])]
                return {
                    'ordinate_id': ord_obj.axis_ordinate_id,
                    'name': ord_obj.name or '',
                    'code': ord_obj.code or '',
                    'level': ord_obj.level or 0,
                    'order': ord_obj.order or 0,
                    'is_abstract_header': ord_obj.is_abstract_header or False,
                    'ordinate_items': items_map.get(ord_obj.axis_ordinate_id, []),
                    'children': children,
                    'leaf_count': TableRenderingService._count_leaves(children) if children else 1
                }

            result.extend([build_node(r) for r in roots])

        return result

    @staticmethod
    def _count_leaves(children: List[Dict]) -> int:
        """Count the number of leaf nodes in a tree."""
        if not children:
            return 1
        total = 0
        for child in children:
            if child['children']:
                total += TableRenderingService._count_leaves(child['children'])
            else:
                total += 1
        return total

    @staticmethod
    def _get_leaf_ordinates(tree: List[Dict]) -> List[AXIS_ORDINATE]:
        """
        Get all leaf (non-abstract) ordinates from a tree in order.

        Args:
            tree: Tree structure from _build_ordinate_tree

        Returns:
            List of AXIS_ORDINATE objects for leaf nodes
        """
        leaves = []
        ordinate_ids = []

        def traverse(nodes):
            for node in nodes:
                if node['children']:
                    traverse(node['children'])
                else:
                    ordinate_ids.append(node['ordinate_id'])

        traverse(tree)

        # Fetch all ordinates in one query
        if ordinate_ids:
            ordinates_dict = {
                o.axis_ordinate_id: o
                for o in AXIS_ORDINATE.objects.filter(axis_ordinate_id__in=ordinate_ids)
            }
            leaves = [ordinates_dict[oid] for oid in ordinate_ids if oid in ordinates_dict]

        return leaves

    @staticmethod
    def _build_cell_lookup(positions) -> Dict[Tuple[str, str], TABLE_CELL]:
        """
        Build a lookup from (row_ordinate_id, col_ordinate_id) to TABLE_CELL.

        Args:
            positions: QuerySet of CELL_POSITION objects

        Returns:
            Dictionary mapping ordinate pairs to cells
        """
        cell_positions = defaultdict(dict)

        for pos in positions:
            cell = pos.cell_id
            ordinate = pos.axis_ordinate_id
            if not ordinate or not ordinate.axis_id:
                continue

            axis = ordinate.axis_id

            if axis.orientation in ('Y', '2'):
                cell_positions[cell.cell_id]['row'] = ordinate.axis_ordinate_id
            else:
                cell_positions[cell.cell_id]['col'] = ordinate.axis_ordinate_id

        # Build final lookup
        lookup = {}
        cells_dict = {}

        # Get all cells in one query
        cell_ids = list(cell_positions.keys())
        if cell_ids:
            for cell in TABLE_CELL.objects.filter(cell_id__in=cell_ids):
                cells_dict[cell.cell_id] = cell

        for cell_id, coords in cell_positions.items():
            if 'row' in coords and 'col' in coords:
                cell = cells_dict.get(cell_id)
                if cell:
                    lookup[(coords['row'], coords['col'])] = cell

        return lookup

    @staticmethod
    def _build_column_headers(tree: List[Dict]) -> Dict[str, Any]:
        """
        Build column header structure with colspan for hierarchical headers.

        Args:
            tree: Tree structure from _build_ordinate_tree

        Returns:
            Dictionary with header levels
        """
        if not tree:
            return {'levels': []}

        # Find max depth
        max_depth = TableRenderingService._get_max_depth(tree)

        # Build header levels
        levels = []
        for depth in range(max_depth):
            level_headers = []
            TableRenderingService._collect_headers_at_depth(tree, depth, 0, level_headers, max_depth)
            levels.append(level_headers)

        return {'levels': levels}

    @staticmethod
    def _get_max_depth(tree: List[Dict]) -> int:
        """Get the maximum depth of the tree."""
        if not tree:
            return 0

        max_depth = 0
        for node in tree:
            depth = 1
            if node['children']:
                depth += TableRenderingService._get_max_depth(node['children'])
            max_depth = max(max_depth, depth)
        return max_depth

    @staticmethod
    def _collect_headers_at_depth(
        nodes: List[Dict],
        target_depth: int,
        current_depth: int,
        result: List[Dict],
        max_depth: int
    ):
        """Collect headers at a specific depth level."""
        for node in nodes:
            if current_depth == target_depth:
                # Calculate colspan (number of leaf descendants)
                colspan = node['leaf_count']

                # Calculate rowspan (if this is a leaf and we're not at max depth)
                remaining_depth = max_depth - current_depth
                rowspan = remaining_depth if not node['children'] else 1

                result.append({
                    'text': node['name'],
                    'ordinate_id': node['ordinate_id'],
                    'colspan': colspan,
                    'rowspan': rowspan,
                    'is_abstract': node['is_abstract_header'],
                    'level': current_depth
                })
            elif current_depth < target_depth and node['children']:
                # Recurse into children
                TableRenderingService._collect_headers_at_depth(
                    node['children'], target_depth, current_depth + 1, result, max_depth
                )

    @staticmethod
    def _build_row_headers(tree: List[Dict]) -> List[Dict]:
        """
        Build row header structure with rowspan for hierarchical headers.

        Args:
            tree: Tree structure from _build_ordinate_tree

        Returns:
            List of row header dictionaries
        """
        headers = []
        TableRenderingService._flatten_row_headers(tree, headers, 0)
        return headers

    @staticmethod
    def _flatten_row_headers(nodes: List[Dict], result: List[Dict], depth: int):
        """Flatten tree into row headers with rowspan."""
        for node in nodes:
            if node['children']:
                # Abstract header with children
                rowspan = node['leaf_count']
                result.append({
                    'text': node['name'],
                    'ordinate_id': node['ordinate_id'],
                    'rowspan': rowspan,
                    'colspan': 1,
                    'is_abstract': True,
                    'level': depth,
                    'has_children': True
                })
                TableRenderingService._flatten_row_headers(node['children'], result, depth + 1)
            else:
                # Leaf node
                result.append({
                    'text': node['name'],
                    'ordinate_id': node['ordinate_id'],
                    'rowspan': 1,
                    'colspan': 1,
                    'is_abstract': False,
                    'level': depth,
                    'has_children': False
                })

    @staticmethod
    def _is_cell_executable(cell: TABLE_CELL) -> bool:
        """Check if a cell is executable."""
        return (
            cell is not None
            and cell.table_cell_combination_id is not None
            and cell.table_cell_combination_id != ''
            and not cell.is_shaded
        )

    @staticmethod
    def _build_datapoint_id(cell: TABLE_CELL, table: TABLE) -> Optional[str]:
        """
        Build the full datapoint ID from a cell's table and combination info.

        The Cell_ classes are named like: Cell_F_04_01_REF_FINREP_3_0_11112_REF
        where:
        - Table code: F_04_01_REF (from TABLE.code field, normalized)
        - Version: FINREP_3_0 (from TABLE.version field, normalized)
        - Combination number: 11112 (extracted from table_cell_combination_id)
        - Suffix: _REF

        Args:
            cell: The TABLE_CELL to build datapoint ID for
            table: The parent TABLE

        Returns:
            Full datapoint ID string or None if cannot be constructed
        """
        try:
            combination_id = cell.table_cell_combination_id
            if not combination_id:
                return None

            # Extract the numeric combination ID
            # Format can be:
            # - "EBA_11112" (EBA prefix) -> extract "11112"
            # - "67316_REF" (REF suffix) -> extract "67316"
            # - Just numeric like "11112"
            combination_number = combination_id
            if combination_id.endswith('_REF'):
                combination_number = combination_id[:-4]  # Remove "_REF" suffix
            elif combination_id.startswith('EBA_'):
                combination_number = combination_id[4:]  # Remove "EBA_" prefix
            elif '_' in combination_id:
                # Handle other formats - try to find the numeric part
                parts = combination_id.split('_')
                for part in parts:
                    if part.isdigit():
                        combination_number = part
                        break

            # Get the table's code and version fields
            table_code = table.code  # e.g., "F_04.01_REF"
            table_version = table.version  # e.g., "FINREP 3.0"

            if not table_code or not table_version:
                logger.warning(f"Table {table.table_id} missing code or version")
                return None

            # Normalize the table code (replace dots with underscores)
            # E.g., "F_04.01_REF" -> "F_04_01_REF"
            normalized_code = table_code.replace('.', '_')

            # Normalize the version (replace dots and spaces with underscores)
            # E.g., "FINREP 3.0" -> "FINREP_3_0"
            normalized_version = table_version.replace('.', '_').replace(' ', '_')

            # Build the full datapoint ID
            # Format: {code}_{version}_{combination_number}_REF
            # E.g., "F_04_01_REF_FINREP_3_0_11112_REF"
            datapoint_id = f"{normalized_code}_{normalized_version}_{combination_number}_REF"

            return datapoint_id

        except Exception as e:
            logger.exception(f"Error building datapoint ID for cell {cell.cell_id}")
            return None

    @staticmethod
    def get_templates_list(
        framework: Optional[str] = None,
        version: Optional[str] = None,
        search: Optional[str] = None,
        has_executable_cells: bool = False,
        page: int = 1,
        page_size: int = 50
    ) -> Dict[str, Any]:
        """
        Get a list of available templates with filtering.

        Args:
            framework: Filter by framework (e.g., 'FINREP', 'COREP')
            version: Filter by version
            search: Search by name or code
            has_executable_cells: Only show templates with executable cells
            page: Page number
            page_size: Page size

        Returns:
            Dictionary with templates list and pagination info
        """
        # Start with all tables
        tables = TABLE.objects.all()

        # Apply filters
        if framework:
            tables = tables.filter(table_id__icontains=framework)

        if version:
            tables = tables.filter(version=version)

        if search:
            tables = tables.filter(
                models.Q(name__icontains=search) |
                models.Q(code__icontains=search) |
                models.Q(table_id__icontains=search)
            )

        # Get tables with axes (actual templates)
        tables_with_axes = tables.filter(
            axis__isnull=False
        ).distinct()

        # Count total before pagination
        total = tables_with_axes.count()

        # Apply pagination
        offset = (page - 1) * page_size
        tables_page = tables_with_axes.order_by('code', 'table_id')[offset:offset + page_size]

        # Build template list
        templates = []
        for table in tables_page:
            # Count cells
            cells = TABLE_CELL.objects.filter(table_id=table)
            total_cells = cells.count()
            executable_cells = sum(
                1 for c in cells
                if c.table_cell_combination_id and not c.is_shaded
            )

            # Skip if filtering for executable cells and none found
            if has_executable_cells and executable_cells == 0:
                continue

            # Extract framework from table_id (e.g., "F_05_01_REF_FINREP_3_0" -> "FINREP")
            table_framework = TableRenderingService._extract_framework(table.table_id)

            templates.append({
                'table_id': table.table_id,
                'code': table.code or '',
                'name': table.name or '',
                'description': table.description or '',
                'framework': table_framework,
                'version': table.version or '',
                'cell_count': total_cells,
                'executable_cell_count': executable_cells,
            })

        return {
            'templates': templates,
            'total': total,
            'page': page,
            'page_size': page_size
        }

    @staticmethod
    def _extract_framework(table_id: str) -> str:
        """Extract framework name from table_id."""
        if not table_id:
            return ''

        table_id_upper = table_id.upper()

        frameworks = ['FINREP', 'COREP', 'AE', 'FP', 'ANACREDIT']
        for fw in frameworks:
            if fw in table_id_upper:
                return fw

        return ''
