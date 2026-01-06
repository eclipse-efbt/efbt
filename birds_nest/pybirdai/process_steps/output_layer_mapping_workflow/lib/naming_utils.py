"""
Utilities for naming conventions in output layer mappings.
Implements the convention: remove vowels, uppercase, replace spaces with underscores.
Also provides mapping ID prefix generation and sequence management.
"""

import re
import logging
from typing import Tuple
from pybirdai.models.bird_meta_data_model import VARIABLE_MAPPING
from pybirdai.process_steps.output_layer_mapping_workflow.lib.table_cell_utils import (
    extract_base_table_code
)

logger = logging.getLogger(__name__)


class NamingUtils:
    """
    Utility class for generating IDs and codes following BIRD naming conventions.
    """

    @classmethod
    def extract_base_table_code(cls, table_id, table_code):
        return extract_base_table_code(table_id, table_code)

    @classmethod
    def strip_z_ordinate_suffix(cls, table_id: str) -> str:
        """
        Strip Z-ordinate suffix from a table ID while keeping the version.

        DEPRECATED: This method delegates to table_utils.get_base_table_id().
        Prefer using get_base_table_id() directly for new code.

        Z-variant tables use '__' as delimiter between base ID and member ID.
        Legacy patterns with single underscore are also supported for backward compatibility.

        Args:
            table_id: Full table ID potentially containing Z-ordinate suffix

        Returns:
            Table ID with Z-ordinate suffix removed

        Examples:
            "EBA_COREP_C_07_00_a_4_0__EBA_qEC_EBA_qx2029" -> "EBA_COREP_C_07_00_a_4_0"
            "F01_01_3_0__EBA_qx50" -> "F01_01_3_0"
            "C_07_00_a_4_0" -> "C_07_00_a_4_0" (no change if no Z-suffix)
        """
        from pybirdai.process_steps.output_layer_mapping_workflow.lib.table_utils import (
            get_base_table_id
        )
        return get_base_table_id(table_id, strip_trailing_z=True)

    @classmethod
    def extract_ordinate_suffix(cls, ordinate_id: str, source_table_id: str = None) -> str:
        """
        Extract the ordinate-specific suffix from a full axis_ordinate_id.

        The suffix is typically the last part containing orientation and order (e.g., X_0220, Y_0281).
        This is useful when creating reference table ordinate IDs to avoid duplicating
        the source table ID.

        Args:
            ordinate_id: Full axis_ordinate_id (e.g., EBA_COREP_C_07_00_a_4_0_EBA_qEC_EBA_qx2041_X_0220)
            source_table_id: Optional source table ID to strip from the beginning

        Returns:
            Ordinate suffix (e.g., X_0220)

        Examples:
            "EBA_COREP_C_07_00_a_4_0_EBA_qEC_EBA_qx2041_X_0220" -> "X_0220"
            "EBA_FINREP_F01_01_3_0_Y_0100" -> "Y_0100"
            "TABLE_ID_Z_999" -> "Z_999"
        """
        if not ordinate_id:
            return ordinate_id

        # Pattern: Match the orientation and order at the end
        # Orientations are X (column), Y (row), Z (3D axis)
        # Order is typically 4 digits
        pattern = r'([XYZ]_\d+)$'
        match = re.search(pattern, ordinate_id)

        if match:
            suffix = match.group(1)
            logger.debug(f"Extracted ordinate suffix: {ordinate_id} -> {suffix}")
            return suffix

        # Fallback: if source_table_id provided, strip it from the beginning
        if source_table_id and ordinate_id.startswith(source_table_id):
            suffix = ordinate_id[len(source_table_id):].lstrip('_')
            logger.debug(f"Extracted ordinate suffix by stripping table ID: {ordinate_id} -> {suffix}")
            return suffix

        # Final fallback: return the original ID
        logger.warning(f"Could not extract ordinate suffix from: {ordinate_id}")
        return ordinate_id

    @classmethod
    def generate_mapping_prefix(
        cls,
        table_code: str,
        version: str,
        table_id: str = None
    ) -> str:
        """
        Generate a prefix for mapping IDs.

        The prefix format is: {table_code_normalized}_{version_normalized}_MAP
        If table_id is provided, extracts the base table code (strips Z-axis suffix).

        Args:
            table_code: Base table code (e.g., "F01_01")
            version: Version string (e.g., "3.2.0" or "3_2_0")
            table_id: Optional full table ID with potential Z-axis suffix

        Returns:
            Mapping prefix string (e.g., "F01_01_3_2_0_MAP")

        Examples:
            generate_mapping_prefix("F01_01", "3.2.0") -> "F01_01_3_2_0_MAP"
            generate_mapping_prefix("F01 01", "3.2.0", "F01_01_3_2_0_Z0") -> "F01_01_3_2_0_MAP"
        """


        # Normalize version: replace dots with underscores
        version_normalized = version.replace('.', '_')

        # Get base table code (strips Z-axis suffix if present)
        if table_id:
            base_table_code = extract_base_table_code(table_id, table_code)
        else:
            base_table_code = table_code

        # Normalize: replace spaces AND dots with underscores
        table_code_normalized = base_table_code.replace(" ", "_").replace(".", "_")

        mapping_prefix = f"{table_code_normalized}_{version_normalized}_MAP"
        logger.debug(f"Generated mapping prefix: {mapping_prefix}")
        return mapping_prefix

    @classmethod
    def calculate_next_sequence(cls, prefix: str) -> int:
        """
        Calculate the next available sequence number for a mapping prefix.

        Queries existing VARIABLE_MAPPINGs with the given prefix to find
        the next available sequence number.

        Args:
            prefix: Mapping prefix (e.g., "F01_01_3_2_0_MAP")

        Returns:
            Next available sequence number (1-indexed)
        """
        existing_count = VARIABLE_MAPPING.objects.filter(
            variable_mapping_id__startswith=prefix
        ).count()
        next_sequence = existing_count + 1
        logger.debug(f"Next sequence for prefix '{prefix}': {next_sequence}")
        return next_sequence

    @classmethod
    def format_mapping_id_suffix(cls, sequence: int) -> str:
        """
        Format a sequence number as a 3-digit mapping ID suffix.

        Args:
            sequence: Sequence number

        Returns:
            Formatted suffix (e.g., "001", "042", "100")
        """
        return f"{sequence:03d}"

    @classmethod
    def generate_internal_id(cls, name: str) -> str:
        """
        Generate an internal ID from a mapping name.

        Implements the BIRD naming convention:
        - Remove vowels (a, e, i, o, u)
        - Convert to uppercase
        - Replace spaces with underscores

        Args:
            name: Human-readable mapping name

        Returns:
            Internal ID following BIRD conventions

        Examples:
            "Credit Card Debt Mapping" -> "CRDT_CRD_DBT_MPPNG"
            "Total Assets" -> "TTL_SSTS"
        """
        if not name:
            return ""

        # Replace spaces with underscores
        result = name.replace(' ', '_')

        # Remove vowels (both lowercase and uppercase)
        vowels = 'aeiouAEIOU'
        result = ''.join(c for c in result if c not in vowels)

        # Convert to uppercase
        result = result.upper()

        logger.debug(f"Generated internal ID: {name} -> {result}")
        return result
