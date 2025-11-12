"""
Utilities for naming conventions in output layer mappings.
Implements the convention: remove vowels, uppercase, replace spaces with underscores.
"""

import re
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


class NamingUtils:
    """
    Utility class for generating IDs and codes following BIRD naming conventions.
    Convention: Remove vowels, uppercase, replace spaces with underscores.
    """

    # Vowels to remove (both upper and lower case)
    VOWELS = set('aeiouAEIOU')

    # Special terms that should be preserved
    PRESERVED_TERMS = {
        'ID', 'SD', 'CS', 'REF', 'NR', 'COMB', 'MAP', 'VAR', 'MEM',
        'HIER', 'ATTR', 'DIM', 'OBS', 'CUBE', 'STRUCT', 'DEF'
    }

    @classmethod
    def generate_internal_id(cls, name: str) -> str:
        """
        Generate an internal ID from a human-readable name.
        Follows convention: remove vowels, uppercase, replace spaces with underscores.

        Args:
            name: Human-readable name

        Returns:
            Generated internal ID

        Examples:
            "Loan Type Mapping" -> "LN_TYP_MPPNG"
            "Interest Rate" -> "NTRST_RT"
            "Customer Account" -> "CSTMR_CCNT"
        """
        if not name:
            return ""

        # Convert to uppercase
        result = name.upper()

        # Replace special characters and spaces with underscores
        result = re.sub(r'[^A-Z0-9_]+', '_', result)

        # Split into words
        words = result.split('_')

        # Process each word
        processed_words = []
        for word in words:
            if word in cls.PRESERVED_TERMS:
                # Keep preserved terms as-is
                processed_words.append(word)
            elif word:
                # Remove vowels from regular words
                processed_word = cls._remove_vowels(word)
                if processed_word:  # Only add if not empty after vowel removal
                    processed_words.append(processed_word)

        # Join with underscores and clean up
        result = '_'.join(processed_words)

        # Remove duplicate underscores
        result = re.sub(r'_+', '_', result)

        # Remove leading/trailing underscores
        result = result.strip('_')

        logger.debug(f"Generated internal ID: {name} -> {result}")
        return result

    @classmethod
    def _remove_vowels(cls, word: str) -> str:
        """
        Remove vowels from a word, keeping the first letter if it's a vowel.

        Args:
            word: Input word

        Returns:
            Word with vowels removed
        """
        if not word:
            return ""

        # Keep first letter even if it's a vowel
        result = word[0]

        # Remove vowels from rest of the word
        for char in word[1:]:
            if char not in cls.VOWELS:
                result += char

        return result

    @classmethod
    def generate_combination_id(cls, base: str, timestamp: str, counter: int) -> str:
        """
        Generate a unique combination ID.

        Args:
            base: Base identifier (e.g., cube ID or table code)
            timestamp: Timestamp string (YYYYMMDDHHMMSS)
            counter: Counter for uniqueness

        Returns:
            Generated combination ID

        Example:
            "F01_01_NR_FINREP_3_0", "20250131120000", 1 -> "F01_01_NR_FINREP_3_0_COMB_20250131120000_0001"
        """
        return f"{base}_COMB_{timestamp}_{counter:04d}"

    @classmethod
    def generate_subdomain_id(cls, variable_id: str, context: str) -> str:
        """
        Generate a subdomain ID for a variable in a specific context.

        Args:
            variable_id: The variable ID
            context: Context identifier (e.g., cube_structure_id)

        Returns:
            Generated subdomain ID

        Example:
            "TYP_INSTRMNT", "F01_01_CS" -> "TYP_INSTRMNT_OUTPUT_SD_F01_01_CS"
        """
        return f"{variable_id}_OUTPUT_SD_{context}"

    @classmethod
    def generate_cube_structure_item_code(
        cls,
        cube_structure_code: str,
        variable_id: str
    ) -> str:
        """
        Generate a cube structure item code.

        Args:
            cube_structure_code: The cube structure code
            variable_id: The variable ID

        Returns:
            Generated CSI code

        Example:
            "F01_01_CS", "TYP_INSTRMNT" -> "F01_01_CS__TYP_INSTRMNT"
        """
        return f"{cube_structure_code}__{variable_id}"

    @classmethod
    def extract_framework_version_from_cube(cls, cube_id: str) -> tuple:
        """
        Extract framework and version from a cube ID.

        Args:
            cube_id: The cube ID

        Returns:
            Tuple of (framework, version) or (None, None) if not found

        Examples:
            "F01_01_REF_FINREP_3_0" -> ("FINREP", "3.0")
            "F01_01_NR_AE_2_1" -> ("AE", "2.1")
        """
        # Common pattern: TABLE_CODE_(REF|NR)_FRAMEWORK_VERSION
        pattern = r'.*_(REF|NR)_([A-Z]+)_(\d+)_(\d+)'
        match = re.match(pattern, cube_id)

        if match:
            framework = match.group(2)
            major_version = match.group(3)
            minor_version = match.group(4)
            version = f"{major_version}.{minor_version}"
            return framework, version

        return None, None

    @classmethod
    def is_reference_cube(cls, cube_id: str) -> bool:
        """
        Check if a cube ID indicates a reference cube.

        Args:
            cube_id: The cube ID

        Returns:
            True if reference cube, False otherwise
        """
        return '_REF_' in cube_id.upper()

    @classmethod
    def is_non_reference_cube(cls, cube_id: str) -> bool:
        """
        Check if a cube ID indicates a non-reference cube.

        Args:
            cube_id: The cube ID

        Returns:
            True if non-reference cube, False otherwise
        """
        return '_NR_' in cube_id.upper()

    @classmethod
    def generate_mapping_id(
        cls,
        mapping_type: str,
        source_name: str,
        target_name: str,
        timestamp: str
    ) -> str:
        """
        Generate a mapping ID.

        Args:
            mapping_type: Type of mapping (VAR, MEM, etc.)
            source_name: Source name
            target_name: Target name
            timestamp: Timestamp string

        Returns:
            Generated mapping ID

        Example:
            "VAR", "Input Variables", "Output Variables", "20250131" ->
            "NPT_VRBLS_TO_TPT_VRBLS_VAR_MAP_20250131"
        """
        source_id = cls.generate_internal_id(source_name)
        target_id = cls.generate_internal_id(target_name)
        return f"{source_id}_TO_{target_id}_{mapping_type}_MAP_{timestamp}"

    @classmethod
    def sanitize_code(cls, code: str) -> str:
        """
        Sanitize a code to ensure it follows naming conventions.

        Args:
            code: Input code

        Returns:
            Sanitized code
        """
        # Convert to uppercase
        result = code.upper()

        # Replace invalid characters with underscores
        result = re.sub(r'[^A-Z0-9_]', '_', result)

        # Remove duplicate underscores
        result = re.sub(r'_+', '_', result)

        # Remove leading/trailing underscores
        result = result.strip('_')

        return result

    @classmethod
    def generate_hierarchy_id(cls, domain_name: str, hierarchy_type: str) -> str:
        """
        Generate a member hierarchy ID.

        Args:
            domain_name: Name of the domain
            hierarchy_type: Type of hierarchy (MAIN, CALC, etc.)

        Returns:
            Generated hierarchy ID

        Example:
            "Counterparty Type", "MAIN" -> "CNTRPRTY_TYP_MAIN_HIER"
        """
        domain_id = cls.generate_internal_id(domain_name)
        return f"{domain_id}_{hierarchy_type}_HIER"

    @classmethod
    def generate_variable_set_id(cls, name: str, context: str) -> str:
        """
        Generate a variable set ID.

        Args:
            name: Name of the variable set
            context: Context (e.g., cube or table)

        Returns:
            Generated variable set ID
        """
        name_id = cls.generate_internal_id(name)
        context_id = cls.generate_internal_id(context)
        return f"{name_id}_VS_{context_id}"

    @classmethod
    def extract_table_code_from_cube(cls, cube_id: str) -> Optional[str]:
        """
        Extract the table code from a cube ID.

        Args:
            cube_id: The cube ID

        Returns:
            Table code or None if not found

        Examples:
            "F01_01_REF_FINREP_3_0" -> "F01_01"
            "F02_00_NR_AE_2_1" -> "F02_00"
        """
        # Pattern: TABLE_CODE_(REF|NR)_...
        pattern = r'^([A-Z]\d+_\d+)_(REF|NR)_'
        match = re.match(pattern, cube_id)

        if match:
            return match.group(1)

        return None

    @classmethod
    def validate_id(cls, id_string: str, id_type: str = "generic") -> tuple:
        """
        Validate an ID string against naming conventions.

        Args:
            id_string: The ID to validate
            id_type: Type of ID for specific validation rules

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        # Check for empty string
        if not id_string:
            errors.append("ID cannot be empty")
            return False, errors

        # Check for invalid characters
        if not re.match(r'^[A-Z0-9_]+$', id_string):
            errors.append("ID contains invalid characters (only A-Z, 0-9, and _ allowed)")

        # Check for leading/trailing underscores
        if id_string.startswith('_') or id_string.endswith('_'):
            errors.append("ID should not start or end with underscore")

        # Check for duplicate underscores
        if '__' in id_string:
            errors.append("ID contains duplicate underscores")

        # Type-specific validation
        if id_type == "cube" and not any(term in id_string for term in ['_REF_', '_NR_']):
            errors.append("Cube ID should contain _REF_ or _NR_")

        if id_type == "combination" and '_COMB_' not in id_string:
            errors.append("Combination ID should contain _COMB_")

        is_valid = len(errors) == 0
        return is_valid, errors

    @classmethod
    def batch_generate_ids(cls, names: List[str], suffix: str = "") -> dict:
        """
        Generate IDs for a batch of names.

        Args:
            names: List of human-readable names
            suffix: Optional suffix to add to all IDs

        Returns:
            Dictionary mapping names to generated IDs
        """
        result = {}
        for name in names:
            base_id = cls.generate_internal_id(name)
            if suffix:
                result[name] = f"{base_id}_{suffix}"
            else:
                result[name] = base_id

        return result