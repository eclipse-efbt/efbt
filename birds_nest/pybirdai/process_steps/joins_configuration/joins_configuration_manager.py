"""
Joins Configuration Manager
Handles reading, writing, validating, and managing join configuration CSV files.

Supports flexible product breakdown formats:
- Legacy format: TYP_INSTRMNT_970 (variable inferred from prefix)
- New single variable format: TYP_INSTRMNT=TYP_INSTRMNT_970
- Multi-variable format: TYP_INSTRMNT=TYP_INSTRMNT_970:TYP_CLLRL=TYP_CLLRL_1
- No breakdown: empty Main Category
"""

import csv
import os
import re
import shutil
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from django.conf import settings
from django.core.exceptions import SuspiciousFileOperation
from django.utils._os import safe_join

from pybirdai.process_steps.joins_meta_data.condition_parser import BreakdownCondition


class JoinsConfigurationManager:
    """Manager for join configuration CSV files."""

    FRAMEWORK_PATTERN = re.compile(r'^[A-Za-z0-9_]+$')

    # CSV file definitions
    CSV_FILES = {
        'in_scope_reports': {
            'filename_pattern': 'in_scope_reports_{framework}.csv',
            'columns': ['In_Scope', 'New'],
            'description': 'In-Scope Reports'
        },
        'product_to_category': {
            'filename_pattern': 'join_for_product_to_reference_category_{framework}.csv',
            'columns': ['Main Category', 'Name', 'slice_name'],
            'description': 'Product to Category Mapping',
            'ancrdt_columns': ['rolc', 'join_identifier']
        },
        'product_il_definitions': {
            'filename_pattern': 'join_for_product_il_definitions_{framework}.csv',
            'columns': ['Name', 'Main Table', 'Filter', 'Related Tables', 'Comments'],
            'description': 'Product IL Definitions'
        }
    }

    # Supported frameworks
    FRAMEWORKS = ['FINREP_REF', 'ANCRDT_REF', 'AE_REF']

    def __init__(self, base_path: Optional[str] = None):
        """
        Initialize the manager.

        Args:
            base_path: Base path for CSV files. Defaults to artefacts/joins_configuration/
        """
        if base_path is None:
            self.base_path = os.path.join(
                settings.BASE_DIR,
                'artefacts',
                'joins_configuration'
            )
        else:
            self.base_path = base_path

        self.base_path = os.path.abspath(self.base_path)

        # Ensure directory exists
        os.makedirs(self.base_path, exist_ok=True)

    def _validate_framework(self, framework: str) -> str:
        """Validate framework names before using them in filesystem paths."""
        if not isinstance(framework, str):
            raise ValueError("Framework name must be a string")

        framework = framework.strip()
        if not framework or not self.FRAMEWORK_PATTERN.fullmatch(framework):
            raise ValueError("Framework name must contain only letters, numbers, and underscores")

        return framework

    def _resolve_file_path(self, filename: str) -> str:
        """Resolve a CSV path and ensure it stays inside the configured base directory."""
        try:
            return safe_join(self.base_path, filename)
        except SuspiciousFileOperation as exc:
            raise ValueError("Invalid file path") from exc

    def get_file_path(self, file_type: str, framework: str) -> str:
        """
        Get the full path for a CSV file.

        Args:
            file_type: Type of file (key from CSV_FILES)
            framework: Framework name (e.g., 'FINREP_REF')

        Returns:
            Full file path
        """
        if file_type not in self.CSV_FILES:
            raise ValueError(f"Unknown file type: {file_type}")

        framework = self._validate_framework(framework)
        filename_pattern = self.CSV_FILES[file_type]['filename_pattern']
        filename = filename_pattern.format(framework=framework)
        return self._resolve_file_path(filename)

    def get_columns(self, file_type: str, framework: str) -> List[str]:
        """
        Get expected columns for a file type.

        Args:
            file_type: Type of file
            framework: Framework name

        Returns:
            List of column names
        """
        file_def = self.CSV_FILES[file_type]

        # ANCRDT has different columns for product_to_category
        if framework == 'ANCRDT_REF' and file_type == 'product_to_category':
            return file_def.get('ancrdt_columns', file_def['columns'])

        return file_def['columns']

    def read_csv(self, file_type: str, framework: str) -> List[Dict[str, str]]:
        """
        Read CSV file and return as list of dictionaries.

        Args:
            file_type: Type of file to read
            framework: Framework name

        Returns:
            List of dictionaries representing CSV rows
        """
        file_path = self.get_file_path(file_type, framework)

        if not os.path.exists(file_path):
            return []

        rows = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)

        return rows

    def write_csv(self, file_type: str, framework: str, data: List[Dict[str, str]],
                  create_backup: bool = True) -> None:
        """
        Write data to CSV file.

        Args:
            file_type: Type of file to write
            framework: Framework name
            data: List of dictionaries to write
            create_backup: Whether to create backup before writing
        """
        file_path = self.get_file_path(file_type, framework)

        # Create backup if requested and file exists
        if create_backup and os.path.exists(file_path):
            self.create_backup(file_type, framework)

        # Get expected columns
        columns = self.get_columns(file_type, framework)

        # Write CSV
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()

            # Only write fields that match expected columns
            for row in data:
                filtered_row = {col: row.get(col, '') for col in columns}
                writer.writerow(filtered_row)

    def create_backup(self, file_type: str, framework: str) -> Optional[str]:
        """
        Create a backup of a CSV file.

        Args:
            file_type: Type of file
            framework: Framework name

        Returns:
            Backup file path or None if source doesn't exist
        """
        source_path = self.get_file_path(file_type, framework)

        if not os.path.exists(source_path):
            return None

        # Create backup filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = os.path.join(self.base_path, 'backups')
        os.makedirs(backup_dir, exist_ok=True)

        filename = os.path.basename(source_path)
        backup_filename = f"{os.path.splitext(filename)[0]}_{timestamp}.bak"
        backup_path = os.path.join(backup_dir, backup_filename)

        shutil.copy2(source_path, backup_path)
        return backup_path

    def validate_csv_structure(self, file_type: str, framework: str,
                               data: List[Dict[str, str]]) -> Tuple[bool, List[str]]:
        """
        Validate CSV data structure.

        Args:
            file_type: Type of file
            framework: Framework name
            data: Data to validate

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        expected_columns = self.get_columns(file_type, framework)

        # Check if data is empty
        if not data:
            return True, []  # Empty is valid

        # Check columns in first row
        actual_columns = set(data[0].keys())
        expected_columns_set = set(expected_columns)

        missing_columns = expected_columns_set - actual_columns
        if missing_columns:
            errors.append(f"Missing columns: {', '.join(missing_columns)}")

        # Validate data based on file type
        if file_type == 'in_scope_reports':
            errors.extend(self._validate_in_scope_reports(data))
        elif file_type == 'product_to_category':
            errors.extend(self._validate_product_to_category(data, framework))
        elif file_type == 'product_il_definitions':
            errors.extend(self._validate_product_il_definitions(data))

        return len(errors) == 0, errors

    def _validate_in_scope_reports(self, data: List[Dict[str, str]]) -> List[str]:
        """Validate in-scope reports data."""
        errors = []

        for i, row in enumerate(data, start=2):  # Start at 2 (1 is header)
            report_code = row.get('In_Scope', '').strip()

            if not report_code:
                errors.append(f"Row {i}: Empty report code")
                continue

            # Basic format check (should start with a letter and contain underscores)
            if not report_code[0].isalpha():
                errors.append(f"Row {i}: Report code '{report_code}' should start with a letter")

        return errors

    def _validate_product_to_category(self, data: List[Dict[str, str]],
                                      framework: str) -> List[str]:
        """
        Validate product to category mapping data.

        Supports flexible formats:
        - Legacy: TYP_INSTRMNT_970 (variable inferred)
        - New: TYP_INSTRMNT=TYP_INSTRMNT_970 (explicit variable)
        - Multi: TYP_INSTRMNT=TYP_INSTRMNT_970:TYP_CLLRL=TYP_CLLRL_1
        - Empty: no breakdown (all data processed together)
        """
        errors = []

        if framework == 'ANCRDT_REF':
            # ANCRDT has different validation
            for i, row in enumerate(data, start=2):
                rolc = row.get('rolc', '').strip()
                join_id = row.get('join_identifier', '').strip()

                if not rolc:
                    errors.append(f"Row {i}: Empty rolc")
                if not join_id:
                    errors.append(f"Row {i}: Empty join_identifier")
        else:
            # FINREP and other frameworks - flexible validation
            for i, row in enumerate(data, start=2):
                main_cat = row.get('Main Category', '').strip()
                name = row.get('Name', '').strip()
                slice_name = row.get('slice_name', '').strip()

                # Empty main_cat is now valid (no breakdown scenario)
                if main_cat:
                    # Validate using BreakdownCondition parser
                    validation_error = self._validate_main_category_format(main_cat, i)
                    if validation_error:
                        errors.append(validation_error)

                # Name and slice_name are required unless it's a no-breakdown row
                if main_cat:  # If there's a main category, name and slice are required
                    if not name:
                        errors.append(f"Row {i}: Empty Name")
                    if not slice_name:
                        errors.append(f"Row {i}: Empty slice_name")
                else:  # No-breakdown row - at least slice_name should be present for identification
                    if not slice_name and not name:
                        errors.append(f"Row {i}: Empty row - provide at least a slice_name for no-breakdown entries")

        return errors

    def _validate_main_category_format(self, main_cat: str, row_num: int) -> Optional[str]:
        """
        Validate Main Category format using BreakdownCondition parser.

        Args:
            main_cat: The Main Category value to validate
            row_num: Row number for error messages

        Returns:
            Error message string if invalid, None if valid.
        """
        try:
            condition = BreakdownCondition(main_cat)
            # Additional validation: ensure we have at least one condition
            if condition.is_empty():
                return None  # Empty is valid (no breakdown)

            # Validate that each condition has both variable and member
            for c in condition.conditions:
                if not c.get('variable'):
                    return f"Row {row_num}: Missing variable in condition '{main_cat}'"
                if not c.get('member'):
                    return f"Row {row_num}: Missing member in condition '{main_cat}'"

            return None  # Valid

        except ValueError as e:
            return f"Row {row_num}: Invalid Main Category format - {str(e)}"

    def _validate_product_il_definitions(self, data: List[Dict[str, str]]) -> List[str]:
        """Validate product IL definitions data."""
        errors = []

        for i, row in enumerate(data, start=2):
            name = row.get('Name', '').strip()
            main_table = row.get('Main Table', '').strip()

            if not name:
                errors.append(f"Row {i}: Empty Name")

            if not main_table:
                errors.append(f"Row {i}: Empty Main Table")

            # Filter and Related Tables are optional but should be valid if present
            related_tables = row.get('Related Tables', '').strip()
            if related_tables:
                # Should be colon-separated
                if ':' not in related_tables and len(related_tables.split()) > 1:
                    errors.append(f"Row {i}: Related Tables should be colon-separated")

        return errors

    def file_exists(self, file_type: str, framework: str) -> bool:
        """
        Check if a CSV file exists.

        Args:
            file_type: Type of file
            framework: Framework name

        Returns:
            True if file exists
        """
        file_path = self.get_file_path(file_type, framework)
        return os.path.exists(file_path)

    def get_file_info(self, file_type: str, framework: str) -> Optional[Dict[str, any]]:
        """
        Get information about a CSV file.

        Args:
            file_type: Type of file
            framework: Framework name

        Returns:
            Dictionary with file info or None if file doesn't exist
        """
        file_path = self.get_file_path(file_type, framework)

        if not os.path.exists(file_path):
            return None

        stat = os.stat(file_path)

        return {
            'path': file_path,
            'size': stat.st_size,
            'modified': datetime.fromtimestamp(stat.st_mtime),
            'exists': True,
            'row_count': len(self.read_csv(file_type, framework))
        }

    def export_to_csv_response(self, file_type: str, framework: str):
        """
        Export CSV file for download (Django HttpResponse).

        Args:
            file_type: Type of file
            framework: Framework name

        Returns:
            Django HttpResponse with CSV content
        """
        from django.http import HttpResponse

        data = self.read_csv(file_type, framework)
        columns = self.get_columns(file_type, framework)

        response = HttpResponse(content_type='text/csv')
        filename = self.CSV_FILES[file_type]['filename_pattern'].format(framework=framework)
        response['Content-Disposition'] = f'attachment; filename="{filename}"'

        writer = csv.DictWriter(response, fieldnames=columns)
        writer.writeheader()

        for row in data:
            filtered_row = {col: row.get(col, '') for col in columns}
            writer.writerow(filtered_row)

        return response

    def import_from_uploaded_file(self, file_type: str, framework: str,
                                   uploaded_file) -> Tuple[bool, List[str], List[Dict[str, str]]]:
        """
        Import data from uploaded CSV file.

        Args:
            file_type: Type of file
            framework: Framework name
            uploaded_file: Django UploadedFile object

        Returns:
            Tuple of (success, errors, data)
        """
        errors = []

        try:
            # Read uploaded file
            content = uploaded_file.read().decode('utf-8')
            lines = content.splitlines()
            reader = csv.DictReader(lines)

            data = list(reader)

            # Validate structure
            is_valid, validation_errors = self.validate_csv_structure(file_type, framework, data)

            if not is_valid:
                errors.extend(validation_errors)
                return False, errors, []

            return True, [], data

        except Exception as e:
            errors.append(f"Error reading file: {str(e)}")
            return False, errors, []

    @classmethod
    def get_available_frameworks(cls) -> List[str]:
        """Get list of supported frameworks."""
        return cls.FRAMEWORKS.copy()

    @classmethod
    def get_file_types(cls) -> Dict[str, Dict[str, any]]:
        """Get dictionary of available file types."""
        return cls.CSV_FILES.copy()
