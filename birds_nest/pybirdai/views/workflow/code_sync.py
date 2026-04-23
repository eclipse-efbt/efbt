"""
Code Sync Utility for ANCRDT Generated Code Lifecycle

This module manages the synchronization of generated Python code between:
- results/generated_python_joins/ (staging area for edits)
- pybirdai/process_steps/filter_code/ (production execution area)

Lifecycle Pattern:
1. Generate → results/generated_python_joins/file.py + file.py.generated
2. Edit → Modify results/generated_python_joins/file.py in web UI
3. Deploy → Sync to process_steps/filter_code/file.py for execution
"""

import os
import re
import shutil
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path


class CodeSyncManager:
    """Manages synchronization of generated code between directories"""

    SAFE_FILENAME_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]*\.py$')

    def __init__(self, base_dir: Optional[str] = None):
        """
        Initialize the sync manager with directory paths.

        Args:
            base_dir: Base directory (defaults to birds_nest/)
        """
        if base_dir is None:
            # Auto-detect base directory
            # code_sync.py is at pybirdai/views/workflow/code_sync.py
            # Need 4 parent levels to reach birds_nest/
            current_file = Path(__file__).resolve()
            base_dir = current_file.parent.parent.parent.parent

        self.base_dir = Path(base_dir)
        self.staging_dir = self.base_dir / 'results' / 'generated_python_joins'
        self.production_dir = self.base_dir / 'pybirdai' / 'process_steps' / 'filter_code'

        # Ensure directories exist
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.production_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def _validate_filename(cls, filename: str) -> str:
        """Reject path traversal and unexpected filenames before touching the filesystem."""
        if not isinstance(filename, str) or not cls.SAFE_FILENAME_PATTERN.fullmatch(filename):
            raise ValueError("Invalid file name")

        return filename

    def sync_file(self, filename: str, create_backup: bool = True) -> Dict[str, any]:
        """
        Sync a single file from staging to production.

        Args:
            filename: Name of the file to sync (e.g., 'ANCRDT_INSTRMNT_C_1_logic.py')
            create_backup: Whether to create a backup before overwriting

        Returns:
            Dict with sync status and metadata
        """
        filename = self._validate_filename(filename)
        source_path = self.staging_dir / filename
        dest_path = self.production_dir / filename

        # Special handling for report_cells.py which is in generated_python_filters
        if filename == 'report_cells.py' and not source_path.exists():
            filters_staging_dir = self.base_dir / 'results' / 'generated_python_filters'
            source_path = filters_staging_dir / filename

        result = {
            'success': False,
            'filename': filename,
            'timestamp': datetime.now().isoformat(),
            'message': '',
            'backup_created': False
        }

        # Check if source exists
        if not source_path.exists():
            result['message'] = f"Source file not found: {source_path}"
            return result

        # Create backup if destination exists and backup requested
        if dest_path.exists() and create_backup:
            backup_path = Path(str(dest_path) + '.backup')
            try:
                shutil.copy2(dest_path, backup_path)
                result['backup_created'] = True
                result['backup_path'] = str(backup_path)
            except Exception as e:
                result['message'] = f"Failed to create backup: {str(e)}"
                return result

        # Copy file to production
        try:
            shutil.copy2(source_path, dest_path)
            result['success'] = True
            result['message'] = f"Successfully synced {filename} to production"
            result['source_path'] = str(source_path)
            result['dest_path'] = str(dest_path)
        except Exception as e:
            result['message'] = f"Failed to sync file: {str(e)}"

        return result

    def sync_all_ancrdt_files(self, create_backup: bool = True) -> List[Dict]:
        """
        Sync all ANCRDT-related files from staging to production.

        Args:
            create_backup: Whether to create backups before overwriting

        Returns:
            List of sync results for each file
        """
        results = []

        # Find all ANCRDT Python files in staging
        ancrdt_patterns = ['ANCRDT_*.py', 'ancrdt_*.py']
        ancrdt_files = []

        for pattern in ancrdt_patterns:
            ancrdt_files.extend(self.staging_dir.glob(pattern))

        # Filter out .generated and .backup files
        ancrdt_files = [
            f for f in ancrdt_files
            if not (f.name.endswith('.generated') or f.name.endswith('.backup'))
        ]

        for file_path in ancrdt_files:
            result = self.sync_file(file_path.name, create_backup)
            results.append(result)

        return results

    def is_synced(self, filename: str) -> bool:
        """
        Check if a file is synchronized between staging and production.

        Args:
            filename: Name of the file to check

        Returns:
            True if files are identical, False otherwise
        """
        filename = self._validate_filename(filename)
        source_path = self.staging_dir / filename
        dest_path = self.production_dir / filename

        # If either doesn't exist, not synced
        if not source_path.exists() or not dest_path.exists():
            return False

        # Compare file contents
        try:
            with open(source_path, 'r', encoding='utf-8') as f1:
                source_content = f1.read()
            with open(dest_path, 'r', encoding='utf-8') as f2:
                dest_content = f2.read()

            return source_content == dest_content
        except Exception:
            return False

    def get_sync_status(self) -> Dict[str, Dict]:
        """
        Get sync status for all ANCRDT files.

        Returns:
            Dict mapping filename to status information
        """
        status_map = {}

        # Find all ANCRDT files in staging
        ancrdt_patterns = ['ANCRDT_*.py', 'ancrdt_*.py']
        ancrdt_files = []

        for pattern in ancrdt_patterns:
            ancrdt_files.extend(self.staging_dir.glob(pattern))

        # Filter out .generated and .backup files
        ancrdt_files = [
            f for f in ancrdt_files
            if not (f.name.endswith('.generated') or f.name.endswith('.backup'))
        ]

        for file_path in ancrdt_files:
            filename = file_path.name
            status_map[filename] = self._get_file_status(filename)

        return status_map

    def get_sync_status_finrep(self) -> Dict[str, Dict]:
        """
        Get sync status for all FINREP files (F_*.py pattern + report_cells.py).

        Returns:
            Dict mapping filename to status information
        """
        status_map = {}

        # Find all FINREP files in staging (F_*.py)
        finrep_files = list(self.staging_dir.glob('F_*.py'))

        # Filter out .generated and .backup files
        finrep_files = [
            f for f in finrep_files
            if not (f.name.endswith('.generated') or f.name.endswith('.backup'))
        ]

        for file_path in finrep_files:
            filename = file_path.name
            status_map[filename] = self._get_file_status(filename)

        # Also check for report_cells.py in generated_python_filters
        filters_staging_dir = self.base_dir / 'results' / 'generated_python_filters'
        report_cells_path = filters_staging_dir / 'report_cells.py'
        if report_cells_path.exists():
            status_map['report_cells.py'] = self._get_file_status_custom(
                'report_cells.py',
                filters_staging_dir,
                self.production_dir
            )

        return status_map

    def _get_file_status_custom(self, filename: str, staging_dir: Path, production_dir: Path) -> Dict[str, any]:
        """
        Get detailed status for a file with custom staging/production directories.

        Args:
            filename: Name of the file
            staging_dir: Custom staging directory
            production_dir: Custom production directory

        Returns:
            Dict with status information
        """
        filename = self._validate_filename(filename)
        source_path = staging_dir / filename
        dest_path = production_dir / filename
        generated_path = staging_dir / (filename + '.generated')

        status = {
            'filename': filename,
            'exists_in_staging': source_path.exists(),
            'exists_in_production': dest_path.exists(),
            'has_generated_base': generated_path.exists(),
            'is_synced': False,
            'is_edited': False,
            'staging_mtime': None,
            'production_mtime': None,
        }

        # Get modification times
        if source_path.exists():
            status['staging_mtime'] = datetime.fromtimestamp(source_path.stat().st_mtime).isoformat()

        if dest_path.exists():
            status['production_mtime'] = datetime.fromtimestamp(dest_path.stat().st_mtime).isoformat()

        # Check if synced (compare file contents)
        if source_path.exists() and dest_path.exists():
            try:
                with open(source_path, 'r', encoding='utf-8') as f1:
                    source_content = f1.read()
                with open(dest_path, 'r', encoding='utf-8') as f2:
                    dest_content = f2.read()
                status['is_synced'] = source_content == dest_content
            except Exception:
                status['is_synced'] = False
        else:
            status['is_synced'] = False

        # Check if edited (differs from .generated base)
        if source_path.exists() and generated_path.exists():
            try:
                with open(source_path, 'r', encoding='utf-8') as f1:
                    source_content = f1.read()
                with open(generated_path, 'r', encoding='utf-8') as f2:
                    generated_content = f2.read()
                status['is_edited'] = source_content != generated_content
            except Exception:
                status['is_edited'] = False

        return status

    def sync_all_finrep_files(self, create_backup: bool = True) -> List[Dict]:
        """
        Sync all FINREP-related files from staging to production.

        Args:
            create_backup: Whether to create backups before overwriting

        Returns:
            List of sync results for each file
        """
        results = []

        # Find all FINREP Python files in staging (F_*.py)
        finrep_files = list(self.staging_dir.glob('F_*.py'))

        # Filter out .generated and .backup files
        finrep_files = [
            f for f in finrep_files
            if not (f.name.endswith('.generated') or f.name.endswith('.backup'))
        ]

        for file_path in finrep_files:
            result = self.sync_file(file_path.name, create_backup)
            results.append(result)

        # Also sync report_cells.py from generated_python_filters
        filters_staging_dir = self.base_dir / 'results' / 'generated_python_filters'
        report_cells_path = filters_staging_dir / 'report_cells.py'
        if report_cells_path.exists():
            result = self.sync_file_custom(
                'report_cells.py',
                filters_staging_dir,
                self.production_dir,
                create_backup
            )
            results.append(result)

        return results

    def sync_file_custom(self, filename: str, staging_dir: Path, production_dir: Path, create_backup: bool = True) -> Dict[str, any]:
        """
        Sync a single file from custom staging to production directory.

        Args:
            filename: Name of the file to sync
            staging_dir: Custom staging directory
            production_dir: Custom production directory
            create_backup: Whether to create a backup before overwriting

        Returns:
            Dict with sync status and metadata
        """
        filename = self._validate_filename(filename)
        source_path = staging_dir / filename
        dest_path = production_dir / filename

        result = {
            'success': False,
            'filename': filename,
            'timestamp': datetime.now().isoformat(),
            'message': '',
            'backup_created': False
        }

        # Check if source exists
        if not source_path.exists():
            result['message'] = f"Source file not found: {source_path}"
            return result

        # Create backup if destination exists and backup requested
        if dest_path.exists() and create_backup:
            backup_path = Path(str(dest_path) + '.backup')
            try:
                shutil.copy2(dest_path, backup_path)
                result['backup_created'] = True
                result['backup_path'] = str(backup_path)
            except Exception as e:
                result['message'] = f"Failed to create backup: {str(e)}"
                return result

        # Copy file to production
        try:
            shutil.copy2(source_path, dest_path)
            result['success'] = True
            result['message'] = f"Successfully synced {filename} to production"
            result['source_path'] = str(source_path)
            result['dest_path'] = str(dest_path)
        except Exception as e:
            result['message'] = f"Failed to sync file: {str(e)}"

        return result

    def _get_file_status(self, filename: str) -> Dict[str, any]:
        """
        Get detailed status for a single file.

        Args:
            filename: Name of the file

        Returns:
            Dict with status information
        """
        filename = self._validate_filename(filename)
        source_path = self.staging_dir / filename
        dest_path = self.production_dir / filename
        generated_path = self.staging_dir / (filename + '.generated')

        status = {
            'filename': filename,
            'exists_in_staging': source_path.exists(),
            'exists_in_production': dest_path.exists(),
            'has_generated_base': generated_path.exists(),
            'is_synced': False,
            'is_edited': False,
            'staging_mtime': None,
            'production_mtime': None,
        }

        # Get modification times
        if source_path.exists():
            status['staging_mtime'] = datetime.fromtimestamp(source_path.stat().st_mtime).isoformat()

        if dest_path.exists():
            status['production_mtime'] = datetime.fromtimestamp(dest_path.stat().st_mtime).isoformat()

        # Check if synced
        status['is_synced'] = self.is_synced(filename)

        # Check if edited (differs from .generated base)
        if source_path.exists() and generated_path.exists():
            try:
                with open(source_path, 'r', encoding='utf-8') as f1:
                    source_content = f1.read()
                with open(generated_path, 'r', encoding='utf-8') as f2:
                    generated_content = f2.read()

                status['is_edited'] = source_content != generated_content
            except Exception:
                status['is_edited'] = False

        return status

    def has_manual_edits(self, filename: str) -> bool:
        """
        Check if a file has been manually edited (differs from .generated base).

        Args:
            filename: Name of the file to check

        Returns:
            True if file has been edited, False otherwise
        """
        filename = self._validate_filename(filename)
        source_path = self.staging_dir / filename
        generated_path = self.staging_dir / (filename + '.generated')

        if not source_path.exists() or not generated_path.exists():
            return False

        try:
            with open(source_path, 'r', encoding='utf-8') as f1:
                source_content = f1.read()
            with open(generated_path, 'r', encoding='utf-8') as f2:
                generated_content = f2.read()

            return source_content != generated_content
        except Exception:
            return False

    def get_diff_summary(self, filename: str) -> Optional[Dict]:
        """
        Get a summary of differences between staging and production versions.

        Args:
            filename: Name of the file

        Returns:
            Dict with diff information or None if comparison fails
        """
        filename = self._validate_filename(filename)
        source_path = self.staging_dir / filename
        dest_path = self.production_dir / filename

        if not source_path.exists() or not dest_path.exists():
            return None

        try:
            with open(source_path, 'r', encoding='utf-8') as f1:
                source_lines = f1.readlines()
            with open(dest_path, 'r', encoding='utf-8') as f2:
                dest_lines = f2.readlines()

            return {
                'filename': filename,
                'staging_lines': len(source_lines),
                'production_lines': len(dest_lines),
                'line_diff': len(source_lines) - len(dest_lines),
                'are_identical': source_lines == dest_lines
            }
        except Exception as e:
            return {
                'filename': filename,
                'error': str(e)
            }


# Convenience functions for direct use
def sync_file(filename: str, create_backup: bool = True) -> Dict:
    """Sync a single file from staging to production"""
    manager = CodeSyncManager()
    return manager.sync_file(filename, create_backup)


def sync_all_ancrdt_files(create_backup: bool = True) -> List[Dict]:
    """Sync all ANCRDT files from staging to production"""
    manager = CodeSyncManager()
    return manager.sync_all_ancrdt_files(create_backup)


def is_synced(filename: str) -> bool:
    """Check if a file is synchronized"""
    manager = CodeSyncManager()
    return manager.is_synced(filename)


def get_sync_status() -> Dict[str, Dict]:
    """Get sync status for all ANCRDT files"""
    manager = CodeSyncManager()
    return manager.get_sync_status()


def has_manual_edits(filename: str) -> bool:
    """Check if a file has manual edits"""
    manager = CodeSyncManager()
    return manager.has_manual_edits(filename)
