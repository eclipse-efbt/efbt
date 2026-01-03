"""
Code Sync Utility for Generated Code Lifecycle

This module manages the synchronization of generated Python code between:
- results/generated_python/ (unified staging area for edits)
- pybirdai/process_steps/filter_code/ (production execution area)

New unified directory structure (2025):

  Results (generation output):
  results/generated_python/
  ├── datasets/
  │   └── ANCRDT/
  │       ├── filter/    - report cells / output tables
  │       └── joins/     - logic files
  └── templates/
      ├── FINREP/
      │   ├── filter/    - report cells
      │   └── joins/     - logic files
      └── COREP/
          ├── filter/
          └── joins/

  Filter Code (runtime):
  filter_code/
  ├── lib/               - shared utilities
  ├── datasets/
  │   └── ANCRDT/
  │       ├── filter/
  │       └── joins/
  └── templates/
      ├── FINREP/
      │   ├── filter/
      │   └── joins/
      └── COREP/
          ├── filter/
          └── joins/

Lifecycle Pattern:
1. Generate → results/generated_python/{type}/{FRAMEWORK}/{filter|joins}/*.py
2. Edit → Modify in web UI
3. Deploy → Sync to filter_code/{type}/{FRAMEWORK}/{filter|joins}/
"""

import os
import shutil
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path


class CodeSyncManager:
    """Manages synchronization of generated code between directories"""

    def __init__(self, base_dir: Optional[str] = None, framework: Optional[str] = None, is_dataset: bool = False):
        """
        Initialize the sync manager with directory paths.

        Args:
            base_dir: Base directory (defaults to birds_nest/)
            framework: Framework name (e.g., 'COREP', 'FINREP', 'ANCRDT')
            is_dataset: True for datasets (ANCRDT), False for templates (FINREP, COREP)
        """
        if base_dir is None:
            # Auto-detect base directory
            current_file = Path(__file__).resolve()
            base_dir = current_file.parent.parent.parent

        self.base_dir = Path(base_dir)
        self.framework = framework
        self.is_dataset = is_dataset

        # Determine type: datasets or templates
        framework_upper = framework.upper().replace('_REF', '') if framework else 'LEGACY'
        type_dir = 'datasets' if is_dataset or framework_upper == 'ANCRDT' else 'templates'

        # Staging directory: results/generated_python/{type}/{FRAMEWORK}/
        self.staging_dir = self.base_dir / 'results' / 'generated_python' / type_dir / framework_upper

        # Production directory: filter_code/{type}/{FRAMEWORK}/
        filter_code_base = self.base_dir / 'pybirdai' / 'process_steps' / 'filter_code'
        self.production_dir = filter_code_base / type_dir / framework_upper

        # Subdirectories for filter and joins
        self.staging_filter_dir = self.staging_dir / 'filter'
        self.staging_joins_dir = self.staging_dir / 'joins'
        self.production_filter_dir = self.production_dir / 'filter'
        self.production_joins_dir = self.production_dir / 'joins'

        # Lib directory for shared utilities
        self.lib_dir = filter_code_base / 'lib'

        # Ensure directories exist
        self.staging_filter_dir.mkdir(parents=True, exist_ok=True)
        self.staging_joins_dir.mkdir(parents=True, exist_ok=True)
        self.production_filter_dir.mkdir(parents=True, exist_ok=True)
        self.production_joins_dir.mkdir(parents=True, exist_ok=True)
        self.lib_dir.mkdir(parents=True, exist_ok=True)

    def _get_production_path(self, filename: str, source_type: str = None) -> Path:
        """
        Get the correct production path for a file based on the new directory structure.

        Args:
            filename: Name of the file (e.g., 'ANCRDT_INSTRMNT_C_1_logic.py', 'corep_report_cells.py')
            source_type: Type of file ('filter' or 'joins')

        Returns:
            Path to the production location
        """
        # Determine if it's a filter or joins file
        if source_type == 'filter' or '_report_cells.py' in filename or filename == 'ancrdt_output_tables.py':
            return self.production_filter_dir / filename
        elif source_type == 'joins' or filename.endswith('_logic.py'):
            return self.production_joins_dir / filename
        else:
            # Default: use filter directory
            return self.production_filter_dir / filename

    def sync_file(self, filename: str, source_type: str = None, create_backup: bool = True) -> Dict[str, any]:
        """
        Sync a single file from staging to production.

        Args:
            filename: Name of the file to sync (e.g., 'ANCRDT_INSTRMNT_C_1_logic.py')
            source_type: Type of file ('filter' or 'joins'). Auto-detected if not provided.
            create_backup: Whether to create a backup before overwriting

        Returns:
            Dict with sync status and metadata
        """
        # Auto-detect source type if not provided
        if source_type is None:
            if '_logic.py' in filename:
                source_type = 'joins'
            else:
                source_type = 'filter'

        # Get source and destination paths
        if source_type == 'joins':
            source_path = self.staging_joins_dir / filename
        else:
            source_path = self.staging_filter_dir / filename

        dest_path = self._get_production_path(filename, source_type)

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
        source_path = self.staging_dir / filename
        dest_path = self._get_production_path(filename)

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

    def _get_file_status(self, filename: str) -> Dict[str, any]:
        """
        Get detailed status for a single file.

        Args:
            filename: Name of the file

        Returns:
            Dict with status information
        """
        source_path = self.staging_dir / filename
        dest_path = self._get_production_path(filename)
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
