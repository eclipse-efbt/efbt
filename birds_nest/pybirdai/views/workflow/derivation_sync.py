"""
Derivation Sync Manager for Derived Fields Lifecycle

This module manages the synchronization of derivation files between:
- resources/derivation_files/generated_from_member_links/ (staging - cube link)
- resources/derivation_files/generated_from_logical_transformation_rules/ (staging - auto)
- resources/derivation_files/manually_generated/ (production)

Lifecycle Pattern:
1. Generate -> Files created in generated_from_* directories
2. Edit -> Modify files in web UI (saved back to staging location)
3. Deploy -> Promote to manually_generated/ (becomes manual, highest priority)

When a file is deployed, it moves to manually_generated/ and takes precedence
over any auto-generated version of the same derivation.
"""

import os
import json
import hashlib
import shutil
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import logging

from pybirdai.utils.secure_error_handling import SecureErrorHandler

logger = logging.getLogger(__name__)


def _safe_derivation_error(exception: Exception, context: str, fallback: str) -> str:
    """Log internal exception details and return a stable public-safe error string."""
    SecureErrorHandler.handle_exception(exception, context)
    return fallback


def is_cube_link_allowed() -> bool:
    """Check if cube_link derivations are allowed based on Context setting."""
    try:
        from pybirdai.context.context import Context
        return Context.cube_link_derivations_allowed
    except Exception:
        return False


class DerivationSyncManager:
    """Manages synchronization of derivation files between staging and production"""

    # File type constants
    TYPE_MANUAL = 'manual'
    TYPE_CUBE_LINK = 'cube_link'
    TYPE_AUTO = 'auto'

    # Status constants
    STATUS_PRODUCTION = 'production'
    STATUS_STAGING_UNMODIFIED = 'staging_unmodified'
    STATUS_STAGING_MODIFIED = 'staging_modified'
    STATUS_SHADOWED = 'shadowed'

    def __init__(self, base_dir: Optional[str] = None):
        """
        Initialize the sync manager with directory paths.

        Args:
            base_dir: Base directory (defaults to birds_nest/)
        """
        if base_dir is None:
            # Auto-detect base directory
            # derivation_sync.py is at pybirdai/views/workflow/derivation_sync.py
            # Need 4 parent levels to reach birds_nest/
            current_file = Path(__file__).resolve()
            base_dir = current_file.parent.parent.parent.parent

        self.base_dir = Path(base_dir)

        # Directory paths
        self.derivation_base = self.base_dir / 'resources' / 'derivation_files'
        self.manual_dir = self.derivation_base / 'manually_generated'
        self.cube_link_dir = self.derivation_base / 'generated_from_member_links'
        self.auto_dir = self.derivation_base / 'generated_from_logical_transformation_rules'

        # Checksums file to track original generated state
        self.checksums_file = self.derivation_base / '.generated_checksums.json'

        # Ensure directories exist
        self.manual_dir.mkdir(parents=True, exist_ok=True)
        self.cube_link_dir.mkdir(parents=True, exist_ok=True)
        self.auto_dir.mkdir(parents=True, exist_ok=True)

    def _compute_checksum(self, file_path: Path) -> str:
        """Compute SHA256 checksum of a file."""
        if not file_path.exists():
            return ''

        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _load_checksums(self) -> Dict[str, str]:
        """Load stored checksums from file."""
        if not self.checksums_file.exists():
            return {}

        try:
            with open(self.checksums_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

    def _save_checksums(self, checksums: Dict[str, str]):
        """Save checksums to file."""
        with open(self.checksums_file, 'w', encoding='utf-8') as f:
            json.dump(checksums, f, indent=2)

    def update_checksum(self, relative_path: str):
        """
        Update the stored checksum for a file (called after generation).

        Args:
            relative_path: Path relative to derivation_base (e.g., 'generated_from_member_links/foo.py')
        """
        checksums = self._load_checksums()
        file_path = self.derivation_base / relative_path
        checksums[relative_path] = self._compute_checksum(file_path)
        self._save_checksums(checksums)

    def is_modified(self, relative_path: str) -> bool:
        """
        Check if a file has been modified from its generated state.

        Args:
            relative_path: Path relative to derivation_base

        Returns:
            True if file differs from stored checksum
        """
        checksums = self._load_checksums()
        file_path = self.derivation_base / relative_path

        if relative_path not in checksums:
            # No stored checksum - not tracked as modified
            return False

        current_checksum = self._compute_checksum(file_path)
        return current_checksum != checksums[relative_path]

    def get_all_derivation_files(self) -> List[Dict]:
        """
        Get all derivation files with their status.

        Returns:
            List of dicts with file info and status
        """
        files = []
        checksums = self._load_checksums()

        # Get manual derivations that define specific class.field combinations
        manual_derivations = self._extract_derivations_from_dir(self.manual_dir)

        # Track which class.field combos are defined in manual files
        manual_keys = set()
        for deriv in manual_derivations:
            key = f"{deriv['class_name']}.{deriv['field_name']}"
            manual_keys.add(key)

        # 1. Manual files (production)
        for py_file in sorted(self.manual_dir.glob('*.py')):
            if py_file.name.startswith('__'):
                continue

            relative_path = f"manually_generated/{py_file.name}"
            files.append({
                'filename': py_file.name,
                'relative_path': relative_path,
                'full_path': str(py_file),
                'type': self.TYPE_MANUAL,
                'status': self.STATUS_PRODUCTION,
                'is_modified': False,  # Manual files are always "as-is"
                'mtime': datetime.fromtimestamp(py_file.stat().st_mtime).isoformat(),
                'size': py_file.stat().st_size,
            })

        # 2. Cube link files (staging) - only if allowed
        if is_cube_link_allowed():
            for py_file in sorted(self.cube_link_dir.glob('*.py')):
                if py_file.name.startswith('__'):
                    continue

                relative_path = f"generated_from_member_links/{py_file.name}"

                # Check if any derivation in this file is shadowed by manual
                file_derivations = self._extract_derivations_from_file(py_file)
                is_shadowed = False
                for deriv in file_derivations:
                    key = f"{deriv['class_name']}.{deriv['field_name']}"
                    if key in manual_keys:
                        is_shadowed = True
                        break

                is_modified = self.is_modified(relative_path)

                if is_shadowed:
                    status = self.STATUS_SHADOWED
                elif is_modified:
                    status = self.STATUS_STAGING_MODIFIED
                else:
                    status = self.STATUS_STAGING_UNMODIFIED

                files.append({
                    'filename': py_file.name,
                    'relative_path': relative_path,
                    'full_path': str(py_file),
                    'type': self.TYPE_CUBE_LINK,
                    'status': status,
                    'is_modified': is_modified,
                    'is_shadowed': is_shadowed,
                    'mtime': datetime.fromtimestamp(py_file.stat().st_mtime).isoformat(),
                    'size': py_file.stat().st_size,
                })

        # 3. Auto-generated files (staging)
        for py_file in sorted(self.auto_dir.glob('*.py')):
            if py_file.name.startswith('__'):
                continue

            relative_path = f"generated_from_logical_transformation_rules/{py_file.name}"

            # Check if any derivation in this file is shadowed by manual
            file_derivations = self._extract_derivations_from_file(py_file)
            is_shadowed = False
            for deriv in file_derivations:
                key = f"{deriv['class_name']}.{deriv['field_name']}"
                if key in manual_keys:
                    is_shadowed = True
                    break

            is_modified = self.is_modified(relative_path)

            if is_shadowed:
                status = self.STATUS_SHADOWED
            elif is_modified:
                status = self.STATUS_STAGING_MODIFIED
            else:
                status = self.STATUS_STAGING_UNMODIFIED

            files.append({
                'filename': py_file.name,
                'relative_path': relative_path,
                'full_path': str(py_file),
                'type': self.TYPE_AUTO,
                'status': status,
                'is_modified': is_modified,
                'is_shadowed': is_shadowed,
                'mtime': datetime.fromtimestamp(py_file.stat().st_mtime).isoformat(),
                'size': py_file.stat().st_size,
            })

        return files

    def _extract_derivations_from_dir(self, directory: Path) -> List[Dict]:
        """Extract all derivations from Python files in a directory."""
        derivations = []
        for py_file in directory.glob('*.py'):
            if py_file.name.startswith('__'):
                continue
            derivations.extend(self._extract_derivations_from_file(py_file))
        return derivations

    def _extract_derivations_from_file(self, file_path: Path) -> List[Dict]:
        """
        Extract derivations (class.field pairs) from a Python file using AST.

        Args:
            file_path: Path to Python file

        Returns:
            List of dicts with class_name and field_name
        """
        import ast

        derivations = []

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                source = f.read()

            tree = ast.parse(source)

            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    class_name = node.name
                    # Remove DerivationMixin suffix if present
                    if class_name.endswith('DerivationMixin'):
                        class_name = class_name[:-len('DerivationMixin')]

                    for item in node.body:
                        if isinstance(item, ast.FunctionDef):
                            # Check for @lineage decorator
                            has_lineage = False
                            for decorator in item.decorator_list:
                                if isinstance(decorator, ast.Call):
                                    if isinstance(decorator.func, ast.Name) and decorator.func.id == 'lineage':
                                        has_lineage = True
                                        break
                                elif isinstance(decorator, ast.Name):
                                    if decorator.id == 'lineage':
                                        has_lineage = True
                                        break

                            if has_lineage:
                                derivations.append({
                                    'class_name': class_name,
                                    'field_name': item.name,
                                })
        except Exception as e:
            logger.warning(f"Error parsing {file_path}: {e}")

        return derivations

    def get_sync_status_summary(self) -> Dict:
        """
        Get a summary of sync status across all derivation files.

        Returns:
            Dict with counts by status
        """
        files = self.get_all_derivation_files()

        summary = {
            'total': len(files),
            'production': 0,
            'staging_modified': 0,
            'staging_unmodified': 0,
            'shadowed': 0,
            'by_type': {
                self.TYPE_MANUAL: 0,
                self.TYPE_CUBE_LINK: 0,
                self.TYPE_AUTO: 0,
            }
        }

        for f in files:
            summary['by_type'][f['type']] += 1

            if f['status'] == self.STATUS_PRODUCTION:
                summary['production'] += 1
            elif f['status'] == self.STATUS_STAGING_MODIFIED:
                summary['staging_modified'] += 1
            elif f['status'] == self.STATUS_STAGING_UNMODIFIED:
                summary['staging_unmodified'] += 1
            elif f['status'] == self.STATUS_SHADOWED:
                summary['shadowed'] += 1

        return summary

    def read_file(self, relative_path: str) -> Optional[str]:
        """
        Read content of a derivation file.

        Args:
            relative_path: Path relative to derivation_base

        Returns:
            File content or None if not found
        """
        file_path = self.derivation_base / relative_path

        if not file_path.exists():
            return None

        # Security check: ensure path is within derivation_base
        try:
            file_path.resolve().relative_to(self.derivation_base.resolve())
        except ValueError:
            logger.warning(f"Attempted path traversal: {relative_path}")
            return None

        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()

    def save_file(self, relative_path: str, content: str) -> Dict:
        """
        Save content to a derivation file.

        Args:
            relative_path: Path relative to derivation_base
            content: New file content

        Returns:
            Dict with save result
        """
        file_path = self.derivation_base / relative_path

        # Security check: ensure path is within derivation_base
        try:
            file_path.resolve().relative_to(self.derivation_base.resolve())
        except ValueError:
            return {
                'success': False,
                'error': 'Invalid path'
            }

        try:
            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)

            return {
                'success': True,
                'path': relative_path,
                'message': f'Saved {relative_path}'
            }
        except Exception as e:
            return {
                'success': False,
                'error': _safe_derivation_error(
                    e,
                    f'saving derivation file {relative_path}',
                    'Unable to save derivation file.',
                )
            }

    def deploy_to_manual(self, relative_path: str, new_filename: Optional[str] = None, split_by_derivation: bool = True) -> Dict:
        """
        Deploy a staging file to manually_generated/ directory.

        This promotes the file to "manual" status with highest priority.
        By default, splits the file into individual derivation files (one per CLASS_FIELD).

        Args:
            relative_path: Path relative to derivation_base (e.g., 'generated_from_member_links/foo.py')
            new_filename: Optional new filename (only used if split_by_derivation=False)
            split_by_derivation: If True, create separate files for each derivation rule

        Returns:
            Dict with deployment result
        """
        source_path = self.derivation_base / relative_path

        if not source_path.exists():
            return {
                'success': False,
                'error': f'Source file not found: {relative_path}'
            }

        derivations = self._extract_derivations_from_file(source_path)

        if not derivations:
            return {
                'success': False,
                'error': 'No derivations found in file'
            }

        # If only one derivation or split disabled, deploy as single file
        if len(derivations) == 1 or not split_by_derivation:
            return self._deploy_single_file(source_path, relative_path, new_filename, derivations)

        # Split into multiple files - one per derivation
        return self._deploy_split_files(source_path, relative_path)

    def _deploy_single_file(self, source_path: Path, relative_path: str,
                           new_filename: Optional[str], derivations: List[Dict]) -> Dict:
        """Deploy source file as a single file to manual directory."""
        if new_filename:
            target_filename = new_filename
        else:
            class_name = derivations[0]['class_name']
            if len(derivations) == 1:
                field_name = derivations[0]['field_name']
                target_filename = f"{class_name}_{field_name}_derived.py"
            else:
                target_filename = f"{class_name}_derived.py"

        target_path = self.manual_dir / target_filename

        # Create backup if target exists
        backup_path = None
        if target_path.exists():
            backup_path = target_path.with_suffix('.py.backup')
            shutil.copy2(target_path, backup_path)

        try:
            shutil.copy2(source_path, target_path)

            result = {
                'success': True,
                'source': relative_path,
                'target': f"manually_generated/{target_filename}",
                'message': f'Deployed {source_path.name} to manually_generated/{target_filename}',
                'files_created': [target_filename]
            }

            if backup_path:
                result['backup'] = backup_path.name

            logger.info(f"Deployed {relative_path} to manually_generated/{target_filename}")
            return result

        except Exception as e:
            return {
                'success': False,
                'error': _safe_derivation_error(
                    e,
                    f'deploying derivation file {relative_path}',
                    'Unable to deploy derivation file.',
                )
            }

    def _deploy_split_files(self, source_path: Path, relative_path: str) -> Dict:
        """
        Split source file into individual derivation files and deploy each.

        Creates one file per derivation: CLASS_FIELD_derived.py
        """
        import ast

        try:
            with open(source_path, 'r', encoding='utf-8') as f:
                source_code = f.read()

            tree = ast.parse(source_code)
            files_created = []
            errors = []

            # Find all classes with lineage-decorated methods
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    class_name = node.name
                    # Remove DerivationMixin suffix if present
                    if class_name.endswith('DerivationMixin'):
                        class_name = class_name[:-len('DerivationMixin')]

                    # Find all lineage-decorated methods in this class
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef):
                            has_lineage = False
                            for decorator in item.decorator_list:
                                if isinstance(decorator, ast.Call):
                                    if isinstance(decorator.func, ast.Name) and decorator.func.id == 'lineage':
                                        has_lineage = True
                                        break
                                elif isinstance(decorator, ast.Name):
                                    if decorator.id == 'lineage':
                                        has_lineage = True
                                        break

                            if has_lineage:
                                field_name = item.name
                                target_filename = f"{class_name}_{field_name}_derived.py"

                                # Generate a standalone file for this derivation
                                try:
                                    file_content = self._generate_single_derivation_file(
                                        class_name, item, source_code
                                    )
                                    target_path = self.manual_dir / target_filename

                                    with open(target_path, 'w', encoding='utf-8') as f:
                                        f.write(file_content)

                                    files_created.append(target_filename)
                                    logger.info(f"Created {target_filename}")

                                except Exception as e:
                                    SecureErrorHandler.handle_exception(
                                        e,
                                        f'generating split derivation file {target_filename}',
                                    )
                                    errors.append(f"{target_filename}: generation failed")

            if files_created:
                result = {
                    'success': True,
                    'source': relative_path,
                    'target': f"manually_generated/ ({len(files_created)} files)",
                    'files_created': files_created,
                    'message': f'Split into {len(files_created)} derivation file(s)'
                }
                if errors:
                    result['errors'] = errors
                return result
            else:
                return {
                    'success': False,
                    'error': 'No derivations could be extracted',
                    'errors': errors
                }

        except Exception as e:
            return {
                'success': False,
                'error': _safe_derivation_error(
                    e,
                    f'splitting derivation file {relative_path}',
                    'Unable to split derivation file.',
                )
            }

    def _generate_single_derivation_file(self, class_name: str, method_node, original_source: str) -> str:
        """
        Generate a standalone Python file for a single derivation method.

        Args:
            class_name: The class name (without DerivationMixin suffix)
            method_node: AST node for the method
            original_source: Original source code (used to extract exact method text)

        Returns:
            Complete Python file content as string
        """
        import ast

        # Extract the method source using line numbers
        lines = original_source.split('\n')

        # Find the start of decorators (they come before the method)
        start_line = method_node.lineno - 1  # 0-indexed

        # Look backwards for decorators
        decorator_start = start_line
        for decorator in method_node.decorator_list:
            if hasattr(decorator, 'lineno'):
                decorator_start = min(decorator_start, decorator.lineno - 1)

        # Find the end of the method
        end_line = method_node.end_lineno if hasattr(method_node, 'end_lineno') else start_line + 20

        # Extract method lines
        method_lines = lines[decorator_start:end_line]
        method_source = '\n'.join(method_lines)

        # Calculate the indentation of the method (should be 4 spaces for class body)
        # We need to add proper indentation for inside a class
        indented_method = '\n'.join('    ' + line if line.strip() else line for line in method_lines)

        # Generate the file content
        file_content = f'''# coding=UTF-8
# This file was deployed from staging to production (manually_generated/)
# It contains a single derivation rule that can be edited independently.
#
# Original source: staging derivation file
# Derivation: {class_name}.{method_node.name}

from django.db import models
from pybirdai.annotations.decorators import lineage


class {class_name}(models.Model):
    """Derivation for {method_node.name}."""

{indented_method}

    class Meta:
        pass
'''
        return file_content

    def revert_to_generated(self, relative_path: str) -> Dict:
        """
        Revert a modified staging file to its original generated state.

        Args:
            relative_path: Path relative to derivation_base

        Returns:
            Dict with revert result
        """
        checksums = self._load_checksums()

        if relative_path not in checksums:
            return {
                'success': False,
                'error': 'No original checksum found - cannot revert'
            }

        # We can't actually revert without storing the original content
        # For now, this would require re-running the generator
        return {
            'success': False,
            'error': 'Revert requires re-running the generator. Use the regenerate function instead.'
        }

    def get_file_info(self, relative_path: str) -> Optional[Dict]:
        """
        Get detailed info about a specific file.

        Args:
            relative_path: Path relative to derivation_base

        Returns:
            Dict with file info or None if not found
        """
        file_path = self.derivation_base / relative_path

        if not file_path.exists():
            return None

        # Determine type based on path
        if 'manually_generated' in relative_path:
            file_type = self.TYPE_MANUAL
            status = self.STATUS_PRODUCTION
        elif 'generated_from_member_links' in relative_path:
            file_type = self.TYPE_CUBE_LINK
            is_modified = self.is_modified(relative_path)
            status = self.STATUS_STAGING_MODIFIED if is_modified else self.STATUS_STAGING_UNMODIFIED
        else:
            file_type = self.TYPE_AUTO
            is_modified = self.is_modified(relative_path)
            status = self.STATUS_STAGING_MODIFIED if is_modified else self.STATUS_STAGING_UNMODIFIED

        derivations = self._extract_derivations_from_file(file_path)

        return {
            'filename': file_path.name,
            'relative_path': relative_path,
            'full_path': str(file_path),
            'type': file_type,
            'status': status,
            'is_modified': self.is_modified(relative_path) if file_type != self.TYPE_MANUAL else False,
            'mtime': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(),
            'size': file_path.stat().st_size,
            'derivations': derivations,
        }


# Convenience functions for direct use
def get_sync_status() -> Dict:
    """Get sync status summary for all derivation files"""
    manager = DerivationSyncManager()
    return manager.get_sync_status_summary()


def get_all_files() -> List[Dict]:
    """Get all derivation files with status"""
    manager = DerivationSyncManager()
    return manager.get_all_derivation_files()


def deploy_file(relative_path: str, new_filename: Optional[str] = None) -> Dict:
    """Deploy a file to manually_generated/"""
    manager = DerivationSyncManager()
    return manager.deploy_to_manual(relative_path, new_filename)
