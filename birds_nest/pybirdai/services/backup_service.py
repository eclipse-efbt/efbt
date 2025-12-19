# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""
Backup Service for PyBIRD AI.

This module provides local backup functionality for pipeline data:
- Creates timestamped backups of database_export and joins_configuration
- Supports backup restoration
- Manages backup lifecycle (list, delete old backups)
"""

import os
import shutil
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

from django.conf import settings

from .pipeline_repo_service import PipelineRepoService, PIPELINES

logger = logging.getLogger(__name__)


class BackupService:
    """
    Service for creating and managing local backups of pipeline data.

    This service provides:
    - Automatic backups before potentially destructive operations
    - Manual backup creation
    - Backup restoration
    - Backup listing and cleanup

    Usage:
        service = BackupService()

        # Create a backup
        backup_id = service.create_backup('dpm', step=2)

        # List backups
        backups = service.list_backups('dpm')

        # Restore a backup
        service.restore_backup('dpm', backup_id)
    """

    def __init__(self):
        """Initialize the backup service."""
        self._base_dir = getattr(settings, 'BASE_DIR', os.getcwd())
        self._resources_dir = os.path.join(self._base_dir, 'resources')
        self._backups_dir = os.path.join(self._resources_dir, 'backups')
        self._pipeline_service = PipelineRepoService()

    def _ensure_backup_dir(self, pipeline: str) -> str:
        """Ensure backup directory exists for a pipeline."""
        backup_dir = os.path.join(self._backups_dir, pipeline)
        os.makedirs(backup_dir, exist_ok=True)
        return backup_dir

    def _generate_backup_id(self, step: Optional[int] = None) -> str:
        """Generate a unique backup ID with timestamp."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if step is not None:
            return f"{timestamp}_step{step}"
        return timestamp

    # ========================================================================
    # Backup Creation
    # ========================================================================

    def create_backup(
        self,
        pipeline: str,
        step: Optional[int] = None,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a backup of pipeline data.

        Backs up:
        - database_export/{pipeline}/ directory
        - joins_configuration/{pipeline}/ directory

        Args:
            pipeline: Pipeline name ('main', 'ancrdt', or 'dpm')
            step: Optional workflow step number for identification
            description: Optional description of the backup

        Returns:
            Dict with:
                - success: True if backup created
                - backup_id: Unique identifier for the backup
                - path: Path to backup directory
                - error: Error message if failed
        """
        if not PipelineRepoService.is_valid_pipeline(pipeline):
            return {
                'success': False,
                'backup_id': None,
                'path': None,
                'error': f'Invalid pipeline: {pipeline}'
            }

        try:
            backup_id = self._generate_backup_id(step)
            backup_dir = self._ensure_backup_dir(pipeline)
            backup_path = os.path.join(backup_dir, backup_id)

            os.makedirs(backup_path, exist_ok=True)

            # Get source paths
            paths = self._pipeline_service.get_all_paths(pipeline)
            export_path = paths['export']
            joins_path = paths['joins']

            files_backed_up = 0

            # Backup database_export if exists
            if os.path.exists(export_path) and os.listdir(export_path):
                export_backup_path = os.path.join(backup_path, 'database_export')
                shutil.copytree(export_path, export_backup_path)
                files_backed_up += len(os.listdir(export_backup_path))
                logger.info(f"Backed up database_export for {pipeline}")

            # Backup joins_configuration if exists
            if os.path.exists(joins_path) and os.listdir(joins_path):
                joins_backup_path = os.path.join(backup_path, 'joins_configuration')
                shutil.copytree(joins_path, joins_backup_path)
                files_backed_up += len(os.listdir(joins_backup_path))
                logger.info(f"Backed up joins_configuration for {pipeline}")

            # Save metadata
            metadata = {
                'backup_id': backup_id,
                'pipeline': pipeline,
                'step': step,
                'description': description,
                'created_at': datetime.now().isoformat(),
                'files_count': files_backed_up
            }

            metadata_path = os.path.join(backup_path, 'backup_metadata.json')
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)

            logger.info(f"Created backup {backup_id} for {pipeline} ({files_backed_up} files)")

            return {
                'success': True,
                'backup_id': backup_id,
                'path': backup_path,
                'files_count': files_backed_up,
                'error': None
            }

        except Exception as e:
            logger.error(f"Failed to create backup for {pipeline}: {e}")
            return {
                'success': False,
                'backup_id': None,
                'path': None,
                'error': str(e)
            }

    # ========================================================================
    # Backup Listing
    # ========================================================================

    def list_backups(self, pipeline: str) -> List[Dict[str, Any]]:
        """
        List all backups for a pipeline.

        Args:
            pipeline: Pipeline name

        Returns:
            List of backup metadata dicts, sorted by date (newest first)
        """
        if not PipelineRepoService.is_valid_pipeline(pipeline):
            return []

        backup_dir = os.path.join(self._backups_dir, pipeline)

        if not os.path.exists(backup_dir):
            return []

        backups = []
        for item in os.listdir(backup_dir):
            item_path = os.path.join(backup_dir, item)
            if not os.path.isdir(item_path):
                continue

            # Try to load metadata
            metadata_path = os.path.join(item_path, 'backup_metadata.json')
            if os.path.exists(metadata_path):
                try:
                    with open(metadata_path, 'r') as f:
                        metadata = json.load(f)
                    backups.append(metadata)
                except Exception:
                    # Fallback if metadata is corrupt
                    backups.append({
                        'backup_id': item,
                        'pipeline': pipeline,
                        'created_at': None,
                        'path': item_path
                    })
            else:
                backups.append({
                    'backup_id': item,
                    'pipeline': pipeline,
                    'created_at': None,
                    'path': item_path
                })

        # Sort by created_at (newest first)
        backups.sort(key=lambda x: x.get('created_at') or '', reverse=True)
        return backups

    def get_backup(self, pipeline: str, backup_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a specific backup.

        Args:
            pipeline: Pipeline name
            backup_id: Backup identifier

        Returns:
            Backup metadata dict or None if not found
        """
        backups = self.list_backups(pipeline)
        for backup in backups:
            if backup.get('backup_id') == backup_id:
                return backup
        return None

    # ========================================================================
    # Backup Restoration
    # ========================================================================

    def restore_backup(
        self,
        pipeline: str,
        backup_id: str,
        create_current_backup: bool = True
    ) -> Dict[str, Any]:
        """
        Restore a backup for a pipeline.

        Args:
            pipeline: Pipeline name
            backup_id: Backup identifier to restore
            create_current_backup: Create backup of current state before restoring

        Returns:
            Dict with:
                - success: True if restored
                - error: Error message if failed
        """
        if not PipelineRepoService.is_valid_pipeline(pipeline):
            return {
                'success': False,
                'error': f'Invalid pipeline: {pipeline}'
            }

        backup_dir = os.path.join(self._backups_dir, pipeline, backup_id)

        if not os.path.exists(backup_dir):
            return {
                'success': False,
                'error': f'Backup not found: {backup_id}'
            }

        try:
            # Optionally backup current state first
            if create_current_backup:
                self.create_backup(pipeline, description=f'Auto-backup before restore of {backup_id}')

            # Get target paths
            paths = self._pipeline_service.get_all_paths(pipeline)

            # Restore database_export
            export_backup = os.path.join(backup_dir, 'database_export')
            if os.path.exists(export_backup):
                target_path = paths['export']
                if os.path.exists(target_path):
                    shutil.rmtree(target_path)
                shutil.copytree(export_backup, target_path)
                logger.info(f"Restored database_export for {pipeline}")

            # Restore joins_configuration
            joins_backup = os.path.join(backup_dir, 'joins_configuration')
            if os.path.exists(joins_backup):
                target_path = paths['joins']
                if os.path.exists(target_path):
                    shutil.rmtree(target_path)
                shutil.copytree(joins_backup, target_path)
                logger.info(f"Restored joins_configuration for {pipeline}")

            logger.info(f"Restored backup {backup_id} for {pipeline}")

            return {
                'success': True,
                'error': None
            }

        except Exception as e:
            logger.error(f"Failed to restore backup {backup_id} for {pipeline}: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    # ========================================================================
    # Backup Cleanup
    # ========================================================================

    def delete_backup(self, pipeline: str, backup_id: str) -> Dict[str, Any]:
        """
        Delete a specific backup.

        Args:
            pipeline: Pipeline name
            backup_id: Backup identifier to delete

        Returns:
            Dict with success status and error if any
        """
        backup_dir = os.path.join(self._backups_dir, pipeline, backup_id)

        if not os.path.exists(backup_dir):
            return {
                'success': False,
                'error': f'Backup not found: {backup_id}'
            }

        try:
            shutil.rmtree(backup_dir)
            logger.info(f"Deleted backup {backup_id} for {pipeline}")
            return {'success': True, 'error': None}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def cleanup_old_backups(
        self,
        pipeline: str,
        keep_count: int = 5
    ) -> Dict[str, Any]:
        """
        Clean up old backups, keeping only the most recent ones.

        Args:
            pipeline: Pipeline name
            keep_count: Number of recent backups to keep

        Returns:
            Dict with count of deleted backups
        """
        backups = self.list_backups(pipeline)

        if len(backups) <= keep_count:
            return {'deleted_count': 0, 'kept_count': len(backups)}

        # Delete oldest backups
        to_delete = backups[keep_count:]
        deleted_count = 0

        for backup in to_delete:
            result = self.delete_backup(pipeline, backup['backup_id'])
            if result['success']:
                deleted_count += 1

        logger.info(f"Cleaned up {deleted_count} old backups for {pipeline}")
        return {
            'deleted_count': deleted_count,
            'kept_count': keep_count
        }


# ============================================================================
# Convenience Functions
# ============================================================================

def get_backup_service() -> BackupService:
    """Get a BackupService instance."""
    return BackupService()


def create_pipeline_backup(pipeline: str, step: Optional[int] = None) -> str:
    """
    Convenience function to create a backup.

    Args:
        pipeline: Pipeline name
        step: Optional workflow step number

    Returns:
        Backup ID or empty string if failed
    """
    service = BackupService()
    result = service.create_backup(pipeline, step)
    return result.get('backup_id', '')
