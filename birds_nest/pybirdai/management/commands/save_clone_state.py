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
Management command to save clone mode state to a private GitHub repository.

This command exports the current database state along with process_metadata.json
to enable restoring the exact workflow state on another environment.

Usage:
    # Using config file (recommended)
    python manage.py save_clone_state --commit-message "DPM Step 2 completed"

    # Using command-line arguments
    python manage.py save_clone_state \
        --repo-url https://github.com/your-org/efbt-clone-state \
        --token ghp_YOUR_TOKEN \
        --branch main \
        --commit-message "DPM Step 2 completed"

    # Local export only (no GitHub push)
    python manage.py save_clone_state --local-only

Config file (clone_mode_config.json):
    {
      "github": {
        "token": "ghp_YOUR_TOKEN",
        "repo_url": "https://github.com/your-org/efbt-clone-state",
        "branch": "main"
      }
    }
"""
import os
import shutil
import tempfile
import zipfile
import requests
import base64
import json
import logging

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

logger = logging.getLogger(__name__)

# Configuration schema for clone mode config file
CONFIG_SCHEMA = {
    'type': 'object',
    'properties': {
        'github': {
            'type': 'object',
            'properties': {
                'token': {
                    'type': 'string',
                    'minLength': 20,
                    'description': 'GitHub personal access token'
                },
                'repo_url': {
                    'type': 'string',
                    'pattern': r'^https?://github\.com/[^/]+/[^/]+/?$',
                    'description': 'GitHub repository URL'
                },
                'branch': {
                    'type': 'string',
                    'minLength': 1,
                    'maxLength': 255,
                    'default': 'main',
                    'description': 'Git branch name'
                }
            },
            'additionalProperties': False
        }
    },
    'additionalProperties': True  # Allow other keys for forward compatibility
}


class Command(BaseCommand):
    help = 'Save clone mode state (database + metadata) to a private GitHub repository'

    def add_arguments(self, parser):
        parser.add_argument(
            '--config',
            default='clone_mode_config.json',
            help='Path to config file (default: clone_mode_config.json)'
        )
        parser.add_argument(
            '--repo-url',
            default=None,
            help='GitHub repository URL (overrides config file)'
        )
        parser.add_argument(
            '--token',
            default=None,
            help='GitHub personal access token (overrides config file)'
        )
        parser.add_argument(
            '--branch',
            default=None,
            help='Branch to push to (default: main, overrides config file)'
        )
        parser.add_argument(
            '--commit-message',
            default='Update clone state',
            help='Commit message for the push'
        )
        parser.add_argument(
            '--output-dir',
            default=None,
            help='Local output directory (default: results/clone_export)'
        )
        parser.add_argument(
            '--local-only',
            action='store_true',
            help='Only export locally, do not push to GitHub'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force export even after code generation (WARNING: generated code may not match)'
        )

    def _validate_config(self, config):
        """
        Validate configuration against the schema.

        Args:
            config: Configuration dictionary to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        github_config = config.get('github', {})

        # Validate token format if present
        token = github_config.get('token')
        if token:
            if len(token) < 20:
                return False, 'GitHub token is too short (minimum 20 characters)'
            # Check for known prefixes
            valid_prefixes = ('ghp_', 'github_pat_', 'ghu_', 'ghs_', 'gho_', 'ghr_')
            if not any(token.startswith(p) for p in valid_prefixes):
                # Allow legacy tokens that are long enough
                if len(token) < 40:
                    return False, 'Token format not recognized. Expected ghp_, github_pat_, or legacy 40+ char token'

        # Validate repo_url format if present
        repo_url = github_config.get('repo_url')
        if repo_url:
            import re
            pattern = r'^https?://github\.com/[a-zA-Z0-9_-]+/[a-zA-Z0-9_.-]+/?$'
            if not re.match(pattern, repo_url):
                return False, f'Invalid GitHub repository URL format: {repo_url}'

        # Validate branch name if present
        branch = github_config.get('branch')
        if branch:
            if len(branch) > 255:
                return False, 'Branch name exceeds maximum length (255 characters)'
            # Check for invalid characters
            invalid_chars = ['..', ' ', '~', '^', ':', '?', '*', '[', '\\']
            for char in invalid_chars:
                if char in branch:
                    return False, f'Branch name contains invalid character: {char}'

        return True, None

    def _validate_repo_restrictions(self, repo_url, token):
        """
        Validate that the repository follows clone mode restrictions.

        For web UI, saving is restricted to user's pybirdai_workplace repo.
        For CLI, we warn but don't block non-standard repos.
        """
        from pybirdai.services.github_service import GitHubService, CLONE_MODE_REPO_NAME

        owner, repo = GitHubService.parse_url(repo_url)
        if not owner or not repo:
            return  # URL validation happens elsewhere

        # Warn if repo name is not the expected 'pybirdai_workplace'
        if repo != CLONE_MODE_REPO_NAME:
            self.stdout.write(self.style.WARNING(
                f'  NOTE: Repository name "{repo}" differs from recommended "{CLONE_MODE_REPO_NAME}".\n'
                f'  The web UI only allows saving to {CLONE_MODE_REPO_NAME} repositories.\n'
                f'  CLI usage allows custom repository names for advanced users.'
            ))

        # Verify user has access to this repo
        if token:
            try:
                github_service = GitHubService(token=token)
                validation = github_service.validate_save_target(owner)

                if not validation['valid']:
                    self.stdout.write(self.style.WARNING(
                        f'  WARNING: {validation["error"]}\n'
                        f'  Proceeding anyway (CLI mode allows this).'
                    ))
                else:
                    self.stdout.write(
                        f'  Verified access to {owner} ({validation["owner_type"]})'
                    )
            except Exception as e:
                self.stdout.write(self.style.WARNING(
                    f'  Could not verify repository access: {e}'
                ))

    def _load_config(self, config_path):
        """Load and validate configuration from JSON file."""
        if not os.path.exists(config_path):
            return {}

        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            self.stdout.write(f'Loaded config from: {config_path}')

            # Validate configuration
            is_valid, error_msg = self._validate_config(config)
            if not is_valid:
                raise CommandError(f'Configuration validation failed: {error_msg}')

            return config
        except json.JSONDecodeError as e:
            raise CommandError(f'Invalid JSON in config file {config_path}: {e}')
        except CommandError:
            raise  # Re-raise CommandError as-is
        except Exception as e:
            raise CommandError(f'Error loading config file {config_path}: {e}')

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting clone state export...'))

        # Load config file
        config = self._load_config(options['config'])
        github_config = config.get('github', {})

        # Get values from command line or config file (command line takes precedence)
        repo_url = options['repo_url'] or github_config.get('repo_url')
        token = options['token'] or github_config.get('token')
        branch = options['branch'] or github_config.get('branch', 'main')
        commit_message = options['commit_message']
        local_only = options['local_only']

        # Validate required fields when not local-only
        if not local_only:
            if not repo_url:
                raise CommandError(
                    'GitHub repository URL is required.\n'
                    'Provide via --repo-url or in clone_mode_config.json'
                )
            if not token:
                raise CommandError(
                    'GitHub token is required.\n'
                    'Provide via --token or in clone_mode_config.json'
                )

            # Warn if repo name is not the expected 'pybirdai_workplace'
            self._validate_repo_restrictions(repo_url, token)

        # Set up output directory
        if options['output_dir']:
            output_dir = options['output_dir']
        else:
            output_dir = os.path.join(settings.BASE_DIR, 'results', 'clone_export')

        os.makedirs(output_dir, exist_ok=True)

        force = options['force']

        try:
            # Step 1: Generate and validate process metadata
            self.stdout.write('Step 1: Generating process metadata...')
            metadata = self._generate_and_validate_metadata(force=force)

            # Step 2: Export database to CSV with IDs
            self.stdout.write('Step 2: Exporting database to CSV...')
            csv_dir = self._export_database(output_dir)

            # Step 3: Save process_metadata.json
            self.stdout.write('Step 3: Saving process_metadata.json...')
            self._save_metadata(csv_dir, metadata)

            # Step 4: Create ZIP package
            self.stdout.write('Step 4: Creating export package...')
            zip_path = self._create_zip_package(csv_dir, output_dir)

            if local_only:
                self.stdout.write(self.style.SUCCESS(
                    f'\nLocal export complete!\n'
                    f'Export directory: {csv_dir}\n'
                    f'ZIP package: {zip_path}'
                ))
                return

            # Step 5: Push to GitHub
            self.stdout.write('Step 5: Pushing to GitHub...')
            self._push_to_github(csv_dir, repo_url, token, branch, commit_message)

            self.stdout.write(self.style.SUCCESS(
                f'\nClone state saved successfully!\n'
                f'Repository: {repo_url}\n'
                f'Branch: {branch}\n'
                f'Local export: {csv_dir}'
            ))

        except Exception as e:
            logger.error(f"Clone state export failed: {e}")
            raise CommandError(f'Clone state export failed: {str(e)}')

    def _generate_and_validate_metadata(self, force=False):
        """Generate process metadata and validate it's suitable for clone mode."""
        from pybirdai.utils.clone_mode.process_metadata import (
            generate_process_metadata,
            validate_metadata_parsing_only
        )

        metadata = generate_process_metadata()

        # Validate that we're not past code generation (unless force is set)
        if not validate_metadata_parsing_only(metadata):
            if force:
                self.stdout.write(self.style.WARNING(
                    '  WARNING: Code generation steps have been completed.\n'
                    '  Exporting anyway due to --force flag.\n'
                    '  Note: Generated code may not match the imported database state.'
                ))
            else:
                raise CommandError(
                    'Cannot save clone state: code generation steps have been completed.\n'
                    'Clone mode only supports saving state before code generation.\n'
                    'This is to ensure generated code matches the cloned database state.\n'
                    'Use --force to export anyway (not recommended).'
                )

        # Display export status (v1.2+)
        export_status = metadata.get('export_status', {})
        if export_status:
            is_complete = export_status.get('is_complete', False)
            if is_complete:
                self.stdout.write(self.style.SUCCESS('  Export Status: COMPLETE'))
                completed_workflows = export_status.get('completed_workflows', [])
                if completed_workflows:
                    self.stdout.write(f'    Completed workflows: {", ".join(completed_workflows)}')
            else:
                self.stdout.write(self.style.WARNING('  Export Status: INCOMPLETE'))
                incomplete_workflows = export_status.get('incomplete_workflows', [])
                if incomplete_workflows:
                    self.stdout.write(f'    Workflows in progress: {", ".join(incomplete_workflows)}')
                self.stdout.write('    Note: Tests have not been run on this export.')

        # Show workflow progress
        workflows = metadata.get('workflows', {})
        for workflow_name, workflow_data in workflows.items():
            if isinstance(workflow_data, dict):
                total_steps = workflow_data.get('total_steps', 0)
                last_step_num = workflow_data.get('last_step_completed', 0)
                is_workflow_complete = workflow_data.get('is_complete', False)
                source_type = workflow_data.get('source_type', '')

                if last_step_num > 0 or is_workflow_complete:
                    source_str = f' ({source_type})' if source_type else ''
                    if is_workflow_complete:
                        self.stdout.write(f'    {workflow_name.upper()}{source_str}: COMPLETE ({total_steps}/{total_steps})')
                    else:
                        self.stdout.write(f'    {workflow_name.upper()}{source_str}: Step {last_step_num}/{total_steps}')

        last_step = metadata.get('last_step_completed', 'Unknown')
        self.stdout.write(f'  Last completed step: {last_step}')

        return metadata

    def _export_database(self, output_dir):
        """Export database to CSV files with ID preservation."""
        from pybirdai.utils.clone_mode.export_with_ids import export_database_to_csv_with_ids

        # Create subdirectory for CSV files
        csv_dir = os.path.join(output_dir, 'database_export')
        os.makedirs(csv_dir, exist_ok=True)

        # Export to a temporary ZIP, then extract
        zip_path = os.path.join(output_dir, 'temp_export.zip')
        export_database_to_csv_with_ids(zip_path)

        # Extract the ZIP to the CSV directory
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(csv_dir)

        # Clean up temporary ZIP
        os.remove(zip_path)

        # Count exported files
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
        self.stdout.write(f'  Exported {len(csv_files)} CSV files')

        return csv_dir

    def _save_metadata(self, csv_dir, metadata):
        """Save process_metadata.json to the export directory."""
        from pybirdai.utils.clone_mode.process_metadata import save_process_metadata

        save_process_metadata(csv_dir, metadata)
        self.stdout.write('  Saved process_metadata.json')

    def _create_zip_package(self, csv_dir, output_dir):
        """Create a ZIP package of all export files."""
        zip_path = os.path.join(output_dir, 'clone_state_export.zip')

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(csv_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, csv_dir)
                    zipf.write(file_path, arcname)

        size_mb = os.path.getsize(zip_path) / (1024 * 1024)
        self.stdout.write(f'  Created ZIP package: {size_mb:.2f} MB')

        return zip_path

    def _push_to_github(self, csv_dir, repo_url, token, branch, commit_message):
        """Push the export files to a GitHub repository using unified GitHubService."""
        from pybirdai.services.github_service import GitHubService

        # Parse repo URL using the service
        owner, repo = GitHubService.parse_url(repo_url)
        if not owner or not repo:
            raise CommandError(f'Invalid repository URL: {repo_url}')

        # Create service instance
        github_service = GitHubService(token=token)

        self.stdout.write(f'  Pushing to {owner}/{repo} on branch {branch}...')

        # Collect files to push
        files_to_push = []
        for root, dirs, files in os.walk(csv_dir):
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, csv_dir)

                with open(file_path, 'rb') as f:
                    content = f.read()

                files_to_push.append({
                    'path': rel_path,
                    'content': content
                })

        self.stdout.write(f'  Found {len(files_to_push)} files to push')

        # Check if branch exists, if not create from default
        branch_result = github_service.get_branch(owner, repo, branch)
        if not branch_result['success']:
            self.stdout.write(f'  Branch {branch} not found, creating from default...')
            # Get default branch
            repo_info = github_service.repo_exists(owner, repo)
            if not repo_info.get('exists'):
                raise CommandError(f'Repository not found: {repo_url}')
            default_branch = repo_info.get('default_branch', 'main')

            create_result = github_service.create_branch(owner, repo, branch, default_branch)
            if not create_result['success']:
                raise CommandError(f'Failed to create branch: {create_result["error"]}')

        # Push files
        result = github_service.push_files(
            owner=owner,
            repo=repo,
            files=files_to_push,
            branch=branch,
            message=commit_message
        )

        if not result['success']:
            raise CommandError(f'Failed to push files: {result["error"]}')

        commit_sha = result.get('commit_sha', 'N/A')
        self.stdout.write(self.style.SUCCESS(f'  Pushed commit {commit_sha[:8]} to {branch}'))
