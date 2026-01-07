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

            # Step 4: Export filter code files
            self.stdout.write('Step 4: Exporting filter code files...')
            self._export_filter_code(output_dir)

            # Step 5: Export join configuration files
            self.stdout.write('Step 5: Exporting join configuration files...')
            self._export_join_configuration(output_dir)

            # Step 6: Create ZIP package
            self.stdout.write('Step 6: Creating export package...')
            zip_path = self._create_zip_package(csv_dir, output_dir)

            if local_only:
                self.stdout.write(self.style.SUCCESS(
                    f'\nLocal export complete!\n'
                    f'Export directory: {csv_dir}\n'
                    f'ZIP package: {zip_path}'
                ))
                return

            # Step 7: Push to GitHub
            self.stdout.write('Step 7: Pushing to GitHub...')
            self._push_to_github(output_dir, repo_url, token, branch, commit_message)

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
        """Export database to CSV files with ID preservation.

        Uses the same structure as GitHub import: export/database_export_ldm/
        This allows clone mode exports to be directly compatible with the
        GitHub import flow used when starting framework workflows.
        """
        from pybirdai.utils.clone_mode.export_with_ids import export_database_to_csv_with_ids

        # Create subdirectory for CSV files using the same structure as GitHub import
        # This matches the path expected by ConfigurableGitHubFileFetcher.fetch_technical_exports()
        csv_dir = os.path.join(output_dir, 'export', 'database_export_ldm')
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
        self.stdout.write(f'  Exported {len(csv_files)} CSV files to export/database_export_ldm/')

        return csv_dir

    def _save_metadata(self, csv_dir, metadata):
        """Save process_metadata.json to the export directory."""
        from pybirdai.utils.clone_mode.process_metadata import save_process_metadata

        save_process_metadata(csv_dir, metadata)
        self.stdout.write('  Saved process_metadata.json')

    def _export_filter_code(self, output_dir):
        """Export all filter code files to the export directory.

        Copies the entire filter_code folder structure:
          pybirdai/process_steps/filter_code/
            datasets/       - Dataset logic (ANCRDT, etc.)
            templates/      - Template logic (FINREP, COREP, etc.)
            lib/            - Shared utilities

        This exports everything regardless of framework.
        """
        filter_code_source = os.path.join(
            settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code'
        )
        filter_code_dest = os.path.join(output_dir, 'export', 'filter_code')

        if not os.path.exists(filter_code_source):
            self.stdout.write('  No filter_code directory found, skipping')
            return

        # Remove existing export if present
        if os.path.exists(filter_code_dest):
            shutil.rmtree(filter_code_dest)

        # Copy entire directory tree
        shutil.copytree(
            filter_code_source,
            filter_code_dest,
            ignore=shutil.ignore_patterns('__pycache__', '*.pyc', '.DS_Store')
        )

        # Count files exported
        file_count = 0
        for root, dirs, files in os.walk(filter_code_dest):
            file_count += len([f for f in files if f.endswith('.py')])

        self.stdout.write(f'  Exported {file_count} filter code files to export/filter_code/')

    def _export_join_configuration(self, output_dir):
        """Export all join configuration files to the export directory.

        Copies the entire joins_configuration folder:
          resources/joins_configuration/
            in_scope_reports_*.csv
            join_for_product_il_definitions_*.csv
            join_for_product_to_reference_category_*.csv

        This exports everything regardless of framework.
        """
        joins_config_source = os.path.join(
            settings.BASE_DIR, 'resources', 'joins_configuration'
        )
        joins_config_dest = os.path.join(output_dir, 'export', 'joins_configuration')

        if not os.path.exists(joins_config_source):
            self.stdout.write('  No joins_configuration directory found, skipping')
            return

        # Remove existing export if present
        if os.path.exists(joins_config_dest):
            shutil.rmtree(joins_config_dest)

        # Copy entire directory (only CSV files)
        os.makedirs(joins_config_dest, exist_ok=True)
        file_count = 0
        for filename in os.listdir(joins_config_source):
            if filename.endswith('.csv'):
                src_path = os.path.join(joins_config_source, filename)
                dst_path = os.path.join(joins_config_dest, filename)
                shutil.copy2(src_path, dst_path)
                file_count += 1

        self.stdout.write(f'  Exported {file_count} join configuration files to export/joins_configuration/')

    def _create_zip_package(self, csv_dir, output_dir):
        """Create a ZIP package of all export files.

        Preserves the export/database_export_ldm/ directory structure in the ZIP.
        """
        zip_path = os.path.join(output_dir, 'clone_state_export.zip')

        # Walk from output_dir to preserve the export/ directory structure
        export_base = os.path.join(output_dir, 'export')
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(export_base):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Archive name preserves export/database_export_ldm/ structure
                    arcname = os.path.relpath(file_path, output_dir)
                    zipf.write(file_path, arcname)

        size_mb = os.path.getsize(zip_path) / (1024 * 1024)
        self.stdout.write(f'  Created ZIP package: {size_mb:.2f} MB')

        return zip_path

    def _push_to_github(self, output_dir, repo_url, token, branch, commit_message):
        """Push the export files to a GitHub repository using unified GitHubService.

        The export structure matches GitHub import format:
        export/database_export_ldm/*.csv - Database CSV files
        export/database_export_ldm/process_metadata.json - Workflow metadata
        """
        from pybirdai.services.github_service import GitHubService

        # Parse repo URL using the service
        owner, repo = GitHubService.parse_url(repo_url)
        if not owner or not repo:
            raise CommandError(f'Invalid repository URL: {repo_url}')

        # Create service instance
        github_service = GitHubService(token=token)

        self.stdout.write(f'  Pushing to {owner}/{repo} on branch {branch}...')

        # Collect files to push - walk from output_dir to preserve export/ path structure
        files_to_push = []
        export_base = os.path.join(output_dir, 'export')
        for root, dirs, files in os.walk(export_base):
            for file in files:
                file_path = os.path.join(root, file)
                # Calculate relative path from output_dir to preserve export/database_export_ldm/ structure
                rel_path = os.path.relpath(file_path, output_dir)

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
