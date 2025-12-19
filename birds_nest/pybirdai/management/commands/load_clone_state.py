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
Management command to load clone mode state from a private GitHub repository.

This command downloads database state and process_metadata.json from a GitHub
repository and restores the workflow state.

Usage:
    python manage.py load_clone_state \
        --repo-url https://github.com/your-org/efbt-clone-state \
        --token ghp_YOUR_TOKEN \
        --branch main

    # Verify only (no import)
    python manage.py load_clone_state \
        --repo-url https://github.com/your-org/efbt-clone-state \
        --token ghp_YOUR_TOKEN \
        --verify-only
"""
import os
import shutil
import logging

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Load clone mode state (database + metadata) from a private GitHub repository'

    def _validate_load_restrictions(self, repo_url, token):
        """
        Validate that the repository follows clone mode restrictions.

        For web UI, loading is restricted to user's pybirdai_workplace repo
        or official regcommunity repos.
        For CLI, we warn but don't block non-standard repos.
        """
        from pybirdai.services.github_service import (
            GitHubService, CLONE_MODE_REPO_NAME, ALLOWED_LOAD_REPOS
        )

        owner, repo = GitHubService.parse_url(repo_url)
        if not owner or not repo:
            return  # URL validation happens elsewhere

        full_name = f'{owner}/{repo}'

        # Check if it's an allowed default repo
        if full_name in ALLOWED_LOAD_REPOS:
            self.stdout.write(f'  Loading from official repository: {full_name}')
            return

        # Check if it matches the expected pybirdai_workplace pattern
        if repo != CLONE_MODE_REPO_NAME:
            self.stdout.write(self.style.WARNING(
                f'  NOTE: Repository "{full_name}" is not in the allowed list.\n'
                f'  The web UI only allows loading from:\n'
                f'    - Your pybirdai_workplace repository\n'
                f'    - Official regcommunity repositories\n'
                f'  CLI usage allows custom repositories for advanced users.'
            ))
        else:
            # Verify user has access if token provided
            if token:
                try:
                    github_service = GitHubService(token=token)
                    validation = github_service.is_allowed_load_repo(repo_url)

                    if validation['allowed']:
                        self.stdout.write(
                            f'  Verified load access: {full_name} ({validation["source_type"]})'
                        )
                    else:
                        self.stdout.write(self.style.WARNING(
                            f'  WARNING: {validation["error"]}\n'
                            f'  Proceeding anyway (CLI mode allows this).'
                        ))
                except Exception as e:
                    self.stdout.write(self.style.WARNING(
                        f'  Could not verify repository access: {e}'
                    ))

    def add_arguments(self, parser):
        parser.add_argument(
            '--repo-url',
            default=None,
            help='GitHub repository URL (e.g., https://github.com/org/repo)'
        )
        parser.add_argument(
            '--token',
            default=None,
            help='GitHub personal access token (required for private repos)'
        )
        parser.add_argument(
            '--branch',
            default='main',
            help='Branch to load from (default: main)'
        )
        parser.add_argument(
            '--verify-only',
            action='store_true',
            help='Only verify metadata, do not import data'
        )
        parser.add_argument(
            '--local-path',
            default=None,
            help='Load from local directory instead of GitHub'
        )
        parser.add_argument(
            '--skip-cleanup',
            action='store_true',
            help='Do not delete existing database data before import'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force import even if metadata shows code generation was completed'
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting clone state load...'))

        repo_url = options['repo_url']
        token = options['token']
        branch = options['branch']
        verify_only = options['verify_only']
        local_path = options['local_path']
        skip_cleanup = options['skip_cleanup']
        force = options['force']

        # Validate: either --repo-url or --local-path must be provided
        if not repo_url and not local_path:
            raise CommandError(
                'Either --repo-url or --local-path must be provided.\n'
                'Use --repo-url to load from GitHub or --local-path to load from a local directory.'
            )

        # Validate repository restrictions (warning only for CLI)
        if repo_url:
            self._validate_load_restrictions(repo_url, token)

        try:
            # Step 1: Get the data (from GitHub or local path)
            if local_path:
                self.stdout.write(f'Step 1: Loading from local path: {local_path}')
                source_dir = local_path
            else:
                self.stdout.write('Step 1: Downloading from GitHub...')
                source_dir = self._download_from_github(repo_url, token, branch)

            # Step 2: Load and validate process_metadata.json
            self.stdout.write('Step 2: Loading and validating metadata...')
            metadata = self._load_and_validate_metadata(source_dir, force=force)

            # Step 3: Verify environment state
            self.stdout.write('Step 3: Verifying environment state...')
            verification_results = self._verify_environment(metadata)

            if verify_only:
                self._print_verification_results(verification_results, metadata)
                if not local_path:
                    shutil.rmtree(source_dir)
                return

            # Step 4: Clean up existing database (optional)
            if not skip_cleanup:
                self.stdout.write('Step 4: Cleaning up existing database...')
                self._cleanup_database()

            # Step 5: Import CSV files
            self.stdout.write('Step 5: Importing CSV files...')
            import_results = self._import_csv_files(source_dir)

            # Step 6: Restore workflow states
            self.stdout.write('Step 6: Restoring workflow states...')
            restore_results = self._restore_workflow_states(metadata)

            # Step 7: Clean up downloaded files
            if not local_path:
                self.stdout.write('Step 7: Cleaning up temporary files...')
                shutil.rmtree(source_dir)

            # Print results
            self._print_results(metadata, import_results, restore_results)

        except Exception as e:
            logger.error(f"Clone state load failed: {e}")
            raise CommandError(f'Clone state load failed: {str(e)}')

    def _download_from_github(self, repo_url, token, branch):
        """Download repository contents from GitHub using unified GitHubService."""
        from pybirdai.services.github_service import GitHubService

        # Parse repo URL using unified service
        owner, repo = GitHubService.parse_url(repo_url)
        if not owner or not repo:
            raise CommandError(f'Invalid repository URL: {repo_url}')

        self.stdout.write(f'  Downloading {owner}/{repo}@{branch}...')

        # Use GitHubService's download_archive method
        service = GitHubService(token=token)
        result = service.download_archive(owner, repo, branch)

        if not result['success']:
            raise CommandError(result['error'])

        self.stdout.write(f'  Downloaded and extracted to {result["path"]}')
        return result['path']

    def _load_and_validate_metadata(self, source_dir, force=False):
        """Load and validate process_metadata.json."""
        from pybirdai.utils.clone_mode.process_metadata import (
            load_process_metadata,
            validate_metadata_parsing_only
        )

        # Find process_metadata.json (might be in root or subdirectory)
        metadata_path = None
        for root, dirs, files in os.walk(source_dir):
            if 'process_metadata.json' in files:
                metadata_path = os.path.join(root, 'process_metadata.json')
                break

        if not metadata_path:
            raise CommandError(
                'process_metadata.json not found in the repository.\n'
                'This repository may not contain a valid clone state export.'
            )

        self.stdout.write(f'  Found metadata at: {metadata_path}')

        # Load and validate
        metadata = load_process_metadata(metadata_path)

        # Validate metadata parsing only
        if not validate_metadata_parsing_only(metadata):
            if force:
                self.stdout.write(self.style.WARNING(
                    '  WARNING: Code generation steps were completed in the export.\n'
                    '  Importing anyway due to --force flag.\n'
                    '  Note: Generated code may not match the imported database state.'
                ))
            else:
                raise CommandError(
                    'Invalid clone state: code generation steps were completed.\n'
                    'Clone mode only supports states before code generation.\n'
                    'Please use an earlier export or start fresh.\n'
                    'Use --force to import anyway (not recommended).'
                )

        self.stdout.write(f'  Metadata version: {metadata.get("version")}')
        self.stdout.write(f'  Last step: {metadata.get("last_step_completed")}')

        return metadata

    def _verify_environment(self, metadata):
        """Verify the current environment state."""
        from pybirdai.utils.clone_mode.process_metadata import verify_environment_state

        results = verify_environment_state(metadata)

        if results.get('errors'):
            for error in results['errors']:
                self.stdout.write(self.style.ERROR(f'  ERROR: {error}'))

        if results.get('warnings'):
            for warning in results['warnings']:
                self.stdout.write(self.style.WARNING(f'  WARNING: {warning}'))

        return results

    def _print_verification_results(self, verification_results, metadata):
        """Print verification results and exit."""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('VERIFICATION RESULTS')
        self.stdout.write('=' * 60)

        self.stdout.write(f'\nMetadata Version: {metadata.get("version")}')
        self.stdout.write(f'Last Completed Step: {metadata.get("last_step_completed")}')
        self.stdout.write(f'Created: {metadata.get("created_at")}')
        self.stdout.write(f'Updated: {metadata.get("updated_at")}')

        # User selections
        selections = metadata.get('user_selections', {})
        self.stdout.write(f'\nSelected Frameworks: {selections.get("selected_frameworks", [])}')
        self.stdout.write(f'Selected Tables: {len(selections.get("selected_tables", []))} tables')

        # Workflow status
        self.stdout.write('\nWorkflow Status:')
        workflows = metadata.get('workflows', {})
        for workflow_name, workflow_data in workflows.items():
            completed = [k for k, v in workflow_data.items() if v.get('status') == 'completed']
            self.stdout.write(f'  {workflow_name}: {len(completed)}/{len(workflow_data)} steps completed')

        # Verification status
        is_valid = verification_results.get('valid', True)
        if is_valid:
            self.stdout.write(self.style.SUCCESS('\nEnvironment verification: PASSED'))
        else:
            self.stdout.write(self.style.ERROR('\nEnvironment verification: FAILED'))

        self.stdout.write('=' * 60)

    def _cleanup_database(self):
        """Delete existing BIRD metadata from database."""
        from pybirdai.entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase

        app_config = RunDeleteBirdMetadataDatabase("pybirdai", "birds_nest")
        app_config.run_delete_bird_metadata_database()
        self.stdout.write('  Database cleaned')

    def _import_csv_files(self, source_dir):
        """Import CSV files into the database."""
        from pybirdai.utils.clone_mode.import_from_metadata_export import CSVDataImporter

        # Find the directory containing CSV files
        csv_dir = None
        for root, dirs, files in os.walk(source_dir):
            csv_files = [f for f in files if f.endswith('.csv')]
            if csv_files:
                csv_dir = root
                break

        if not csv_dir:
            raise CommandError('No CSV files found in the repository')

        self.stdout.write(f'  Importing from: {csv_dir}')

        # Import using the existing importer (ordered to respect foreign keys)
        # Note: use_fast_import=False for reliability over speed
        importer = CSVDataImporter()
        results = importer.import_from_path_ordered(csv_dir, use_fast_import=False)

        # Count results (results dict uses 'imported_count' key)
        total_records = sum(r.get('imported_count', 0) for r in results.values())
        self.stdout.write(f'  Imported {total_records} records from {len(results)} tables')

        return results

    def _restore_workflow_states(self, metadata):
        """Restore workflow execution states from metadata."""
        from pybirdai.utils.clone_mode.process_metadata import restore_workflow_states

        results = restore_workflow_states(metadata)

        restored_count = len(results.get('workflows_restored', []))
        self.stdout.write(f'  Restored {restored_count} workflow states')

        return results

    def _print_results(self, metadata, import_results, restore_results):
        """Print final results."""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write(self.style.SUCCESS('CLONE STATE LOADED SUCCESSFULLY'))
        self.stdout.write('=' * 60)

        last_step = metadata.get('last_step_completed', 'Unknown')
        self.stdout.write(f'\nRestored to step: {last_step}')

        # Determine restart point
        restart_url = self._get_restart_url(last_step)
        self.stdout.write(f'\nTo continue, navigate to:')
        self.stdout.write(self.style.SUCCESS(f'  {restart_url}'))

        # User selections reminder
        selections = metadata.get('user_selections', {})
        frameworks = selections.get('selected_frameworks', [])
        tables = selections.get('selected_tables', [])

        if frameworks:
            self.stdout.write(f'\nFrameworks: {", ".join(frameworks)}')
        if tables:
            self.stdout.write(f'Tables: {len(tables)} tables loaded')

        self.stdout.write('=' * 60)

    def _get_restart_url(self, last_step):
        """Get the URL to restart from based on the last completed step."""
        restart_urls = {
            'DPM_STEP1_EXTRACT_METADATA': '/pybirdai/workflow/dpm/step2/',
            'DPM_STEP2_PROCESS_TABLES': '/pybirdai/workflow/dpm/step3/',
            'MAIN_TASK1_DATABASE_SETUP': '/pybirdai/workflow/dashboard/',
            'MAIN_TASK2_DATA_IMPORT': '/pybirdai/workflow/dashboard/',
        }

        return restart_urls.get(last_step, '/pybirdai/workflow/dashboard/')
