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
    python manage.py save_clone_state \
        --repo-url https://github.com/your-org/efbt-clone-state \
        --token ghp_YOUR_TOKEN \
        --branch main \
        --commit-message "DPM Step 2 completed"
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


class Command(BaseCommand):
    help = 'Save clone mode state (database + metadata) to a private GitHub repository'

    def add_arguments(self, parser):
        parser.add_argument(
            '--repo-url',
            required=True,
            help='GitHub repository URL (e.g., https://github.com/org/repo)'
        )
        parser.add_argument(
            '--token',
            required=True,
            help='GitHub personal access token with repo permissions'
        )
        parser.add_argument(
            '--branch',
            default='main',
            help='Branch to push to (default: main)'
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

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting clone state export...'))

        repo_url = options['repo_url']
        token = options['token']
        branch = options['branch']
        commit_message = options['commit_message']
        local_only = options['local_only']

        # Set up output directory
        if options['output_dir']:
            output_dir = options['output_dir']
        else:
            output_dir = os.path.join(settings.BASE_DIR, 'results', 'clone_export')

        os.makedirs(output_dir, exist_ok=True)

        try:
            # Step 1: Generate and validate process metadata
            self.stdout.write('Step 1: Generating process metadata...')
            metadata = self._generate_and_validate_metadata()

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

    def _generate_and_validate_metadata(self):
        """Generate process metadata and validate it's suitable for clone mode."""
        from pybirdai.utils.clone_mode.process_metadata import (
            generate_process_metadata,
            validate_metadata_parsing_only
        )

        metadata = generate_process_metadata()

        # Validate that we're not past code generation
        if not validate_metadata_parsing_only(metadata):
            raise CommandError(
                'Cannot save clone state: code generation steps have been completed.\n'
                'Clone mode only supports saving state before code generation.\n'
                'This is to ensure generated code matches the cloned database state.'
            )

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
        """Push the export files to a GitHub repository."""
        # Parse repo URL to get owner and repo name
        # Expected format: https://github.com/owner/repo
        parts = repo_url.rstrip('/').split('/')
        if len(parts) < 2:
            raise CommandError(f'Invalid repository URL: {repo_url}')

        owner = parts[-2]
        repo = parts[-1]

        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/vnd.github.v3+json',
            'X-GitHub-Api-Version': '2022-11-28'
        }

        api_base = f'https://api.github.com/repos/{owner}/{repo}'

        # Get the current commit SHA for the branch
        self.stdout.write(f'  Getting branch {branch} info...')
        ref_response = requests.get(
            f'{api_base}/git/refs/heads/{branch}',
            headers=headers
        )

        if ref_response.status_code == 404:
            # Branch doesn't exist, we'll create it from the default branch
            self.stdout.write(f'  Branch {branch} not found, will create it')
            base_sha = self._get_default_branch_sha(api_base, headers)
        elif ref_response.status_code != 200:
            raise CommandError(f'Failed to get branch info: {ref_response.status_code} - {ref_response.text}')
        else:
            base_sha = ref_response.json()['object']['sha']

        # Create blobs for each file
        self.stdout.write('  Creating file blobs...')
        tree_items = []

        for root, dirs, files in os.walk(csv_dir):
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, csv_dir)

                with open(file_path, 'rb') as f:
                    content = f.read()

                # Create blob
                blob_response = requests.post(
                    f'{api_base}/git/blobs',
                    headers=headers,
                    json={
                        'content': base64.b64encode(content).decode('utf-8'),
                        'encoding': 'base64'
                    }
                )

                if blob_response.status_code != 201:
                    raise CommandError(f'Failed to create blob for {rel_path}: {blob_response.text}')

                blob_sha = blob_response.json()['sha']
                tree_items.append({
                    'path': rel_path,
                    'mode': '100644',
                    'type': 'blob',
                    'sha': blob_sha
                })

        self.stdout.write(f'  Created {len(tree_items)} blobs')

        # Create tree
        self.stdout.write('  Creating tree...')
        tree_response = requests.post(
            f'{api_base}/git/trees',
            headers=headers,
            json={'tree': tree_items}
        )

        if tree_response.status_code != 201:
            raise CommandError(f'Failed to create tree: {tree_response.text}')

        tree_sha = tree_response.json()['sha']

        # Create commit
        self.stdout.write('  Creating commit...')
        commit_response = requests.post(
            f'{api_base}/git/commits',
            headers=headers,
            json={
                'message': commit_message,
                'tree': tree_sha,
                'parents': [base_sha]
            }
        )

        if commit_response.status_code != 201:
            raise CommandError(f'Failed to create commit: {commit_response.text}')

        commit_sha = commit_response.json()['sha']

        # Update branch reference
        self.stdout.write(f'  Updating branch {branch}...')
        ref_update_response = requests.patch(
            f'{api_base}/git/refs/heads/{branch}',
            headers=headers,
            json={'sha': commit_sha, 'force': True}
        )

        if ref_update_response.status_code == 404:
            # Branch doesn't exist, create it
            ref_create_response = requests.post(
                f'{api_base}/git/refs',
                headers=headers,
                json={
                    'ref': f'refs/heads/{branch}',
                    'sha': commit_sha
                }
            )
            if ref_create_response.status_code != 201:
                raise CommandError(f'Failed to create branch: {ref_create_response.text}')
        elif ref_update_response.status_code != 200:
            raise CommandError(f'Failed to update branch: {ref_update_response.text}')

        self.stdout.write(self.style.SUCCESS(f'  Pushed commit {commit_sha[:8]} to {branch}'))

    def _get_default_branch_sha(self, api_base, headers):
        """Get the SHA of the default branch."""
        repo_response = requests.get(api_base, headers=headers)
        if repo_response.status_code != 200:
            raise CommandError(f'Failed to get repository info: {repo_response.text}')

        default_branch = repo_response.json().get('default_branch', 'main')
        ref_response = requests.get(
            f'{api_base}/git/refs/heads/{default_branch}',
            headers=headers
        )

        if ref_response.status_code != 200:
            raise CommandError(f'Failed to get default branch SHA: {ref_response.text}')

        return ref_response.json()['object']['sha']
