# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

import logging
from django.core.management.base import BaseCommand
from django.conf import settings
import os
from pybirdai.utils.github_file_fetcher import GitHubFileFetcher

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Download REF_FINREP report template HTML files from GitHub repository'

    def add_arguments(self, parser):
        parser.add_argument(
            '--repository',
            type=str,
            default='https://github.com/regcommunity/FreeBIRD',
            help='GitHub repository URL'
        )
        parser.add_argument(
            '--remote-dir',
            type=str,
            default='birds_nest/results/generated_html',
            help='Remote directory path containing the HTML files'
        )
        parser.add_argument(
            '--local-dir',
            type=str,
            default=None,
            help='Local directory path to save the templates (default: pybirdai/templates/pybirdai)'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force download even if files already exist'
        )

    def handle(self, *args, **options):
        repository_url = options['repository']
        remote_dir = options['remote_dir']
        local_dir = options.get('local_dir')
        force = options.get('force', False)

        # Set default local directory if not provided
        if not local_dir:
            local_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'templates', 'pybirdai')

        self.stdout.write(self.style.SUCCESS(f'Downloading REF_FINREP report templates from {repository_url}'))
        self.stdout.write(f'Remote directory: {remote_dir}')
        self.stdout.write(f'Local directory: {local_dir}')

        try:
            # Initialize GitHub file fetcher
            fetcher = GitHubFileFetcher(repository_url)
            
            # Check if files already exist
            if not force and os.path.exists(local_dir):
                existing_files = [f for f in os.listdir(local_dir) if f.endswith('.html') and 'REF_FINREP' in f]
                if existing_files:
                    self.stdout.write(self.style.WARNING(f'Found {len(existing_files)} existing REF_FINREP templates. Use --force to overwrite.'))
                    return

            # Download report templates
            downloaded_count = fetcher.fetch_report_template_htmls(remote_dir, local_dir)
            
            if downloaded_count > 0:
                self.stdout.write(self.style.SUCCESS(f'Successfully downloaded {downloaded_count} REF_FINREP report templates'))
            else:
                self.stdout.write(self.style.WARNING('No REF_FINREP report templates found to download'))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error downloading report templates: {str(e)}'))
            logger.error(f'Error downloading report templates: {str(e)}', exc_info=True)
            raise