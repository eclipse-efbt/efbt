# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation

"""
Management command to import CSV data from technical_export into the database.

Usage:
    python manage.py import_technical_export                     # Import all
    python manage.py import_technical_export --templates-only    # Only report templates
    python manage.py import_technical_export --hierarchies-only  # Only hierarchies
    python manage.py import_technical_export --dpm               # Use DPM mode (CSV copy)
    python manage.py import_technical_export --input-dir results/technical_export/
"""

from django.core.management.base import BaseCommand, CommandError
import os
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Import CSV data from technical_export directory into the database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--input-dir',
            type=str,
            default='results/technical_export',
            help='Directory containing CSV files (default: results/technical_export)'
        )
        parser.add_argument(
            '--templates-only',
            action='store_true',
            help='Only import report templates (domains, variables, tables, axes, cells, etc.)'
        )
        parser.add_argument(
            '--hierarchies-only',
            action='store_true',
            help='Only import hierarchies (member hierarchies and nodes)'
        )
        parser.add_argument(
            '--semantic-only',
            action='store_true',
            help='Only import semantic integrations (mappings)'
        )
        parser.add_argument(
            '--dpm',
            action='store_true',
            help='Use DPM mode with CSV copy for large datasets (faster but requires PostgreSQL)'
        )
        parser.add_argument(
            '--list-files',
            action='store_true',
            help='List CSV files in the input directory without importing'
        )

    def handle(self, *args, **options):
        input_dir = options['input_dir']

        # Validate input directory
        if not os.path.exists(input_dir):
            raise CommandError(f'Input directory not found: {input_dir}')

        if options['list_files']:
            self.list_csv_files(input_dir)
            return

        # Determine what to import
        import_templates = not options['hierarchies_only'] and not options['semantic_only']
        import_hierarchies = not options['templates_only'] and not options['semantic_only']
        import_semantic = options['semantic_only'] or (not options['templates_only'] and not options['hierarchies_only'])

        # If semantic-only is specified, don't import templates or hierarchies
        if options['semantic_only']:
            import_templates = False
            import_hierarchies = False
            import_semantic = True

        use_dpm = options['dpm']

        self.stdout.write(self.style.SUCCESS(f'Starting import from: {input_dir}'))
        self.stdout.write(f'  Import templates: {import_templates}')
        self.stdout.write(f'  Import hierarchies: {import_hierarchies}')
        self.stdout.write(f'  Import semantic integrations: {import_semantic}')
        self.stdout.write(f'  DPM mode (CSV copy): {use_dpm}\n')

        # Create SDDContext
        from pybirdai.context.sdd_context_django import SDDContext
        sdd_context = SDDContext()

        # Set directories - the import functions expect file_directory to be the parent
        # and will append 'technical_export/' internally
        if 'technical_export' in input_dir:
            # Strip technical_export from path as it's added by the import functions
            parent_dir = input_dir.replace('/technical_export', '').replace('\\technical_export', '')
            if parent_dir.endswith('/') or parent_dir.endswith('\\'):
                parent_dir = parent_dir[:-1]
            sdd_context.file_directory = parent_dir + os.sep
        else:
            sdd_context.file_directory = input_dir + os.sep

        sdd_context.output_directory = 'results' + os.sep

        self.stdout.write(f'  File directory: {sdd_context.file_directory}')

        # Import using the existing orchestrator
        from pybirdai.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import ImportWebsiteToSDDModel
        importer = ImportWebsiteToSDDModel()

        try:
            if import_templates:
                self.stdout.write(self.style.HTTP_INFO('\n--- Importing Report Templates ---'))
                self.stdout.write('This includes: maintenance agencies, frameworks, domains, members,')
                self.stdout.write('variables, subdomains, tables, axes, axis ordinates,')
                self.stdout.write('table cells, ordinate items, cell positions\n')

                importer.import_report_templates_from_sdd(sdd_context, dpm=use_dpm)
                self.stdout.write(self.style.SUCCESS('Report templates imported successfully!'))

            if import_hierarchies:
                self.stdout.write(self.style.HTTP_INFO('\n--- Importing Hierarchies ---'))
                self.stdout.write('This includes: member hierarchies, member hierarchy nodes,')
                self.stdout.write('parent-child member relationships\n')

                importer.import_hierarchies_from_sdd(sdd_context)
                self.stdout.write(self.style.SUCCESS('Hierarchies imported successfully!'))

            if import_semantic:
                self.stdout.write(self.style.HTTP_INFO('\n--- Importing Semantic Integrations ---'))
                self.stdout.write('This includes: variable mappings, member mappings,')
                self.stdout.write('mapping definitions, mapping to cube relationships\n')

                importer.import_semantic_integrations_from_sdd(sdd_context)
                self.stdout.write(self.style.SUCCESS('Semantic integrations imported successfully!'))

            self.stdout.write(self.style.SUCCESS('\n=== Import completed successfully! ==='))

        except Exception as e:
            logger.exception(f'Error during import: {e}')
            raise CommandError(f'Import failed: {str(e)}')

    def list_csv_files(self, input_dir):
        """List CSV files in the input directory."""
        self.stdout.write(self.style.SUCCESS(f'CSV files in {input_dir}:\n'))

        csv_files = []
        for filename in sorted(os.listdir(input_dir)):
            if filename.endswith('.csv'):
                filepath = os.path.join(input_dir, filename)
                size = os.path.getsize(filepath)
                size_str = self._format_size(size)

                # Count lines
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        line_count = sum(1 for _ in f) - 1  # Subtract header
                except:
                    line_count = '?'

                csv_files.append((filename, size_str, line_count))

        if not csv_files:
            self.stdout.write('  No CSV files found.')
            return

        # Print table
        self.stdout.write(f'{"Filename":<40} {"Size":<12} {"Records":<10}')
        self.stdout.write('-' * 62)

        for filename, size_str, line_count in csv_files:
            self.stdout.write(f'{filename:<40} {size_str:<12} {line_count:<10}')

        self.stdout.write(f'\nTotal: {len(csv_files)} files')

    def _format_size(self, size):
        """Format file size in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f'{size:.1f} {unit}'
            size /= 1024
        return f'{size:.1f} TB'
