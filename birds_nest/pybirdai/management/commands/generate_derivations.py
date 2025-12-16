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
Django management command for generating derived fields from ECB logical transformation rules.

Workflow:
    1. --download: Download transformation rules from ECB
    2. --generate: Generate Python files for ALL DER rules (no config needed)
    3. --export-config: Create config file with all rules (set enabled=true for ones you want)
    4. --merge: Merge only enabled rules (from config) into bird data model

Usage:
    # Step 1: Download transformation rules from ECB
    python manage.py generate_derivations --download

    # Step 2: Generate Python derivation files for ALL rules
    python manage.py generate_derivations --generate

    # Step 3: List available derived fields to see what's available
    python manage.py generate_derivations --list

    # Step 4: Export all rules to config file (then edit to enable the ones you want)
    python manage.py generate_derivations --export-config

    # Step 5: Merge ONLY enabled rules into bird data model
    python manage.py generate_derivations --merge

    # Or do steps 1-2 together:
    python manage.py generate_derivations --download --generate
"""

from django.core.management.base import BaseCommand
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Generate derived fields from ECB logical transformation rules'

    def add_arguments(self, parser):
        parser.add_argument(
            '--download',
            action='store_true',
            help='Download logical transformation rules from ECB website',
        )
        parser.add_argument(
            '--generate',
            action='store_true',
            help='Generate Python derivation files for ALL DER rules (no config filter)',
        )
        parser.add_argument(
            '--merge',
            action='store_true',
            help='Merge derived fields into bird data model (uses config to filter)',
        )
        parser.add_argument(
            '--list',
            action='store_true',
            help='List all available derived fields from transformation rules',
        )
        parser.add_argument(
            '--export-config',
            action='store_true',
            help='Export all available rules to derivation_config.csv',
        )
        parser.add_argument(
            '--output-dir',
            type=str,
            default='resources/technical_export',
            help='Directory for downloaded CSV files',
        )
        parser.add_argument(
            '--config-csv',
            type=str,
            default='resources/derivation_files/derivation_config.csv',
            help='Path to derivation configuration CSV',
        )
        parser.add_argument(
            '--generated-output-dir',
            type=str,
            default='resources/derivation_files/generated_from_logical_transformation_rules',
            help='Directory for generated Python files',
        )
        parser.add_argument(
            '--enable-all',
            action='store_true',
            help='When exporting config, enable all fields by default',
        )

    def handle(self, *args, **options):
        from pybirdai.entry_points.database_setup import (
            run_download_transformation_rules,
            run_generate_derivation_files,
            run_list_available_rules,
            run_merge_derived_fields,
            export_available_rules_to_config,
        )
        from pybirdai.process_steps.database_setup.derivation_pipeline import (
            DEFAULT_TRANSFORMATION_RULES_CSV,
        )
        import os

        # Determine transformation rules CSV path
        # Note: ECB API returns 'logical_transformation_rule.csv' (not sddlogicaltransformationrule.csv)
        transformation_rules_csv = os.path.join(
            options['output_dir'], 'logical_transformation_rule.csv'
        )

        # Handle --download
        if options['download']:
            self.stdout.write(self.style.SUCCESS('Downloading transformation rules from ECB...'))
            try:
                csv_path = run_download_transformation_rules(output_dir=options['output_dir'])
                self.stdout.write(self.style.SUCCESS(f'Downloaded to: {csv_path}'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Download failed: {e}'))
                return

        # Handle --list
        if options['list']:
            self.stdout.write(self.style.SUCCESS('Listing available derived fields...'))
            if not os.path.exists(transformation_rules_csv):
                self.stdout.write(self.style.ERROR(
                    f'Transformation rules CSV not found: {transformation_rules_csv}\n'
                    'Run with --download first.'
                ))
                return
            available = run_list_available_rules(transformation_rules_csv)
            self._display_available_rules(available)

        # Handle --export-config
        if options['export_config']:
            self.stdout.write(self.style.SUCCESS('Exporting available rules to config...'))
            if not os.path.exists(transformation_rules_csv):
                self.stdout.write(self.style.ERROR(
                    f'Transformation rules CSV not found: {transformation_rules_csv}\n'
                    'Run with --download first.'
                ))
                return
            config_path = export_available_rules_to_config(
                transformation_rules_csv=transformation_rules_csv,
                config_csv=options['config_csv'],
                enabled_by_default=options['enable_all']
            )
            self.stdout.write(self.style.SUCCESS(f'Exported config to: {config_path}'))

        # Handle --generate
        if options['generate']:
            self.stdout.write(self.style.SUCCESS('Generating derivation files for ALL DER rules...'))
            if not os.path.exists(transformation_rules_csv):
                self.stdout.write(self.style.ERROR(
                    f'Transformation rules CSV not found: {transformation_rules_csv}\n'
                    'Run with --download first.'
                ))
                return
            generated = run_generate_derivation_files(
                transformation_rules_csv=transformation_rules_csv,
                output_dir=options['generated_output_dir']
            )
            self._display_generated_files(generated)

        # Handle --merge
        if options['merge']:
            self.stdout.write(self.style.SUCCESS('Merging derived fields into bird data model...'))
            try:
                result = run_merge_derived_fields()
                if result:
                    self.stdout.write(self.style.SUCCESS('Successfully merged derived fields'))
                else:
                    self.stdout.write(self.style.WARNING(
                        'No modifications made (file may already be modified)'
                    ))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Merge failed: {e}'))

        # If no action specified, show help
        if not any([options['download'], options['generate'], options['merge'],
                    options['list'], options['export_config']]):
            self.stdout.write(self.style.WARNING(
                'No action specified. Use --help to see available options.'
            ))

    def _display_available_rules(self, available: dict):
        """Display available rules in a formatted way."""
        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'Found {len(available)} classes with derived fields:'))
        self.stdout.write('')

        for class_name in sorted(available.keys()):
            fields = available[class_name]
            self.stdout.write(f'  {self.style.HTTP_INFO(class_name)}: {len(fields)} field(s)')
            for field in fields:
                self.stdout.write(f'    - {field}')
            self.stdout.write('')

    def _display_generated_files(self, generated: dict):
        """Display generated files in a formatted way."""
        self.stdout.write('')
        if not generated:
            self.stdout.write(self.style.WARNING('No files generated.'))
            self.stdout.write('Check that:')
            self.stdout.write('  1. The transformation rules CSV exists')
            self.stdout.write('  2. The derivation config CSV has enabled entries')
            self.stdout.write('  3. The configured fields exist in the transformation rules')
            return

        self.stdout.write(self.style.SUCCESS(f'Generated {len(generated)} file(s):'))
        self.stdout.write('')

        for class_name, file_path in sorted(generated.items()):
            self.stdout.write(f'  {self.style.HTTP_INFO(class_name)}: {file_path}')
