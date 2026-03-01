# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
"""
Management command to fix lineage references by deleting all FunctionColumnReference
records so they can be recreated with dependency_string populated.
"""

from django.core.management.base import BaseCommand
from pybirdai.models import FunctionColumnReference


class Command(BaseCommand):
    help = 'Delete all FunctionColumnReference records so they can be recreated with dependency_string'

    def add_arguments(self, parser):
        parser.add_argument(
            '--all',
            action='store_true',
            help='Delete ALL FunctionColumnReference records (needed to populate dependency_string)',
        )

    def handle(self, *args, **options):
        if options['all']:
            # Delete ALL FunctionColumnReference records
            count = FunctionColumnReference.objects.count()
            FunctionColumnReference.objects.all().delete()
            self.stdout.write(self.style.SUCCESS(f"Deleted all {count} FunctionColumnReference records"))
        else:
            # Just delete Cell metric_value references
            from pybirdai.models import Function
            cell_functions = Function.objects.filter(
                table__name__startswith='Cell_',
                name__contains='metric_value'
            )
            deleted_count, _ = FunctionColumnReference.objects.filter(
                function__in=cell_functions
            ).delete()
            self.stdout.write(self.style.SUCCESS(f"Deleted {deleted_count} Cell metric_value references"))

        self.stdout.write("Re-run your datapoint execution to create new references with dependency_string")
