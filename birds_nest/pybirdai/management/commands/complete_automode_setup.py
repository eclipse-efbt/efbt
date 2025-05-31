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

from django.core.management.base import BaseCommand
from pybirdai.entry_points.automode_database_setup import RunAutomodeDatabaseSetup
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Complete the automode database setup by applying file changes and running migrations'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting automode database setup completion...'))
        
        try:
            app_config = RunAutomodeDatabaseSetup('pybirdai', 'birds_nest')
            app_config.run_post_setup_operations()
            
            self.stdout.write(
                self.style.SUCCESS(
                    'Automode database setup completed successfully!\n'
                    'You can now start the Django server and use the application.'
                )
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Automode database setup failed: {str(e)}')
            )
            raise 