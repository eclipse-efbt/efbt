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
from django.contrib.auth import get_user_model
from django.conf import settings
import os
import json
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Ensure a superuser exists for development/codespace environments'

    # Default credentials for development use
    DEFAULT_USERNAME = 'admin'
    DEFAULT_PASSWORD = 'password'
    DEFAULT_EMAIL = 'a@b.com'

    def handle(self, *args, **options):
        User = get_user_model()

        # Check if superuser already exists
        if User.objects.filter(is_superuser=True).exists():
            self.stdout.write(
                self.style.SUCCESS('Superuser already exists - skipping creation')
            )
            # Still save credentials file so home page can display them
            self._save_credentials()
            return

        try:
            # Create the superuser
            user = User.objects.create_superuser(
                username=self.DEFAULT_USERNAME,
                email=self.DEFAULT_EMAIL,
                password=self.DEFAULT_PASSWORD
            )

            self.stdout.write(
                self.style.SUCCESS(
                    f'Successfully created superuser:\n'
                    f'  Username: {self.DEFAULT_USERNAME}\n'
                    f'  Password: {self.DEFAULT_PASSWORD}\n'
                    f'  Email: {self.DEFAULT_EMAIL}\n'
                    f'  Admin URL: /admin/'
                )
            )

            # Save credentials to file for home page display
            self._save_credentials()

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Failed to create superuser: {str(e)}')
            )
            logger.error(f'Failed to create superuser: {e}')
            raise

    def _save_credentials(self):
        """Save credentials to a JSON file for home page display."""
        try:
            base_dir = getattr(settings, 'BASE_DIR', '.')
            if hasattr(base_dir, '__fspath__'):
                base_dir = str(base_dir)

            credentials_path = os.path.join(base_dir, '.superuser_credentials.json')

            credentials = {
                'username': self.DEFAULT_USERNAME,
                'password': self.DEFAULT_PASSWORD,
                'email': self.DEFAULT_EMAIL,
                'admin_url': '/admin/'
            }

            with open(credentials_path, 'w') as f:
                json.dump(credentials, f, indent=2)

            logger.info(f'Saved superuser credentials to {credentials_path}')

        except Exception as e:
            logger.warning(f'Could not save credentials file: {e}')
            # Non-fatal - superuser was still created
