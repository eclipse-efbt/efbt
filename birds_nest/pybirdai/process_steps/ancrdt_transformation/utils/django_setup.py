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
Django environment setup utility for ANCRDT transformation processes.

This module provides a reusable Django configuration mechanism that ensures
Django is properly initialized before using Django models and ORM functionality.
It uses a singleton pattern to prevent redundant initialization.
"""

import django
import os
import sys
import logging

logger = logging.getLogger(__name__)


class DjangoSetup:
    """
    Singleton class for configuring Django environment.

    This class ensures Django is configured exactly once per process,
    preventing redundant initialization and potential configuration conflicts.

    Usage:
        from pybirdai.process_steps.ancrdt_transformation.utils import DjangoSetup

        DjangoSetup.configure_django()
        # Now Django models can be imported and used
    """

    _initialized = False

    @classmethod
    def configure_django(cls, logger_instance=None):
        """
        Configure Django settings and initialize Django.

        This method sets up the Django environment by:
        1. Adding the project root to sys.path
        2. Setting the DJANGO_SETTINGS_MODULE environment variable
        3. Calling django.setup() to initialize Django

        The configuration only happens once per process (singleton pattern).
        Subsequent calls are no-ops.

        Args:
            logger_instance (logging.Logger, optional): Custom logger to use.
                If None, uses the module's default logger.

        Raises:
            Exception: If Django configuration fails

        Example:
            >>> DjangoSetup.configure_django()
            >>> from pybirdai.models import CUBE
            >>> cubes = CUBE.objects.all()
        """
        if cls._initialized:
            return

        log = logger_instance or logger

        try:
            # Calculate project root (birds_nest directory)
            # Current file is in: birds_nest/pybirdai/process_steps/ancrdt_transformation/utils/
            # Project root is 4 levels up
            project_root = os.path.abspath(
                os.path.join(os.path.dirname(__file__), '../../../..')
            )

            # Add project root to Python path if not already present
            if project_root not in sys.path:
                sys.path.insert(0, project_root)

            # Set Django settings module
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

            # Initialize Django
            # This allows us to use Django models without running the server
            django.setup()

            log.info(
                "Django configured successfully with settings module: %s",
                os.environ['DJANGO_SETTINGS_MODULE']
            )
            cls._initialized = True

        except Exception as e:
            log.error(f"Django configuration failed: {str(e)}")
            raise

    @classmethod
    def is_initialized(cls):
        """
        Check if Django has been initialized.

        Returns:
            bool: True if Django is configured, False otherwise
        """
        return cls._initialized

    @classmethod
    def reset(cls):
        """
        Reset the initialization flag.

        This method is primarily for testing purposes, allowing Django
        to be reconfigured in a new test environment.

        Warning:
            This should not be used in production code. Django initialization
            should only happen once per process.
        """
        cls._initialized = False
