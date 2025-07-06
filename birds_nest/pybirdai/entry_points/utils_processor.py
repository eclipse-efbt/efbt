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

"""
Utils Processor Entry Point

This entry point provides access to general utility functionality
through the process step architecture, maintaining backward compatibility
with existing imports from utils.utils.
"""

import django
import os
from django.apps import AppConfig
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class RunUtilsProcessor(AppConfig):
    """
    Django AppConfig for running general utility processing services.

    This entry point provides access to string processing, validation,
    and other utility functionality through the process step architecture.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    def ready(self):
        """
        Prepare and execute utils processor services.

        This method sets up the necessary contexts and provides access to
        general utility processing services.
        """
        from pybirdai.process_steps.utils_integration.data_processing.utils_processor import (
            UtilsProcessorProcessStep
        )
        from pybirdai.context.context import Context

        logger.info("Initializing Utils Processor entry point")

        try:
            # Create context for utils processor services
            context = Context()

            # Initialize utils processor process step
            utils_processor_step = UtilsProcessorProcessStep(context)
            context.utils_processor_step = utils_processor_step

            # Store context globally for access by other components
            if not hasattr(settings, 'UTILS_PROCESSOR_CONTEXT'):
                settings.UTILS_PROCESSOR_CONTEXT = context

            logger.info("Utils Processor entry point initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Utils Processor: {e}")
            raise


def process_string_data(text: str, **kwargs):
    """
    Entry point function for string processing operations.

    Args:
        text (str): Text to process
        **kwargs: Additional processing parameters

    Returns:
        dict: Result dictionary with success status and details
    """
    try:
        from pybirdai.process_steps.utils_integration.data_processing.utils_processor import (
            UtilsProcessorProcessStep
        )

        step = UtilsProcessorProcessStep()
        result = step.execute(
            operation="string_processing",
            text=text,
            **kwargs
        )

        logger.info(f"String processing completed: {result.get('message', 'Success')}")
        return result

    except Exception as e:
        logger.error(f"String processing failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'String processing failed'
        }


def generate_unique_value(enum_obj, adapted_value):
    """
    Entry point function for generating unique enumeration values.

    Args:
        enum_obj: The enumeration object
        adapted_value (str): Value to make unique

    Returns:
        dict: Result dictionary with success status and details
    """
    try:
        from pybirdai.process_steps.utils_integration.data_processing.utils_processor import (
            UtilsProcessorProcessStep
        )

        step = UtilsProcessorProcessStep()
        result = step.execute(
            operation="unique_value",
            enum_obj=enum_obj,
            adapted_value=adapted_value
        )

        logger.info(f"Unique value generation completed: {result.get('message', 'Success')}")
        return result

    except Exception as e:
        logger.error(f"Unique value generation failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Unique value generation failed'
        }


def generate_unique_name(enum_obj, enum_used_name):
    """
    Entry point function for generating unique enumeration names.

    Args:
        enum_obj: The enumeration object
        enum_used_name (str): Name to make unique

    Returns:
        dict: Result dictionary with success status and details
    """
    try:
        from pybirdai.process_steps.utils_integration.data_processing.utils_processor import (
            UtilsProcessorProcessStep
        )

        step = UtilsProcessorProcessStep()
        result = step.execute(
            operation="unique_name",
            enum_obj=enum_obj,
            enum_used_name=enum_used_name
        )

        logger.info(f"Unique name generation completed: {result.get('message', 'Success')}")
        return result

    except Exception as e:
        logger.error(f"Unique name generation failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Unique name generation failed'
        }


# Convenience functions for backward compatibility
def get_utils_processor():
    """
    Get a Utils processor instance.

    Returns:
        Utils: Configured utils processor instance
    """
    from pybirdai.process_steps.utils_integration.data_processing.utils_processor import Utils

    return Utils()


# Legacy compatibility - re-export Utils class from original module
try:
    from pybirdai.utils.utils import Utils
except ImportError:
    logger.warning("Could not import original Utils class")
    pass


# Additional backward compatibility functions
def unique_value(the_enum, adapted_value):
    """
    Backward compatibility wrapper for Utils.unique_value
    """
    utils = get_utils_processor()
    return utils.unique_value(the_enum, adapted_value)


def unique_name(the_enum, enum_used_name):
    """
    Backward compatibility wrapper for Utils.unique_name
    """
    utils = get_utils_processor()
    return utils.unique_name(the_enum, enum_used_name)


def sanitize_identifier(identifier: str):
    """
    Backward compatibility wrapper for Utils.sanitize_identifier
    """
    utils = get_utils_processor()
    return utils.sanitize_identifier(identifier)


def process_string(text: str, **kwargs):
    """
    Backward compatibility wrapper for Utils.process_string
    """
    utils = get_utils_processor()
    return utils.process_string(text, **kwargs)
