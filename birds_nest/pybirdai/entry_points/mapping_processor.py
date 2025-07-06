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
Mapping Processor Entry Point

This entry point provides access to mapping library functionality
through the process step architecture, maintaining backward compatibility
with existing imports from utils.mapping_library.
"""

import django
import os
from django.apps import AppConfig
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class RunMappingProcessor(AppConfig):
    """
    Django AppConfig for running mapping processing services.

    This entry point provides access to mapping library functionality
    through the process step architecture.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    def ready(self):
        """
        Prepare and execute mapping processor services.

        This method sets up the necessary contexts and provides access to
        mapping processing services.
        """
        logger.info("Initializing Mapping Processor entry point")

        try:
            # Import the original mapping library functions for backward compatibility
            from pybirdai.utils.mapping_library import build_mapping_results,add_variable_to_mapping,create_or_update_member,update_member_mapping_item,delete_member_mapping_item,get_member_by_code,seach_members,create_mapping_definition,update_mapping_definition,get_mapping_definitions,validate_mapping,export_mapping_to_csv,import_mapping_from_csv

            logger.info("Mapping Processor entry point initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Mapping Processor: {e}")
            raise


# Re-export all functions from the original mapping library for backward compatibility
def build_mapping_results(*args, **kwargs):
    """
    Build mapping results using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.build_mapping_results
    """
    from pybirdai.utils.mapping_library import build_mapping_results as original_function
    return original_function(*args, **kwargs)


def add_variable_to_mapping(*args, **kwargs):
    """
    Add variable to mapping using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.add_variable_to_mapping
    """
    from pybirdai.utils.mapping_library import add_variable_to_mapping as original_function
    return original_function(*args, **kwargs)


def create_or_update_member(*args, **kwargs):
    """
    Create or update member using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.create_or_update_member
    """
    from pybirdai.utils.mapping_library import create_or_update_member as original_function
    return original_function(*args, **kwargs)


def update_member_mapping_item(*args, **kwargs):
    """
    Update member mapping item using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.update_member_mapping_item
    """
    from pybirdai.utils.mapping_library import update_member_mapping_item as original_function
    return original_function(*args, **kwargs)


def delete_member_mapping_item(*args, **kwargs):
    """
    Delete member mapping item using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.delete_member_mapping_item
    """
    from pybirdai.utils.mapping_library import delete_member_mapping_item as original_function
    return original_function(*args, **kwargs)


def get_member_by_code(*args, **kwargs):
    """
    Get member by code using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.get_member_by_code
    """
    from pybirdai.utils.mapping_library import get_member_by_code as original_function
    return original_function(*args, **kwargs)


def search_members(*args, **kwargs):
    """
    Search members using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.search_members
    """
    from pybirdai.utils.mapping_library import search_members as original_function
    return original_function(*args, **kwargs)


def create_mapping_definition(*args, **kwargs):
    """
    Create mapping definition using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.create_mapping_definition
    """
    from pybirdai.utils.mapping_library import create_mapping_definition as original_function
    return original_function(*args, **kwargs)


def update_mapping_definition(*args, **kwargs):
    """
    Update mapping definition using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.update_mapping_definition
    """
    from pybirdai.utils.mapping_library import update_mapping_definition as original_function
    return original_function(*args, **kwargs)


def get_mapping_definitions(*args, **kwargs):
    """
    Get mapping definitions using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.get_mapping_definitions
    """
    from pybirdai.utils.mapping_library import get_mapping_definitions as original_function
    return original_function(*args, **kwargs)


def validate_mapping(*args, **kwargs):
    """
    Validate mapping using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.validate_mapping
    """
    from pybirdai.utils.mapping_library import validate_mapping as original_function
    return original_function(*args, **kwargs)


def export_mapping_to_csv(*args, **kwargs):
    """
    Export mapping to CSV using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.export_mapping_to_csv
    """
    from pybirdai.utils.mapping_library import export_mapping_to_csv as original_function
    return original_function(*args, **kwargs)


def import_mapping_from_csv(*args, **kwargs):
    """
    Import mapping from CSV using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.import_mapping_from_csv
    """
    from pybirdai.utils.mapping_library import import_mapping_from_csv as original_function
    return original_function(*args, **kwargs)


def process_related_mappings(*args, **kwargs):
    """
    Process related mappings using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.process_related_mappings
    """
    try:
        from pybirdai.utils.mapping_library import process_related_mappings as original_function
        return original_function(*args, **kwargs)
    except ImportError:
        logger.warning("process_related_mappings not found in mapping_library, using placeholder")
        return None


def process_member_mappings(*args, **kwargs):
    """
    Process member mappings using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.process_member_mappings
    """
    try:
        from pybirdai.utils.mapping_library import process_member_mappings as original_function
        return original_function(*args, **kwargs)
    except ImportError:
        logger.warning("process_member_mappings not found in mapping_library, using placeholder")
        return None


def create_table_data(*args, **kwargs):
    """
    Create table data using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.create_table_data
    """
    try:
        from pybirdai.utils.mapping_library import create_table_data as original_function
        return original_function(*args, **kwargs)
    except ImportError:
        logger.warning("create_table_data not found in mapping_library, using placeholder")
        return None


def get_reference_variables(*args, **kwargs):
    """
    Get reference variables using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.get_reference_variables
    """
    try:
        from pybirdai.utils.mapping_library import get_reference_variables as original_function
        return original_function(*args, **kwargs)
    except ImportError:
        logger.warning("get_reference_variables not found in mapping_library, using placeholder")
        return None


def get_source_variables(*args, **kwargs):
    """
    Get source variables using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.get_source_variables
    """
    try:
        from pybirdai.utils.mapping_library import get_source_variables as original_function
        return original_function(*args, **kwargs)
    except ImportError:
        logger.warning("get_source_variables not found in mapping_library, using placeholder")
        return None


def cascade_member_mapping_changes(*args, **kwargs):
    """
    Cascade member mapping changes using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.cascade_member_mapping_changes
    """
    try:
        from pybirdai.utils.mapping_library import cascade_member_mapping_changes as original_function
        return original_function(*args, **kwargs)
    except ImportError:
        logger.warning("cascade_member_mapping_changes not found in mapping_library, using placeholder")
        return None


def process_mapping_chain(*args, **kwargs):
    """
    Process mapping chain using the process step architecture.
    Backward compatibility wrapper for utils.mapping_library.process_mapping_chain
    """
    try:
        from pybirdai.utils.mapping_library import process_mapping_chain as original_function
        return original_function(*args, **kwargs)
    except ImportError:
        logger.warning("process_mapping_chain not found in mapping_library, using placeholder")
        return None


# Legacy compatibility - import all functions from original module
try:
    from pybirdai.utils.mapping_library import *
except ImportError:
    logger.warning("Could not import original mapping_library functions")
    pass
