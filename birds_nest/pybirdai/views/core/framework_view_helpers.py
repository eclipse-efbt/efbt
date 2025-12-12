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
Framework-aware view helpers for filtering Django querysets by framework.

This module provides convenience functions for views to filter data
by the current framework (FINREP, COREP, ANCRDT, etc.).
"""

import logging
from typing import Optional
from django.http import HttpRequest
from pybirdai.views.core.framework_filters import FrameworkSubgraphFetcher

logger = logging.getLogger(__name__)


def get_current_framework(request: HttpRequest) -> Optional[str]:
    """
    Get the current framework from request session.

    Args:
        request: Django HttpRequest object

    Returns:
        Framework ID (e.g., 'EBA_FINREP') or None if not set
    """
    return FrameworkSubgraphFetcher.get_current_framework_from_session(request)


def set_current_framework(request: HttpRequest, framework_id: str):
    """
    Set the current framework in request session.

    Args:
        request: Django HttpRequest object
        framework_id: Framework ID (e.g., 'EBA_FINREP', 'FINREP', or 'EBA_COREP')
    """
    # Normalize framework_id to include EBA_ prefix
    if not framework_id.startswith('EBA_'):
        framework_id = f'EBA_{framework_id}'

    FrameworkSubgraphFetcher.set_current_framework_in_session(request, framework_id)
    logger.info(f"Set framework in session: {framework_id}")


def get_variables_for_current_framework(request: HttpRequest, fallback_to_all: bool = True):
    """
    Get VARIABLE queryset filtered by current framework.

    Args:
        request: Django HttpRequest object
        fallback_to_all: If True, returns all variables when no framework is set

    Returns:
        Django queryset of VARIABLE objects
    """
    from pybirdai.models.bird_meta_data_model import VARIABLE

    framework_id = get_current_framework(request)
    if framework_id:
        logger.debug(f"Filtering variables by framework: {framework_id}")
        return FrameworkSubgraphFetcher.get_variables_for_framework(framework_id)
    elif fallback_to_all:
        logger.debug("No framework set, returning all variables")
        return VARIABLE.objects.all()
    else:
        return VARIABLE.objects.none()


def get_members_for_current_framework(request: HttpRequest, fallback_to_all: bool = True):
    """
    Get MEMBER queryset filtered by current framework.

    Args:
        request: Django HttpRequest object
        fallback_to_all: If True, returns all members when no framework is set

    Returns:
        Django queryset of MEMBER objects
    """
    from pybirdai.models.bird_meta_data_model import MEMBER

    framework_id = get_current_framework(request)
    if framework_id:
        logger.debug(f"Filtering members by framework: {framework_id}")
        return FrameworkSubgraphFetcher.get_members_for_framework(framework_id)
    elif fallback_to_all:
        logger.debug("No framework set, returning all members")
        return MEMBER.objects.all()
    else:
        return MEMBER.objects.none()


def get_cubes_for_current_framework(request: HttpRequest, fallback_to_all: bool = True):
    """
    Get CUBE queryset filtered by current framework.

    Args:
        request: Django HttpRequest object
        fallback_to_all: If True, returns all cubes when no framework is set

    Returns:
        Django queryset of CUBE objects
    """
    from pybirdai.models.bird_meta_data_model import CUBE

    framework_id = get_current_framework(request)
    if framework_id:
        logger.debug(f"Filtering cubes by framework: {framework_id}")
        return FrameworkSubgraphFetcher.get_cubes_for_framework(framework_id)
    elif fallback_to_all:
        logger.debug("No framework set, returning all cubes")
        return CUBE.objects.all()
    else:
        return CUBE.objects.none()


def get_tables_for_current_framework(request: HttpRequest, fallback_to_all: bool = True):
    """
    Get TABLE queryset filtered by current framework.

    Args:
        request: Django HttpRequest object
        fallback_to_all: If True, returns all tables when no framework is set

    Returns:
        Django queryset of TABLE objects
    """
    from pybirdai.models.bird_meta_data_model import TABLE

    framework_id = get_current_framework(request)
    if framework_id:
        logger.debug(f"Filtering tables by framework: {framework_id}")
        return FrameworkSubgraphFetcher.get_tables_for_framework(framework_id)
    elif fallback_to_all:
        logger.debug("No framework set, returning all tables")
        return TABLE.objects.all()
    else:
        return TABLE.objects.none()


def get_domains_for_current_framework(request: HttpRequest, fallback_to_all: bool = True):
    """
    Get DOMAIN queryset filtered by current framework.

    Args:
        request: Django HttpRequest object
        fallback_to_all: If True, returns all domains when no framework is set

    Returns:
        Django queryset of DOMAIN objects
    """
    from pybirdai.models.bird_meta_data_model import DOMAIN

    framework_id = get_current_framework(request)
    if framework_id:
        logger.debug(f"Filtering domains by framework: {framework_id}")
        return FrameworkSubgraphFetcher.get_domains_for_framework(framework_id)
    elif fallback_to_all:
        logger.debug("No framework set, returning all domains")
        return DOMAIN.objects.all()
    else:
        return DOMAIN.objects.none()


def add_framework_context(request: HttpRequest, context: dict) -> dict:
    """
    Add framework information to template context.

    Args:
        request: Django HttpRequest object
        context: Existing template context dictionary

    Returns:
        Updated context dictionary with framework info
    """
    framework_id = get_current_framework(request)
    context['current_framework'] = framework_id
    context['current_framework_code'] = framework_id.replace('EBA_', '') if framework_id else None
    context['has_framework_filter'] = framework_id is not None

    # Add framework statistics if framework is set
    if framework_id:
        try:
            stats = FrameworkSubgraphFetcher.get_framework_statistics(framework_id)
            context['framework_stats'] = stats
        except Exception as e:
            logger.warning(f"Could not get framework statistics: {e}")
            context['framework_stats'] = {}

    return context


# Convenience function for common view pattern
def get_framework_filtered_context(request: HttpRequest) -> dict:
    """
    Get common framework-filtered querysets for template context.

    This is a convenience function that returns the most commonly needed
    querysets filtered by framework.

    Args:
        request: Django HttpRequest object

    Returns:
        Dictionary with framework-filtered querysets
    """
    context = {
        'all_variables': get_variables_for_current_framework(request),
        'all_members': get_members_for_current_framework(request),
        'all_cubes': get_cubes_for_current_framework(request),
        'all_tables': get_tables_for_current_framework(request),
    }

    # Add framework metadata
    context = add_framework_context(request, context)

    return context
