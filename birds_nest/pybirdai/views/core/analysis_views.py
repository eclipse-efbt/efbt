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
#    Neil Mackenzie - initial API and implementation
"""
Analysis views for duplicate detection, gap analysis, and visualizations.
"""
import logging
from django.http import HttpResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.views.generic import ListView
from django.core.paginator import Paginator

from pybirdai.models.bird_meta_data_model import (
    CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK
)
from pybirdai.context.sdd_context_django import SDDContext

logger = logging.getLogger(__name__)


class DuplicatePrimaryMemberIdListView(ListView):
    """Class-based view for duplicate detection."""
    template_name = 'pybirdai/duplicate_primary_member_id_list.html'
    context_object_name = 'grouped_links'
    paginate_by = 20

    def get_queryset(self):
        """Get the queryset for duplicate detection."""
        # Get filter parameters
        foreign_cube_filter = self.request.GET.get('foreign_cube', '')
        primary_cube_filter = self.request.GET.get('primary_cube', '')

        # Get SDDContext
        sdd_context = SDDContext()

        # Get all cube_links and filter by foreign_cube if provided
        cube_links = CUBE_LINK.objects.all()

        if foreign_cube_filter:
            cube_links = cube_links.filter(foreign_cube_id=foreign_cube_filter)

        if primary_cube_filter:
            cube_links = cube_links.filter(primary_cube_id=primary_cube_filter)

        grouped_data = []
        for cube_link in cube_links:
            # Get the cube structure item links for this cube link
            links = sdd_context.cube_structure_item_link_to_cube_link_map.get(cube_link.cube_link_id, [])
            if links:
                grouped_data.append({
                    'cube_link': cube_link,
                    'links': links
                })

        return grouped_data

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Add filter options to context
        context['foreign_cubes'] = CUBE_LINK.objects.values_list('foreign_cube_id', flat=True).distinct()
        context['primary_cubes'] = CUBE_LINK.objects.values_list('primary_cube_id', flat=True).distinct()
        context['selected_foreign_cube'] = self.request.GET.get('foreign_cube', '')
        context['selected_primary_cube'] = self.request.GET.get('primary_cube', '')
        return context


class JoinIdentifierListView(ListView):
    """Class-based view for join identifiers."""
    model = CUBE_LINK
    template_name = 'pybirdai/join_identifier_list.html'
    context_object_name = 'cube_links'
    paginate_by = 20


def duplicate_primary_member_id_list(request):
    """Function-based duplicate list with filters."""
    sdd_context = SDDContext()

    # Get filter parameters
    foreign_cube_filter = request.GET.get('foreign_cube', '')
    primary_cube_filter = request.GET.get('primary_cube', '')
    join_identifier_filter = request.GET.get('join_identifier', '')

    # Get all cube_links
    cube_links = CUBE_LINK.objects.all().order_by('cube_link_id')

    # Apply filters
    if foreign_cube_filter:
        cube_links = cube_links.filter(foreign_cube_id=foreign_cube_filter)
    if primary_cube_filter:
        cube_links = cube_links.filter(primary_cube_id=primary_cube_filter)
    if join_identifier_filter:
        cube_links = cube_links.filter(join_identifier=join_identifier_filter)

    # Group links by cube_link
    grouped_data = []
    for cube_link in cube_links:
        links = sdd_context.cube_structure_item_link_to_cube_link_map.get(cube_link.cube_link_id, [])
        if links:
            grouped_data.append({
                'cube_link': cube_link,
                'links': links
            })

    # Paginate the grouped data
    page_number = request.GET.get('page', 1)
    paginator = Paginator(grouped_data, 20)
    page_obj = paginator.get_page(page_number)

    context = {
        'grouped_links': page_obj.object_list,
        'page_obj': page_obj,
        'foreign_cubes': CUBE_LINK.objects.values_list('foreign_cube_id', flat=True).distinct(),
        'primary_cubes': CUBE_LINK.objects.values_list('primary_cube_id', flat=True).distinct(),
        'join_identifiers': CUBE_LINK.objects.values_list('join_identifier', flat=True).distinct(),
        'selected_foreign_cube': foreign_cube_filter,
        'selected_primary_cube': primary_cube_filter,
        'selected_join_identifier': join_identifier_filter,
    }

    return render(request, 'pybirdai/duplicate_primary_member_id_list.html', context)


def show_gaps(request):
    """Show missing cube structure item links (gap analysis)."""
    sdd_context = SDDContext()

    # Get filter parameters
    foreign_cube_filter = request.GET.get('foreign_cube', '')
    primary_cube_filter = request.GET.get('primary_cube', '')

    # Get cube links
    cube_links = CUBE_LINK.objects.all().order_by('cube_link_id')

    # Apply filters
    if foreign_cube_filter:
        cube_links = cube_links.filter(foreign_cube_id=foreign_cube_filter)
    if primary_cube_filter:
        cube_links = cube_links.filter(primary_cube_id=primary_cube_filter)

    gaps = []
    for cube_link in cube_links:
        # Get foreign cube variables
        foreign_cube = cube_link.foreign_cube_id
        if foreign_cube and hasattr(foreign_cube, 'cube_structure_id'):
            foreign_variables = set(
                item.variable_id.variable_id
                for item in sdd_context.cube_structure_item_dictionary.values()
                if item.cube_structure_id and item.cube_structure_id.cube_structure_id == foreign_cube.cube_structure_id.cube_structure_id
                and item.variable_id
            )
        else:
            foreign_variables = set()

        # Get primary cube variables
        primary_cube = cube_link.primary_cube_id
        if primary_cube and hasattr(primary_cube, 'cube_structure_id'):
            primary_variables = set(
                item.variable_id.variable_id
                for item in sdd_context.cube_structure_item_dictionary.values()
                if item.cube_structure_id and item.cube_structure_id.cube_structure_id == primary_cube.cube_structure_id.cube_structure_id
                and item.variable_id
            )
        else:
            primary_variables = set()

        # Get linked variables
        links = sdd_context.cube_structure_item_link_to_cube_link_map.get(cube_link.cube_link_id, [])
        linked_foreign = set()
        linked_primary = set()
        for link in links:
            if link.foreign_cube_variable_code and link.foreign_cube_variable_code.variable_id:
                linked_foreign.add(link.foreign_cube_variable_code.variable_id.variable_id)
            if link.primary_cube_variable_code and link.primary_cube_variable_code.variable_id:
                linked_primary.add(link.primary_cube_variable_code.variable_id.variable_id)

        # Find gaps
        missing_foreign = foreign_variables - linked_foreign
        missing_primary = primary_variables - linked_primary

        if missing_foreign or missing_primary:
            gaps.append({
                'cube_link': cube_link,
                'missing_foreign': missing_foreign,
                'missing_primary': missing_primary
            })

    # Paginate
    page_number = request.GET.get('page', 1)
    paginator = Paginator(gaps, 20)
    page_obj = paginator.get_page(page_number)

    context = {
        'gaps': page_obj.object_list,
        'page_obj': page_obj,
        'foreign_cubes': CUBE_LINK.objects.values_list('foreign_cube_id', flat=True).distinct(),
        'primary_cubes': CUBE_LINK.objects.values_list('primary_cube_id', flat=True).distinct(),
        'selected_foreign_cube': foreign_cube_filter,
        'selected_primary_cube': primary_cube_filter,
    }

    return render(request, 'pybirdai/show_gaps.html', context)


def return_cubelink_visualisation(request):
    """Generate cube link visualizations."""
    from pybirdai.utils import visualisation_service

    logger.info("Handling cube link visualization request")
    if request.method == 'GET':
        cube_id = request.GET.get('cube_id', '')
        join_identifier = request.GET.get('join_identifier', '').replace("+", " ")
        in_md = request.GET.get('in_md', "false").lower() == 'true'
        logger.debug(f"Visualization params - cube_id: {cube_id}, join_identifier: {join_identifier}, in_md: {in_md}")

        if cube_id:
            logger.info(f"Generating visualization for cube_id: {cube_id}")
            html_content = visualisation_service.process_cube_visualization(cube_id, join_identifier, in_md)
            return HttpResponse(html_content)
        else:
            logger.warning("Missing required parameter: cube_link_id")
            return HttpResponseBadRequest("Missing required parameter: cube_link_id")
    else:
        logger.warning(f"Invalid request method: {request.method}")
        return HttpResponseBadRequest("Only GET requests are supported")
