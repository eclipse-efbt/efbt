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
from collections import defaultdict
from django.http import HttpResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.views.generic import ListView
from django.core.paginator import Paginator

from pybirdai.models.bird_meta_data_model import (
    CUBE_LINK, CUBE_STRUCTURE_ITEM, CUBE_STRUCTURE_ITEM_LINK
)
from pybirdai.context.sdd_context_django import SDDContext
from pybirdai.context.context import Context
from pybirdai.process_steps.joins_meta_data.create_joins_meta_data import JoinsMetaDataCreator
from pybirdai.process_steps.joins_meta_data.main_category_finder import MainCategoryFinder

logger = logging.getLogger(__name__)

FRAMEWORK_VERSION_MAP = {
    "FINREP_REF": ["3", "3.0-Ind", "FINREP 3.0-Ind"],
    "COREP_REF": ["4", "4.0", "COREP 4.0"],
}


class DuplicatePrimaryMemberIdListView(ListView):
    """Class-based view for duplicate detection."""
    template_name = 'pybirdai/miscellaneous/duplicate_primary_member_id_list.html'
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
    template_name = 'pybirdai/miscellaneous/join_identifier_list.html'
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

    return render(request, 'pybirdai/miscellaneous/duplicate_primary_member_id_list.html', context)


def show_gaps(request):
    """Show output-layer items that could not be linked for a given product."""
    sdd_context = SDDContext()
    context = Context()
    context.ldm_or_il = 'il'
    joins_creator = JoinsMetaDataCreator()
    main_category_finder = MainCategoryFinder()

    # Get filter parameters
    foreign_cube_filter = request.GET.get('foreign_cube', '')
    primary_cube_filter = request.GET.get('primary_cube', '')
    join_identifier_filter = request.GET.get('join_identifier', '')

    cube_links = CUBE_LINK.objects.select_related(
        'foreign_cube_id__cube_structure_id',
        'primary_cube_id__cube_structure_id',
    ).all().order_by('cube_link_id')

    # Apply filters
    if foreign_cube_filter:
        cube_links = cube_links.filter(foreign_cube_id=foreign_cube_filter)
    if primary_cube_filter:
        cube_links = cube_links.filter(primary_cube_id=primary_cube_filter)
    if join_identifier_filter:
        cube_links = cube_links.filter(join_identifier=join_identifier_filter)

    frameworks = sorted({
        _detect_framework_from_cube_id(cube_link.foreign_cube_id.cube_id)
        for cube_link in cube_links
        if cube_link.foreign_cube_id and _detect_framework_from_cube_id(cube_link.foreign_cube_id.cube_id)
    })

    for framework in frameworks:
        main_category_finder.create_report_to_main_category_maps(
            context,
            sdd_context,
            framework,
            FRAMEWORK_VERSION_MAP[framework],
        )
    context, sdd_context = joins_creator.do_stuff_and_prepare_context(context, sdd_context)

    if len(sdd_context.bird_cube_structure_item_dictionary) == 0:
        for cube_structure_item in CUBE_STRUCTURE_ITEM.objects.select_related('cube_structure_id', 'variable_id'):
            cube_structure = cube_structure_item.cube_structure_id
            if not cube_structure:
                continue
            cube_structure_key = cube_structure.cube_structure_id
            sdd_context.bird_cube_structure_item_dictionary.setdefault(cube_structure_key, []).append(cube_structure_item)

    if len(sdd_context.cube_structure_item_link_to_cube_link_map) == 0:
        for link in CUBE_STRUCTURE_ITEM_LINK.objects.select_related(
            'cube_link_id',
            'foreign_cube_variable_code__variable_id',
            'primary_cube_variable_code__variable_id',
        ):
            if not link.cube_link_id:
                continue
            sdd_context.cube_structure_item_link_to_cube_link_map.setdefault(
                link.cube_link_id.cube_link_id, []
            ).append(link)

    grouped_cube_links = defaultdict(list)
    for cube_link in cube_links:
        foreign_cube = cube_link.foreign_cube_id
        if not foreign_cube:
            continue
        grouped_cube_links[(foreign_cube.cube_id, cube_link.join_identifier)].append(cube_link)

    gaps = []
    for (foreign_cube_id, join_identifier), related_cube_links in grouped_cube_links.items():
        foreign_cube = related_cube_links[0].foreign_cube_id
        framework = _detect_framework_from_cube_id(foreign_cube.cube_id)
        category = _extract_main_category(related_cube_links[0])
        if not framework or not category:
            continue

        output_structure_key = foreign_cube.cube_id + "_cube_structure"
        if framework == "COREP_REF":
            output_structure_key = foreign_cube.cube_id[0:-5] + "_cube_structure"

        output_items = sdd_context.bird_cube_structure_item_dictionary.get(output_structure_key, [])
        if not output_items:
            continue

        existing_foreign_item_ids = {
            link.foreign_cube_variable_code_id
            for cube_link in related_cube_links
            for link in sdd_context.cube_structure_item_link_to_cube_link_map.get(cube_link.cube_link_id, [])
            if link.foreign_cube_variable_code_id
        }

        missing_items = []
        for output_item in output_items:
            if not output_item.variable_id or not output_item.variable_id.variable_id:
                continue

            variable_id_str = output_item.variable_id.variable_id
            operation_exists = joins_creator.operation_exists_in_cell_for_report_with_category(
                context,
                sdd_context,
                output_item,
                category,
                foreign_cube.cube_id,
            )
            in_facetted_items = variable_id_str in context.facetted_items

            if not (operation_exists or in_facetted_items):
                continue

            if output_item.pk not in existing_foreign_item_ids:
                missing_items.append({
                    'variable_id': variable_id_str,
                    'description': output_item.description,
                    'is_mandatory': output_item.is_mandatory,
                    'order': output_item.order,
                })

        if missing_items:
            gaps.append({
                'foreign_cube_id': foreign_cube_id,
                'join_identifier': join_identifier,
                'main_category': category,
                'cube_links': related_cube_links,
                'missing_items': sorted(missing_items, key=lambda item: (item['order'] is None, item['order'], item['variable_id'])),
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
        'join_identifiers': CUBE_LINK.objects.values_list('join_identifier', flat=True).distinct(),
        'selected_foreign_cube': foreign_cube_filter,
        'selected_primary_cube': primary_cube_filter,
        'selected_join_identifier': join_identifier_filter,
    }

    return render(request, 'pybirdai/reports/validation/show_gaps.html', context)


def _detect_framework_from_cube_id(cube_id):
    if "_REF_FINREP_" in cube_id:
        return "FINREP_REF"
    if "_REF_AE" in cube_id:
        return "AE_REF"
    if "COREP" in cube_id:
        return "COREP_REF"
    return None


def _extract_main_category(cube_link):
    description = cube_link.description or ""
    parts = description.split(":", 3)
    if len(parts) >= 2:
        return parts[1]
    return None


def return_cubelink_visualisation(request):
    """Generate cube link visualizations."""
    from pybirdai.views.core import visualisation_service

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
