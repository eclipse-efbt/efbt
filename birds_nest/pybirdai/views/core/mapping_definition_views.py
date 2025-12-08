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
CRUD views for mapping definition operations.
"""
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from django.core.paginator import Paginator

from pybirdai.models.bird_meta_data_model import (
    MAPPING_DEFINITION, MAPPING_TO_CUBE, MAINTENANCE_AGENCY,
    MEMBER_MAPPING, VARIABLE_MAPPING
)
from pybirdai.context.sdd_context_django import SDDContext
from .view_helpers import paginated_modelformset_view, delete_item


def edit_mapping_definitions(request):
    """Paginated edit view for mapping definitions."""
    return paginated_modelformset_view(request, MAPPING_DEFINITION, 'pybirdai/edit_mapping_definitions.html', order_by='mapping_id')


def edit_mapping_to_cubes(request):
    """Edit mapping to cube relationships with filters."""
    # Get filter parameters
    mapping_filter = request.GET.get('mapping_filter')
    cube_filter = request.GET.get('cube_filter')

    # Start with all objects and order them
    queryset = MAPPING_TO_CUBE.objects.all().order_by('mapping_id__name', 'cube_mapping_id')

    # Apply filters if they exist
    if mapping_filter:
        queryset = queryset.filter(mapping_id__mapping_id=mapping_filter)
    if cube_filter:
        queryset = queryset.filter(cube_mapping_id=cube_filter)

    # Get all mapping definitions and unique cube mappings for the dropdowns
    mapping_definitions = MAPPING_DEFINITION.objects.all().order_by('name')
    cube_mappings = (MAPPING_TO_CUBE.objects
                    .values_list('cube_mapping_id', flat=True)
                    .distinct()
                    .order_by('cube_mapping_id'))

    # Paginate after filtering
    paginator = Paginator(queryset, 10)  # Show 10 items per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # Create formset for the current page
    MappingToCubeFormSet = modelformset_factory(
        MAPPING_TO_CUBE,
        fields=('mapping_id', 'cube_mapping_id', 'valid_from', 'valid_to'),
        extra=0
    )

    if request.method == 'POST':
        formset = MappingToCubeFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            formset.save()
            messages.success(request, 'Changes saved successfully.')
            return redirect('pybirdai:edit_mapping_to_cubes')
    else:
        formset = MappingToCubeFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'mapping_definitions': mapping_definitions,
        'cube_mappings': cube_mappings,
    }

    return render(request, 'pybirdai/edit_mapping_to_cubes.html', context)


def create_mapping_definition(request):
    """Create new mapping definition."""
    if request.method == 'POST':
        try:
            maintenance_agency = None
            if request.POST.get('maintenance_agency_id'):
                maintenance_agency = get_object_or_404(MAINTENANCE_AGENCY, maintenance_agency_id=request.POST.get('maintenance_agency_id'))

            member_mapping = None
            if request.POST.get('member_mapping_id'):
                member_mapping = get_object_or_404(MEMBER_MAPPING, member_mapping_id=request.POST.get('member_mapping_id'))

            variable_mapping = None
            if request.POST.get('variable_mapping_id'):
                variable_mapping = get_object_or_404(VARIABLE_MAPPING, variable_mapping_id=request.POST.get('variable_mapping_id'))

            mapping_definition = MAPPING_DEFINITION(
                name=request.POST.get('name'),
                code=request.POST.get('code'),
                maintenance_agency_id=maintenance_agency,
                mapping_id=request.POST.get('mapping_id'),
                mapping_type=request.POST.get('mapping_type'),
                member_mapping_id=member_mapping,
                variable_mapping_id=variable_mapping
            )
            mapping_definition.save()

            messages.success(request, 'Mapping Definition created successfully.')
        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            SecureErrorHandler.secure_message(request, e, 'Mapping Definition creation')

    return redirect('pybirdai:edit_mapping_definitions')


def create_mapping_to_cube(request):
    """Create mapping to cube relationship."""
    sdd_context = SDDContext()
    if request.method == 'POST':
        try:
            # Get form data
            mapping_id = request.POST.get('mapping_id')
            cube_mapping = request.POST.get('cube_mapping_id')
            valid_from = request.POST.get('valid_from')
            valid_to = request.POST.get('valid_to')

            # Get the mapping definition object
            mapping = get_object_or_404(MAPPING_DEFINITION, mapping_id=mapping_id)

            # Create new mapping to cube
            mapping_to_cube = MAPPING_TO_CUBE(
                mapping_id=mapping,
                cube_mapping_id=cube_mapping,
                valid_from=valid_from if valid_from else None,
                valid_to=valid_to if valid_to else None
            )
            mapping_to_cube.save()

            # Update cache
            try:
                mapping_to_cube_list = sdd_context.mapping_to_cube_dictionary[
                    mapping_to_cube.cube_mapping_id]
                mapping_to_cube_list.append(mapping_to_cube)
            except KeyError:
                sdd_context.mapping_to_cube_dictionary[
                    mapping_to_cube.cube_mapping_id] = [mapping_to_cube]

            messages.success(request, 'New mapping to cube created successfully.')
        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            SecureErrorHandler.secure_message(request, e, 'mapping to cube creation')

    return redirect('pybirdai:edit_mapping_to_cubes')


def delete_mapping_definition(request, mapping_id):
    """Delete mapping definition."""
    return delete_item(request, MAPPING_DEFINITION, 'mapping_id', 'edit_mapping_definitions')


def delete_mapping_to_cube(request, mapping_to_cube_id):
    """Delete mapping to cube relationship."""
    sdd_context = SDDContext()
    try:
        # Get the mapping_id and cube_mapping_id from the POST data
        mapping_id = request.POST.get('mapping_id')
        cube_mapping_id = request.POST.get('cube_mapping_id')

        if not all([mapping_id, cube_mapping_id]):
            raise ValueError("Missing required fields for deletion")

        # Get the item using the composite key fields
        item = MAPPING_TO_CUBE.objects.get(
            mapping_id=MAPPING_DEFINITION.objects.get(mapping_id=mapping_id),
            cube_mapping_id=cube_mapping_id
        )

        # Update cache
        try:
            cube_mapping_list = sdd_context.mapping_to_cube_dictionary[item.cube_mapping_id]
            for the_item in cube_mapping_list:
                if the_item.mapping_id.mapping_id == mapping_id:
                    cube_mapping_list.remove(the_item)
        except KeyError:
            pass  # Key not in cache

        item.delete()
        messages.success(request, 'MAPPING_TO_CUBE deleted successfully.')
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        SecureErrorHandler.secure_message(request, e, 'MAPPING_TO_CUBE deletion')
    return redirect('pybirdai:edit_mapping_to_cubes')
