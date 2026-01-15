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

from django.db import transaction

from pybirdai.models.bird_meta_data_model import (
    MAPPING_DEFINITION, MAPPING_TO_CUBE, MAINTENANCE_AGENCY,
    MEMBER_MAPPING, VARIABLE_MAPPING, FRAMEWORK, CUBE
)
from pybirdai.context.sdd_context_django import SDDContext
from .view_helpers import delete_item


def get_mapping_definitions_for_framework(framework_id):
    """
    Get mapping definitions filtered by framework.
    Flow: Framework -> Cubes -> cube codes -> MAPPING_TO_CUBE -> MAPPING_DEFINITION
    """
    # Get cubes for this framework
    cubes = CUBE.objects.filter(framework_id=framework_id)
    cube_codes = [cube.code for cube in cubes if cube.code]

    if not cube_codes:
        return MAPPING_DEFINITION.objects.none()

    # Get mapping_ids where cube_mapping_id matches cube codes
    mapping_to_cubes = MAPPING_TO_CUBE.objects.filter(
        cube_mapping_id__in=cube_codes
    )
    mapping_ids = [mtc.mapping_id_id for mtc in mapping_to_cubes if mtc.mapping_id_id]

    if not mapping_ids:
        return MAPPING_DEFINITION.objects.none()

    return MAPPING_DEFINITION.objects.filter(mapping_id__in=mapping_ids)


def edit_mapping_definitions(request):
    """Paginated edit view for mapping definitions with optional framework filter."""
    # Get all maintenance agencies for the create form
    maintenance_agencies = MAINTENANCE_AGENCY.objects.all().order_by('name')

    # Get all member mappings and variable mappings for dropdowns
    member_mappings = MEMBER_MAPPING.objects.all().order_by('name')
    variable_mappings = VARIABLE_MAPPING.objects.all().order_by('name')

    # Get all frameworks for the dropdown
    frameworks = FRAMEWORK.objects.all().order_by('name')

    # Get filter value from request
    selected_framework = request.GET.get('framework', '')

    # Apply framework filter if provided
    if selected_framework:
        queryset = get_mapping_definitions_for_framework(selected_framework).order_by('mapping_id')
    else:
        queryset = MAPPING_DEFINITION.objects.all().order_by('mapping_id')

    # Get paginated formset
    page_number = request.GET.get('page', 1)
    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(page_number)

    ModelFormSet = modelformset_factory(MAPPING_DEFINITION, fields='__all__', extra=0)

    if request.method == 'POST':
        formset = ModelFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'MAPPING_DEFINITION updated successfully.')
            return redirect(request.path)
        else:
            messages.error(request, 'There was an error updating the MAPPING_DEFINITION.')
    else:
        formset = ModelFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'maintenance_agencies': maintenance_agencies,
        'member_mappings': member_mappings,
        'variable_mappings': variable_mappings,
        'frameworks': frameworks,
        'selected_framework': selected_framework,
    }
    return render(request, 'pybirdai/miscellaneous/edit_mapping_definitions.html', context)


def edit_mapping_to_cubes(request):
    """Edit mapping to cube relationships with filters including framework."""
    # Get all frameworks for the dropdown
    frameworks = FRAMEWORK.objects.all().order_by('name')

    # Get filter parameters
    selected_framework = request.GET.get('framework', '')
    mapping_filter = request.GET.get('mapping_filter')
    cube_filter = request.GET.get('cube_filter')

    # Start with all objects and order them
    queryset = MAPPING_TO_CUBE.objects.all().order_by('mapping_id__name', 'cube_mapping_id')

    # Apply framework filter if provided
    if selected_framework:
        # Get cubes for this framework
        cubes = CUBE.objects.filter(framework_id=selected_framework)
        cube_codes = [cube.code for cube in cubes if cube.code]
        if cube_codes:
            queryset = queryset.filter(cube_mapping_id__in=cube_codes)
        else:
            queryset = queryset.none()

    # Apply other filters if they exist
    if mapping_filter:
        queryset = queryset.filter(mapping_id__mapping_id=mapping_filter)
    if cube_filter:
        queryset = queryset.filter(cube_mapping_id=cube_filter)

    # Get all mapping definitions and unique cube mappings for the dropdowns
    # Filter these based on framework if selected
    if selected_framework:
        mapping_definitions = get_mapping_definitions_for_framework(selected_framework).order_by('name')
        cubes = CUBE.objects.filter(framework_id=selected_framework)
        cube_codes = [cube.code for cube in cubes if cube.code]
        cube_mappings = (MAPPING_TO_CUBE.objects
                        .filter(cube_mapping_id__in=cube_codes)
                        .values_list('cube_mapping_id', flat=True)
                        .distinct()
                        .order_by('cube_mapping_id'))
    else:
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
        'frameworks': frameworks,
        'selected_framework': selected_framework,
    }

    return render(request, 'pybirdai/miscellaneous/edit_mapping_to_cubes.html', context)


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
