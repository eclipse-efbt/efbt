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
CRUD views for variable mapping operations.
"""
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from django.core.paginator import Paginator
from django.db import transaction

from pybirdai.models.bird_meta_data_model import (
    VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM, VARIABLE, MAINTENANCE_AGENCY
)
from .view_helpers import delete_item, redirect_with_allowed_query_params


def edit_variable_mappings(request):
    """Paginated edit view for variable mappings."""
    # Get all maintenance agencies for the create form
    maintenance_agencies = MAINTENANCE_AGENCY.objects.all().order_by('name')

    # Get paginated formset
    page_number = request.GET.get('page', 1)
    all_items = VARIABLE_MAPPING.objects.all().order_by('variable_mapping_id')
    paginator = Paginator(all_items, 20)
    page_obj = paginator.get_page(page_number)

    ModelFormSet = modelformset_factory(VARIABLE_MAPPING, fields='__all__', extra=0)

    if request.method == 'POST':
        formset = ModelFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Variable Mappings updated successfully.')
            return redirect_with_allowed_query_params(request, 'pybirdai:edit_variable_mappings', ('page',))
        else:
            messages.error(request, 'There was an error updating the Variable Mappings.')
    else:
        formset = ModelFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'maintenance_agencies': maintenance_agencies,
    }
    return render(request, 'pybirdai/miscellaneous/edit_variable_mappings.html', context)


def edit_variable_mapping_items(request):
    """Paginated edit view for variable mapping items with filters."""
    # Get unique values for filters
    unique_variable_mappings = VARIABLE_MAPPING_ITEM.objects.values_list('variable_mapping_id', flat=True).distinct()
    unique_variables = VARIABLE_MAPPING_ITEM.objects.values_list('variable_id', flat=True).distinct()

    # Get all variable mappings and variables for the create form
    all_variable_mappings = VARIABLE_MAPPING.objects.all().order_by('variable_mapping_id')
    all_variables = VARIABLE.objects.all().order_by('variable_id')

    # Get filter values from request
    selected_variable_mapping = request.GET.get('variable_mapping_id', '')
    selected_variable = request.GET.get('variable_id', '')

    # Apply filters and ordering
    queryset = VARIABLE_MAPPING_ITEM.objects.all().order_by('id')
    if selected_variable_mapping:
        queryset = queryset.filter(variable_mapping_id=selected_variable_mapping)
    if selected_variable:
        queryset = queryset.filter(variable_id=selected_variable)

    # Add pagination and formset creation
    page_number = request.GET.get('page', 1)
    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(page_number)

    ModelFormSet = modelformset_factory(VARIABLE_MAPPING_ITEM, fields='__all__', extra=0)
    formset = ModelFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'unique_variable_mappings': unique_variable_mappings,
        'unique_variables': unique_variables,
        'selected_variable_mapping': selected_variable_mapping,
        'selected_variable': selected_variable,
        'all_variable_mappings': all_variable_mappings,
        'all_variables': all_variables,
    }
    return render(request, 'pybirdai/miscellaneous/edit_variable_mapping_items.html', context)


def create_variable_mapping_item(request):
    """Create new variable mapping item."""
    if request.method == 'POST':
        try:
            # Get form data
            variable_mapping = get_object_or_404(VARIABLE_MAPPING, variable_mapping_id=request.POST.get('variable_mapping_id'))
            variable = get_object_or_404(VARIABLE, variable_id=request.POST.get('variable_id'))

            # Create new item
            item = VARIABLE_MAPPING_ITEM(
                variable_mapping_id=variable_mapping,
                is_source=request.POST.get('is_source'),
                variable_id=variable,
                valid_from=request.POST.get('valid_from') or None,
                valid_to=request.POST.get('valid_to') or None
            )
            item.save()

            messages.success(request, 'Variable Mapping Item created successfully.')
        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            SecureErrorHandler.secure_message(request, e, "Variable Mapping Item creation")

    return redirect('pybirdai:edit_variable_mapping_items')


def create_variable_mapping(request):
    """Create new variable mapping."""
    if request.method == 'POST':
        try:
            # Create new variable mapping
            variable_mapping = VARIABLE_MAPPING(
                name=request.POST.get('name'),
                code=request.POST.get('code'),
                variable_mapping_id=request.POST.get('variable_mapping_id'),
                maintenance_agency_id=MAINTENANCE_AGENCY.objects.get(
                    maintenance_agency_id=request.POST.get('maintenance_agency_id')
                )
            )
            variable_mapping.save()
            messages.success(request, 'Variable mapping created successfully.')
        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            SecureErrorHandler.secure_message(request, e, 'variable mapping creation')
    return redirect('pybirdai:edit_variable_mappings')


def delete_variable_mapping(request, variable_mapping_id):
    """Delete variable mapping."""
    return delete_item(request, VARIABLE_MAPPING, 'variable_mapping_id', 'edit_variable_mappings', variable_mapping_id)


def delete_variable_mapping_item(request):
    """Delete variable mapping item."""
    if request.method == 'POST':
        try:
            variable_mapping_id = request.GET.get('variable_mapping_id')
            variable_id = request.GET.get('variable_id')
            is_source = request.GET.get('is_source')

            # Get the item using the composite key
            item = get_object_or_404(
                VARIABLE_MAPPING_ITEM,
                variable_mapping_id=variable_mapping_id,
                variable_id=variable_id,
                is_source=is_source
            )

            item.delete()
            messages.success(request, 'Variable Mapping Item deleted successfully.')
        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            SecureErrorHandler.secure_message(request, e, 'Variable Mapping Item deletion')

    return redirect('pybirdai:edit_variable_mapping_items')
