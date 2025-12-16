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
CRUD views for member mapping operations.
"""
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from django.core.paginator import Paginator
from django.db import transaction

from pybirdai.models.bird_meta_data_model import (
    MEMBER_MAPPING, MEMBER_MAPPING_ITEM, MEMBER, VARIABLE,
    MAINTENANCE_AGENCY, MEMBER_HIERARCHY
)
from .view_helpers import delete_item


def edit_member_mappings(request):
    """Paginated edit view for member mappings."""
    # Get all maintenance agencies for the create form
    maintenance_agencies = MAINTENANCE_AGENCY.objects.all().order_by('name')

    # Get paginated formset
    page_number = request.GET.get('page', 1)
    all_items = MEMBER_MAPPING.objects.all().order_by('member_mapping_id')
    paginator = Paginator(all_items, 20)
    page_obj = paginator.get_page(page_number)

    ModelFormSet = modelformset_factory(MEMBER_MAPPING, fields='__all__', extra=0)

    if request.method == 'POST':
        formset = ModelFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'MEMBER_MAPPING updated successfully.')
            return redirect(request.path)
        else:
            messages.error(request, 'There was an error updating the MEMBER_MAPPING.')
    else:
        formset = ModelFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'maintenance_agencies': maintenance_agencies,
    }
    return render(request, 'pybirdai/miscellaneous/edit_member_mappings.html', context)


def edit_member_mapping_items(request):
    """Paginated edit view with filters for member mapping items."""
    # Get unique values for filters
    member_mappings = MEMBER_MAPPING_ITEM.objects.values_list('member_mapping_id', flat=True).distinct()
    members = MEMBER_MAPPING_ITEM.objects.values_list('member_id', flat=True).distinct()
    variables = MEMBER_MAPPING_ITEM.objects.values_list('variable_id', flat=True).distinct()

    # Get all available choices for dropdowns
    all_member_mappings = MEMBER_MAPPING.objects.all().order_by('member_mapping_id')
    all_members = MEMBER.objects.all().order_by('member_id')
    all_variables = VARIABLE.objects.all().order_by('variable_id')
    all_member_hierarchies = MEMBER_HIERARCHY.objects.all().order_by('member_hierarchy_id')

    # Get filter values from request
    selected_member_mapping = request.GET.get('member_mapping_id', '')
    selected_member = request.GET.get('member_id', '')
    selected_variable = request.GET.get('variable_id', '')
    selected_is_source = request.GET.get('is_source', '')

    # Apply filters
    queryset = MEMBER_MAPPING_ITEM.objects.all().order_by('id')
    if selected_member_mapping:
        queryset = queryset.filter(member_mapping_id=selected_member_mapping)
    if selected_member:
        queryset = queryset.filter(member_id=selected_member)
    if selected_variable:
        queryset = queryset.filter(variable_id=selected_variable)
    if selected_is_source:
        # Handle both lowercase and uppercase boolean strings
        if selected_is_source.lower() == 'true':
            queryset = queryset.filter(is_source__in=['true', 'True'])
        else:
            queryset = queryset.filter(is_source__in=['false', 'False'])

    # Add pagination and formset creation
    page_number = request.GET.get('page', 1)
    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(page_number)

    ModelFormSet = modelformset_factory(MEMBER_MAPPING_ITEM, fields='__all__', extra=0)

    if request.method == 'POST':
        formset = ModelFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Member Mapping Items updated successfully.')
            return redirect(request.path)
        else:
            messages.error(request, 'There was an error updating the Member Mapping Items.')
    else:
        formset = ModelFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'member_mappings': member_mappings,
        'members': members,
        'variables': variables,
        'selected_member_mapping': selected_member_mapping,
        'selected_member': selected_member,
        'selected_variable': selected_variable,
        'selected_is_source': selected_is_source,
        'all_member_mappings': all_member_mappings,
        'all_members': all_members,
        'all_variables': all_variables,
        'all_member_hierarchies': all_member_hierarchies,
    }
    return render(request, 'pybirdai/miscellaneous/edit_member_mapping_items.html', context)


def create_member_mapping(request):
    """Create new member mapping."""
    if request.method == 'POST':
        try:
            maintenance_agency = None
            if request.POST.get('maintenance_agency_id'):
                maintenance_agency = get_object_or_404(MAINTENANCE_AGENCY, maintenance_agency_id=request.POST.get('maintenance_agency_id'))

            member_mapping = MEMBER_MAPPING(
                name=request.POST.get('name'),
                code=request.POST.get('code'),
                maintenance_agency_id=maintenance_agency,
                member_mapping_id=request.POST.get('member_mapping_id')
            )
            member_mapping.save()

            messages.success(request, 'Member Mapping created successfully.')
        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            SecureErrorHandler.secure_message(request, e, 'member mapping creation')

    return redirect('pybirdai:edit_member_mappings')


def add_member_mapping_item(request):
    """Create new member mapping item."""
    if request.method == 'POST':
        try:
            # Extract data from POST request
            is_source = request.POST.get('is_source', '').lower()  # Convert to lowercase for consistency
            member_id = request.POST.get('member_id')
            variable_id = request.POST.get('variable_id')
            member_mapping_row = request.POST.get('member_mapping_row')
            member_mapping_id = request.POST.get('member_mapping_id')
            member_hierarchy_id = request.POST.get('member_hierarchy')
            valid_from = request.POST.get('valid_from') or None
            valid_to = request.POST.get('valid_to') or None

            # Get related objects
            member = get_object_or_404(MEMBER, member_id=member_id) if member_id else None
            variable = get_object_or_404(VARIABLE, variable_id=variable_id) if variable_id else None
            member_mapping = get_object_or_404(MEMBER_MAPPING, member_mapping_id=member_mapping_id) if member_mapping_id else None
            member_hierarchy = get_object_or_404(MEMBER_HIERARCHY, member_hierarchy_id=member_hierarchy_id) if member_hierarchy_id else None

            # Create new member mapping item
            MEMBER_MAPPING_ITEM.objects.create(
                is_source=is_source,
                member_id=member,
                variable_id=variable,
                member_mapping_row=member_mapping_row,
                member_mapping_id=member_mapping,
                member_hierarchy=member_hierarchy,
                valid_from=valid_from,
                valid_to=valid_to
            )

            messages.success(request, 'Member Mapping Item created successfully.')
        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            SecureErrorHandler.secure_message(request, e, 'member mapping item creation')

    return redirect('pybirdai:edit_member_mapping_items')


def delete_member_mapping(request, member_mapping_id):
    """Delete member mapping."""
    return delete_item(request, MEMBER_MAPPING, 'member_mapping_id', 'edit_member_mappings')


def delete_member_mapping_item(request, item_id):
    """Delete member mapping item."""
    if request.method == 'POST':
        try:
            # Get the composite key fields from GET parameters
            member_mapping_id = request.GET.get('member_mapping_id')
            member_id = request.GET.get('member_id')
            variable_id = request.GET.get('variable_id')
            is_source = request.GET.get('is_source')
            member_mapping_row = request.GET.get('member_mapping_row')

            # Get the item using the composite key
            item = get_object_or_404(
                MEMBER_MAPPING_ITEM,
                member_mapping_id=member_mapping_id,
                member_id=member_id,
                variable_id=variable_id,
                is_source=is_source,
                member_mapping_row=member_mapping_row
            )

            item.delete()
            messages.success(request, 'Member Mapping Item deleted successfully.')
        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            SecureErrorHandler.secure_message(request, e, 'MEMBER_MAPPING_ITEM deletion')

    return redirect('pybirdai:edit_member_mapping_items')


def view_member_mapping_items_by_row(request):
    """Display member mapping items organized by row."""
    # Get all member mappings for the dropdown
    member_mappings = MEMBER_MAPPING.objects.all().order_by('member_mapping_id')

    # Get the selected mapping from the query parameters
    selected_mapping = request.GET.get('member_mapping', '')

    items_by_row = {}
    source_variables = set()  # Track source variables
    target_variables = set()  # Track target variables

    if selected_mapping:
        # Get all items for the selected mapping
        items = MEMBER_MAPPING_ITEM.objects.filter(member_mapping_id=selected_mapping)

        # First pass: collect variables and organize items by row
        for item in items:
            row = item.member_mapping_row
            if row not in items_by_row:
                items_by_row[row] = {'items': {}}

            # Add item to the row dictionary, using variable as key
            if item.variable_id:
                var_id = item.variable_id.variable_id
                items_by_row[row]['items'][var_id] = item

                # Track whether this variable is used as source or target
                if item.is_source.lower() == 'true':
                    source_variables.add(var_id)
                else:
                    target_variables.add(var_id)

    # Convert to sorted lists - source variables first, then target variables
    source_variables = sorted(list(source_variables))
    target_variables = sorted(list(target_variables))

    # Convert items_by_row to a sorted list of tuples based on numeric row value
    sorted_items = sorted(items_by_row.items(), key=lambda x: int(x[0]))
    items_by_row = dict(sorted_items)

    context = {
        'member_mappings': member_mappings,
        'selected_mapping': selected_mapping,
        'items_by_row': items_by_row,
        'source_variables': source_variables,
        'target_variables': target_variables,
    }

    return render(request, 'pybirdai/miscellaneous/view_member_mapping_items_by_row.html', context)
