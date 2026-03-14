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
CRUD views for combination and output layer operations.
"""
from urllib.parse import unquote
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from django.core.paginator import Paginator
from django.db import transaction

from pybirdai.models.bird_meta_data_model import (
    COMBINATION, COMBINATION_ITEM, CUBE
)
from .view_helpers import paginated_modelformset_view


def combinations(request):
    """Paginated edit view for combinations."""
    return paginated_modelformset_view(request, COMBINATION, 'pybirdai/miscellaneous/combinations.html', order_by='combination_id')


def combination_items(request):
    """Paginated edit view with filters for combination items."""
    # Get unique values for filters
    unique_combinations = COMBINATION_ITEM.objects.values_list('combination_id', flat=True).distinct()
    unique_member_ids = COMBINATION_ITEM.objects.values_list('member_id', flat=True).distinct()
    unique_variable_ids = COMBINATION_ITEM.objects.values_list('variable_id', flat=True).distinct()

    # Get all combinations for the create form
    all_combinations = COMBINATION.objects.all().order_by('combination_id')

    # Get filter values from request
    selected_combination = request.GET.get('combination_id', '')
    selected_member = request.GET.get('member_id', '')
    selected_variable = request.GET.get('variable_id', '')

    # Apply filters and ordering
    queryset = COMBINATION_ITEM.objects.all().order_by('id')
    if selected_combination:
        queryset = queryset.filter(combination_id=selected_combination)
    if selected_member:
        queryset = queryset.filter(member_id=selected_member)
    if selected_variable:
        queryset = queryset.filter(variable_id=selected_variable)

    # Add pagination and formset creation
    page_number = request.GET.get('page', 1)
    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(page_number)

    ModelFormSet = modelformset_factory(COMBINATION_ITEM, fields='__all__', extra=0)

    if request.method == 'POST':
        formset = ModelFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'COMBINATION_ITEM updated successfully.')
            return redirect(request.path)
        else:
            messages.error(request, 'There was an error updating the COMBINATION_ITEM.')
    else:
        formset = ModelFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'unique_combinations': unique_combinations,
        'unique_member_ids': unique_member_ids,
        'unique_variable_ids': unique_variable_ids,
        'selected_combination': selected_combination,
        'selected_member': selected_member,
        'selected_variable': selected_variable,
        'all_combinations': all_combinations,
    }
    return render(request, 'pybirdai/miscellaneous/combination_items.html', context)


def output_layers(request):
    """Paginated edit view for output layers (RC cubes)."""
    # Output layers are identified by cube_type='RC'; cube IDs are framework/table based.
    page_number = request.GET.get('page', 1)
    queryset = CUBE.objects.filter(cube_type='RC').order_by('cube_id')
    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(page_number)

    ModelFormSet = modelformset_factory(CUBE, fields='__all__', extra=0)

    if request.method == 'POST':
        formset = ModelFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Output Layers updated successfully.')
            return redirect(request.path)
        else:
            messages.error(request, 'There was an error updating the Output Layers.')
    else:
        formset = ModelFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'pybirdai/miscellaneous/output_layers.html', context)


def delete_combination(request, combination_id):
    """Delete combination."""
    try:
        item = get_object_or_404(COMBINATION, combination_id=combination_id)
        item.delete()
        messages.success(request, 'COMBINATION deleted successfully.')
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        SecureErrorHandler.secure_message(request, e, 'COMBINATION deletion')
    return redirect('pybirdai:edit_combinations')


def delete_combination_item(request, item_id):
    """Delete combination item."""
    try:
        # Get the combination_id and member_id from the POST data
        combination_id = request.POST.get('combination_id')
        member_id = request.POST.get('member_id')

        if not all([combination_id, member_id]):
            raise ValueError("Missing required fields for deletion")

        # Get the item using the composite key fields
        item = COMBINATION_ITEM.objects.get(
            combination_id=COMBINATION.objects.get(combination_id=combination_id),
            member_id=member_id
        )
        item.delete()
        messages.success(request, 'COMBINATION_ITEM deleted successfully.')
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        SecureErrorHandler.secure_message(request, e, 'COMBINATION_ITEM deletion')
    return redirect('pybirdai:edit_combination_items')


def delete_cube(request, cube_id):
    """Delete cube with URL decoding."""
    try:
        decoded_cube_id = unquote(cube_id)
        item = get_object_or_404(CUBE, cube_id=decoded_cube_id)
        item.delete()
        messages.success(request, 'CUBE deleted successfully.')
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        SecureErrorHandler.secure_message(request, e, 'CUBE deletion')
    return redirect('pybirdai:edit_output_layers')
