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
View helper functions for common patterns.

Includes reusable patterns for pagination, deletion, and serialization.
"""
from datetime import datetime
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from django.core.paginator import Paginator
from django.db import transaction
from pybirdai.models.bird_meta_data_model import (
    MAINTENANCE_AGENCY, MEMBER_MAPPING, VARIABLE_MAPPING
)


def serialize_datetime(obj):
    """JSON serializer for datetime objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def paginated_modelformset_view(request, model, template_name, order_by=None):
    """
    Reusable paginated modelformset view pattern.

    Args:
        request: Django HTTP request
        model: Django model class
        template_name: Template path to render
        order_by: Optional field name for ordering

    Returns:
        Rendered template response
    """
    # Get all maintenance agencies for the create form
    maintenance_agencies = MAINTENANCE_AGENCY.objects.all().order_by('name')

    # Get all member mappings and variable mappings for dropdowns
    member_mappings = MEMBER_MAPPING.objects.all().order_by('name')
    variable_mappings = VARIABLE_MAPPING.objects.all().order_by('name')

    # Get paginated formset
    page_number = request.GET.get('page', 1)
    queryset = model.objects.all()
    if order_by:
        queryset = queryset.order_by(order_by)
    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(page_number)

    ModelFormSet = modelformset_factory(model, fields='__all__', extra=0)

    if request.method == 'POST':
        formset = ModelFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, f'{model.__name__} updated successfully.')
            return redirect(request.path)
        else:
            messages.error(request, f'There was an error updating the {model.__name__}.')
    else:
        formset = ModelFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'maintenance_agencies': maintenance_agencies,
        'member_mappings': member_mappings,
        'variable_mappings': variable_mappings,
    }
    return render(request, template_name, context)


def delete_item(request, model, id_field, redirect_view, decoded_id=None):
    """
    Generic delete helper for model instances.

    Args:
        request: Django HTTP request
        model: Django model class
        id_field: Name of the ID field
        redirect_view: View name to redirect to after deletion
        decoded_id: Pre-decoded ID value (optional)

    Returns:
        Redirect response
    """
    try:
        id_value = decoded_id if decoded_id is not None else request.POST.get('id')
        if id_value is None:
            id_value = request.POST.get(id_field)
        item = get_object_or_404(model, **{id_field: id_value})
        item.delete()
        messages.success(request, f'{model.__name__} deleted successfully.')
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        SecureErrorHandler.secure_message(request, e, f'{model.__name__} deletion')
    return redirect(f'pybirdai:{redirect_view}')
