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
CRUD views for cube link operations.
"""
import logging
from django.http import JsonResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from django.core.paginator import Paginator
from django.urls import reverse
from django.views.decorators.http import require_http_methods
from django.views.decorators.clickjacking import xframe_options_exempt

from pybirdai.models.bird_meta_data_model import (
    CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK, CUBE, CUBE_STRUCTURE_ITEM, MEMBER_LINK
)
from .cache_manager import (
    remove_cube_link_from_cache,
    add_cube_link_to_cache,
    remove_cube_structure_item_link_from_cache,
    add_cube_structure_item_link_to_cache
)

logger = logging.getLogger(__name__)


@xframe_options_exempt
def edit_cube_links(request):
    """Paginated edit view for cube links with filters."""
    # Get unique values for filters
    foreign_cubes = CUBE_LINK.objects.values_list('foreign_cube_id', flat=True).distinct()
    join_identifiers = CUBE_LINK.objects.values_list('join_identifier', flat=True).distinct()

    # Get all cubes for the add form
    all_cubes = CUBE.objects.all().order_by('cube_id')

    # Get filter values from request
    selected_foreign_cube = request.GET.get('foreign_cube', '')
    selected_identifier = request.GET.get('join_identifier', '')

    # Apply filters and ordering
    queryset = CUBE_LINK.objects.all().order_by('cube_link_id')
    if selected_foreign_cube:
        queryset = queryset.filter(foreign_cube_id=selected_foreign_cube)
    if selected_identifier:
        queryset = queryset.filter(join_identifier=selected_identifier)

    # Add pagination and formset creation
    page_number = request.GET.get('page', 1)
    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(page_number)

    ModelFormSet = modelformset_factory(CUBE_LINK, fields='__all__', extra=0)
    formset = ModelFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'foreign_cubes': foreign_cubes,
        'join_identifiers': join_identifiers,
        'selected_foreign_cube': selected_foreign_cube,
        'selected_identifier': selected_identifier,
        'all_cubes': all_cubes,
    }
    return render(request, 'pybirdai/miscellaneous/edit_cube_links.html', context)


def edit_cube_structure_item_links(request):
    """Edit cube structure item links with dynamic filtering."""
    # Get unique values for dropdowns
    queryset = CUBE_STRUCTURE_ITEM_LINK.objects.all().order_by('cube_structure_item_link_id')
    unique_cube_links = CUBE_LINK.objects.values_list('cube_link_id', flat=True).distinct()

    # Get filter values from request
    selected_cube_link = request.GET.get('cube_link', '')

    # Apply filters
    if selected_cube_link:
        queryset = queryset.filter(cube_link_id=selected_cube_link)
        # Get the selected cube link object to access foreign and primary cubes
        cube_link = CUBE_LINK.objects.get(cube_link_id=selected_cube_link)
        # Get cube structure items for foreign and primary cubes
        foreign_cube_items = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id=cube_link.foreign_cube_id.cube_structure_id
        ).order_by('variable_id')
        primary_cube_items = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id=cube_link.primary_cube_id.cube_structure_id
        ).order_by('variable_id')
    else:
        foreign_cube_items = []
        primary_cube_items = []

    # Add pagination and formset creation
    page_number = request.GET.get('page', 1)
    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(page_number)

    ModelFormSet = modelformset_factory(CUBE_STRUCTURE_ITEM_LINK, fields='__all__', extra=0)
    formset = ModelFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'unique_cube_links': unique_cube_links,
        'foreign_cube_items': foreign_cube_items,
        'primary_cube_items': primary_cube_items,
        'selected_cube_link': selected_cube_link,
    }

    return render(request, 'pybirdai/miscellaneous/edit_cube_structure_item_links.html', context)


def delete_cube_link(request, cube_link_id):
    """Delete cube link with cascading deletion of child records and cache updates."""
    try:
        link = get_object_or_404(CUBE_LINK, cube_link_id=cube_link_id)

        # Find all child CUBE_STRUCTURE_ITEM_LINKs
        child_links = CUBE_STRUCTURE_ITEM_LINK.objects.filter(cube_link_id=cube_link_id)
        child_link_ids = list(child_links.values_list('cube_structure_item_link_id', flat=True))

        # Delete all grandchild MEMBER_LINKs for each child
        member_links_deleted = 0
        for child_link_id in child_link_ids:
            deleted_count, _ = MEMBER_LINK.objects.filter(
                cube_structure_item_link_id=child_link_id
            ).delete()
            member_links_deleted += deleted_count

        # Update cache for each child CUBE_STRUCTURE_ITEM_LINK before deletion
        for child_link_id in child_link_ids:
            remove_cube_structure_item_link_from_cache(child_link_id, cube_link_id)

        # Delete all child CUBE_STRUCTURE_ITEM_LINKs
        structure_links_deleted, _ = child_links.delete()

        # Update the in-memory dictionaries for parent
        remove_cube_link_from_cache(cube_link_id)

        # Delete the parent CUBE_LINK
        link.delete()

        message = f'CUBE_LINK deleted successfully (also deleted {structure_links_deleted} structure item link(s) and {member_links_deleted} member link(s)).'
        messages.success(request, message)
        return JsonResponse({'status': 'success', 'message': message})
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        SecureErrorHandler.secure_message(request, e, "CUBE_LINK deletion")
        return SecureErrorHandler.secure_json_response(e, "CUBE_LINK deletion", request)


@require_http_methods(["POST"])
def delete_cube_structure_item_link(request, cube_structure_item_link_id, from_duplicate_list=False):
    """
    Delete cube structure item link with cascading deletion of member links and cache updates.

    Args:
        request: HTTP request
        cube_structure_item_link_id: ID of the link to delete
        from_duplicate_list: If True, redirect to duplicate list with preserved filters
    """
    # Check if this is an AJAX request (from embed iframe)
    is_ajax = (
        request.headers.get('X-Requested-With') == 'XMLHttpRequest' or
        request.content_type == 'application/json'
    )

    try:
        link = get_object_or_404(CUBE_STRUCTURE_ITEM_LINK, cube_structure_item_link_id=cube_structure_item_link_id)
        # Store the cube_link_id before deleting
        cube_link_id = link.cube_link_id.cube_link_id if link.cube_link_id else None

        # Delete all child MEMBER_LINKs first
        member_links_deleted, _ = MEMBER_LINK.objects.filter(
            cube_structure_item_link_id=cube_structure_item_link_id
        ).delete()

        # Delete the CUBE_STRUCTURE_ITEM_LINK
        link.delete()

        # Update the in-memory dictionaries
        remove_cube_structure_item_link_from_cache(cube_structure_item_link_id, cube_link_id)

        if member_links_deleted > 0:
            message = f'Link deleted successfully (also deleted {member_links_deleted} member link(s)).'
        else:
            message = 'Link deleted successfully.'

        # Return JSON for AJAX requests, redirect for regular form submissions
        if is_ajax:
            return JsonResponse({'status': 'success', 'message': message})

        messages.success(request, message)
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        if is_ajax:
            return JsonResponse({'status': 'error', 'message': str(e)}, status=400)
        SecureErrorHandler.secure_message(request, e, 'cube structure item link deletion')

    # Check the referer to determine which page to redirect back to
    referer = request.META.get('HTTP_REFERER', '')
    if 'edit-cube-structure-item-links' in referer:
        redirect_url = reverse('pybirdai:edit_cube_structure_item_links')
    else:
        # Preserve the filter parameters in the redirect for duplicate_primary_member_id_list
        params = request.GET.copy()
        params.pop('page', None)  # Remove page parameter to avoid invalid page numbers
        redirect_url = reverse('pybirdai:duplicate_primary_member_id_list')
        if params:
            redirect_url += f'?{params.urlencode()}'

    return redirect(redirect_url)


# Keep a reference to the original function name for backward compatibility
def delete_cube_structure_item_link_dupl(request, cube_structure_item_link_id):
    """Backward compatible wrapper - redirects to duplicate list."""
    return delete_cube_structure_item_link(request, cube_structure_item_link_id, from_duplicate_list=True)


@require_http_methods(["POST"])
def bulk_delete_cube_structure_item_links(request):
    """Bulk deletion with cascading deletion of member links and in-memory cache updates."""
    logger.info("Received request to bulk delete CUBE_STRUCTURE_ITEM_LINK items.")

    selected_ids = request.POST.getlist('selected_items')

    if not selected_ids:
        logger.warning("No items selected for bulk deletion.")
        messages.warning(request, "No items selected for deletion.")
        return redirect('pybirdai:duplicate_primary_member_id_list')

    logger.debug(f"Selected IDs for deletion: {selected_ids}")

    try:
        # Fetch the links before deleting to get related cube_link_ids
        logger.debug(f"Fetching {len(selected_ids)} CUBE_STRUCTURE_ITEM_LINK objects for deletion.")
        links_to_delete = CUBE_STRUCTURE_ITEM_LINK.objects.filter(
            cube_structure_item_link_id__in=selected_ids
        ).select_related('cube_link_id')
        logger.debug(f"Fetched {links_to_delete.count()} objects.")

        # Store info for cache update before deleting
        link_info = [
            (link.cube_structure_item_link_id, link.cube_link_id.cube_link_id if link.cube_link_id else None)
            for link in links_to_delete
        ]

        # Delete all child MEMBER_LINKs first (cascading delete)
        logger.info("Deleting child MEMBER_LINK records first (cascading delete).")
        member_links_deleted, _ = MEMBER_LINK.objects.filter(
            cube_structure_item_link_id__in=selected_ids
        ).delete()
        logger.info(f"Deleted {member_links_deleted} child MEMBER_LINK record(s).")

        logger.info("Starting bulk deletion of CUBE_STRUCTURE_ITEM_LINK objects from database.")
        deleted_count, _ = links_to_delete.delete()
        logger.info(f"Database deletion complete. Deleted {deleted_count} link(s).")

        logger.info("Updating in-memory SDDContext dictionaries.")
        # Update the in-memory dictionaries for each deleted link
        for cube_structure_item_link_id, cube_link_id in link_info:
            remove_cube_structure_item_link_from_cache(cube_structure_item_link_id, cube_link_id)

        if member_links_deleted > 0:
            messages.success(request, f"{deleted_count} link(s) deleted successfully (also deleted {member_links_deleted} member link(s)).")
        else:
            messages.success(request, f"{deleted_count} link(s) deleted successfully.")
        logger.info(f"Bulk deletion process completed successfully. {deleted_count} link(s) and {member_links_deleted} member link(s) deleted.")
    except Exception as e:
        logger.error(f'Error during bulk deletion: {str(e)}', exc_info=True)
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        SecureErrorHandler.secure_message(request, e, 'bulk deletion')

    # Redirect back to the duplicate list page, preserving filters
    logger.info("Redirecting back to the duplicate primary member ID list page.")
    params = request.GET.copy()
    redirect_url = reverse('pybirdai:duplicate_primary_member_id_list')
    page = params.get('page', 1)
    redirect_url += f'?page={page}'

    # Append foreign_cube filter if present
    foreign_cube = params.get('foreign_cube')
    if foreign_cube:
        redirect_url += f'&foreign_cube={foreign_cube}'

    # Append primary_cube filter if present
    primary_cube = params.get('primary_cube')
    if primary_cube:
        redirect_url += f'&primary_cube={primary_cube}'

    return redirect(redirect_url)


@require_http_methods(["POST"])
def add_cube_structure_item_link(request):
    """Create new cube structure item link."""
    try:
        # Get the user-provided ID
        cube_structure_item_link_id = request.POST['cube_structure_item_link_id']

        # Get the CUBE_LINK instance
        cube_link = get_object_or_404(CUBE_LINK, cube_link_id=request.POST['cube_link_id'])

        # Get the CUBE_STRUCTURE_ITEM instances
        foreign_cube_variable = get_object_or_404(CUBE_STRUCTURE_ITEM, id=request.POST['foreign_cube_variable_code'])
        primary_cube_variable = get_object_or_404(CUBE_STRUCTURE_ITEM, id=request.POST['primary_cube_variable_code'])

        # Create the new link with the user-provided ID
        new_link = CUBE_STRUCTURE_ITEM_LINK.objects.create(
            cube_structure_item_link_id=cube_structure_item_link_id,
            cube_link_id=cube_link,
            foreign_cube_variable_code=foreign_cube_variable,
            primary_cube_variable_code=primary_cube_variable
        )

        # Update the in-memory dictionaries
        add_cube_structure_item_link_to_cache(new_link, cube_link.cube_link_id)

        messages.success(request, 'New cube structure item link created successfully.')
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        SecureErrorHandler.secure_message(request, e, 'cube structure item link creation')

    return redirect('pybirdai:edit_cube_structure_item_links')


@require_http_methods(["POST"])
def add_cube_link(request):
    """Create new cube link."""
    try:
        # Get the cube instances
        primary_cube = get_object_or_404(CUBE, cube_id=request.POST['primary_cube_id'])
        foreign_cube = get_object_or_404(CUBE, cube_id=request.POST['foreign_cube_id'])

        # Create the new cube link
        new_link = CUBE_LINK.objects.create(
            cube_link_id=request.POST['cube_link_id'],
            code=request.POST.get('code'),
            name=request.POST.get('name'),
            description=request.POST.get('description'),
            order_relevance=request.POST.get('order_relevance'),
            primary_cube_id=primary_cube,
            foreign_cube_id=foreign_cube,
            cube_link_type=request.POST.get('cube_link_type'),
            join_identifier=request.POST.get('join_identifier')
        )

        # Update the in-memory dictionaries
        add_cube_link_to_cache(new_link)

        messages.success(request, 'New cube link created successfully.')
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        SecureErrorHandler.secure_message(request, e, 'cube link creation')

    return redirect('pybirdai:edit_cube_links')
