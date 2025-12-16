"""
Views for managing MEMBER_LINK records.
Provides CRUD operations for member links in the ANCRDT workflow.
"""

from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse
from django.contrib import messages
from django.core.paginator import Paginator
from django.forms import modelformset_factory
from django.utils import timezone
from urllib.parse import unquote
from datetime import datetime
from pybirdai.models.bird_meta_data_model import (
    MEMBER_LINK,
    CUBE_STRUCTURE_ITEM_LINK,
    MEMBER
)


def edit_member_links(request):
    """
    DEPRECATED: This view is no longer used.
    Member links editing has been integrated into the ANCRDT dashboard (Step 2).
    This function redirects to the ANCRDT dashboard for backward compatibility.
    """
    from django.shortcuts import redirect
    from django.urls import reverse
    from django.contrib import messages

    messages.info(request, 'Member links editor has been integrated into the ANCRDT workflow Step 2 review page.')
    return redirect('pybirdai:ancrdt_step_2_review')


def edit_member_links_legacy(request):
    """
    LEGACY: Original view for editing MEMBER_LINK records.
    Kept for reference only. Use the integrated dashboard instead.
    Allows filtering, pagination, and batch editing of member links.
    """
    # Get unique values for filters
    all_member_links = MEMBER_LINK.objects.all().order_by('cube_structure_item_link_id', 'primary_member_id').select_related(
        'cube_structure_item_link_id', 'primary_member_id', 'foreign_member_id')
    primary_members = set(ml.primary_member_id_id for ml in all_member_links)
    foreign_members = set(ml.foreign_member_id_id for ml in all_member_links)
    cube_structure_item_links = set(ml.cube_structure_item_link_id_id for ml in all_member_links)

    # Get all members for the add form
    all_members = MEMBER.objects.all().order_by('member_id')
    all_cube_structure_item_links = CUBE_STRUCTURE_ITEM_LINK.objects.all().order_by('cube_structure_item_link_id')

    # Get filter values from request
    selected_cube_structure_item_link = request.GET.get('cube_structure_item_link', '')
    selected_primary_member = request.GET.get('primary_member', '')
    selected_foreign_member = request.GET.get('foreign_member', '')

    # Decode URL-encoded parameters (e.g., %20 -> space)
    if selected_cube_structure_item_link:
        selected_cube_structure_item_link = unquote(selected_cube_structure_item_link)
    if selected_primary_member:
        selected_primary_member = unquote(selected_primary_member)
    if selected_foreign_member:
        selected_foreign_member = unquote(selected_foreign_member)

    # Apply filters and ordering
    queryset = MEMBER_LINK.objects.all().order_by('cube_structure_item_link_id', 'primary_member_id').select_related(
        'cube_structure_item_link_id', 'primary_member_id', 'foreign_member_id')
    if selected_cube_structure_item_link:
        queryset = queryset.filter(cube_structure_item_link_id=selected_cube_structure_item_link)
    if selected_primary_member:
        queryset = queryset.filter(primary_member_id=selected_primary_member)
    if selected_foreign_member:
        queryset = queryset.filter(foreign_member_id=selected_foreign_member)

    # Handle form submission
    if request.method == 'POST':
        if 'save' in request.POST:
            ModelFormSet = modelformset_factory(MEMBER_LINK, fields='__all__', extra=0)
            formset = ModelFormSet(request.POST, queryset=queryset)
            if formset.is_valid():
                formset.save()
                messages.success(request, 'Member links updated successfully!')
                return redirect(request.get_full_path())
            else:
                messages.error(request, 'Please correct the errors below.')
        elif 'add' in request.POST:
            # Handle adding new member link
            try:
                new_link = MEMBER_LINK.objects.create(
                    cube_structure_item_link_id_id=request.POST.get('cube_structure_item_link_id'),
                    primary_member_id_id=request.POST.get('primary_member_id'),
                    foreign_member_id_id=request.POST.get('foreign_member_id'),
                    is_linked=request.POST.get('is_linked') == 'on',
                    valid_from=request.POST.get('valid_from') if request.POST.get('valid_from') else None,
                    valid_to=request.POST.get('valid_to') if request.POST.get('valid_to') else None,
                )
                messages.success(request, 'Member link created successfully!')
                return redirect(request.get_full_path())
            except Exception as e:
                messages.error(request, f'Error creating member link: {str(e)}')

    # Add pagination and formset creation
    page_number = request.GET.get('page', 1)
    paginator = Paginator(queryset, 20)
    page_obj = paginator.get_page(page_number)

    ModelFormSet = modelformset_factory(MEMBER_LINK, fields='__all__', extra=0)
    formset = ModelFormSet(queryset=page_obj.object_list)

    context = {
        'formset': formset,
        'page_obj': page_obj,
        'cube_structure_item_links': cube_structure_item_links,
        'primary_members': primary_members,
        'foreign_members': foreign_members,
        'selected_cube_structure_item_link': selected_cube_structure_item_link,
        'selected_primary_member': selected_primary_member,
        'selected_foreign_member': selected_foreign_member,
        'all_members': all_members,
        'all_cube_structure_item_links': all_cube_structure_item_links,
    }
    return render(request, 'pybirdai/miscellaneous/member_links_embed.html', context)


def get_member_links_json(request):
    """
    AJAX endpoint to get member links as JSON.
    Supports filtering by cube_structure_item_link, primary_member, and foreign_member.
    """
    try:
        # Get filter parameters
        cube_link_id = request.GET.get('cube_structure_item_link', '')
        primary_member_id = request.GET.get('primary_member', '')
        foreign_member_id = request.GET.get('foreign_member', '')

        # Decode URL-encoded parameters (e.g., %20 -> space)
        if cube_link_id:
            cube_link_id = unquote(cube_link_id)
        if primary_member_id:
            primary_member_id = unquote(primary_member_id)
        if foreign_member_id:
            foreign_member_id = unquote(foreign_member_id)

        # Build queryset with filters
        queryset = MEMBER_LINK.objects.all().select_related(
            'cube_structure_item_link_id', 'primary_member_id', 'foreign_member_id'
        ).order_by('cube_structure_item_link_id', 'primary_member_id')

        if cube_link_id:
            queryset = queryset.filter(cube_structure_item_link_id=cube_link_id)
        if primary_member_id:
            queryset = queryset.filter(primary_member_id=primary_member_id)
        if foreign_member_id:
            queryset = queryset.filter(foreign_member_id=foreign_member_id)

        # Build response data
        links = []
        for link in queryset:
            links.append({
                'cube_structure_item_link_id': link.cube_structure_item_link_id_id,
                'primary_member_id': link.primary_member_id_id,
                'foreign_member_id': link.foreign_member_id_id,
                'is_linked': link.is_linked,
                'valid_from': link.valid_from.isoformat() if link.valid_from else None,
                'valid_to': link.valid_to.isoformat() if link.valid_to else None,
            })

        return JsonResponse({
            'status': 'success',
            'links': links,
            'count': len(links)
        })
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=400)


def get_member_links_filter_options(request):
    """
    API endpoint to get distinct filter options for member links.
    Returns distinct cube structure item links available for filtering.
    """
    try:
        # Get distinct cube structure item links from member links
        cube_structure_item_links = MEMBER_LINK.objects.values_list(
            'cube_structure_item_link_id', flat=True
        ).distinct().order_by('cube_structure_item_link_id')

        cube_structure_item_links_list = [link for link in cube_structure_item_links if link]  # Filter out None values

        return JsonResponse({
            'status': 'success',
            'cube_structure_item_links': cube_structure_item_links_list
        })
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=400)


def get_related_members_json(request, cube_link_id):
    """
    AJAX endpoint to get members related to a specific cube_structure_item_link.
    Returns distinct primary and foreign members that have links to this cube structure item link.
    """
    try:
        # Decode URL-encoded parameter (e.g., %20 -> space)
        cube_link_id = unquote(cube_link_id)

        # Get all member links for this cube structure item link
        member_links = MEMBER_LINK.objects.filter(
            cube_structure_item_link_id=cube_link_id
        ).select_related('primary_member_id', 'foreign_member_id')

        # Get distinct members
        primary_members = set()
        foreign_members = set()

        for link in member_links:
            if link.primary_member_id_id:
                primary_members.add(link.primary_member_id_id)
            if link.foreign_member_id_id:
                foreign_members.add(link.foreign_member_id_id)

        # Get domain-filtered members for add form
        # Based on the domains of the cube structure item link's variables
        domain_filtered_members = []
        try:
            cube_link = CUBE_STRUCTURE_ITEM_LINK.objects.select_related(
                'primary_cube_variable_code__variable_id',
                'foreign_cube_variable_code__variable_id'
            ).get(cube_structure_item_link_id=cube_link_id)

            # Get domains from primary and foreign variables
            domains = set()
            if cube_link.primary_cube_variable_code and cube_link.primary_cube_variable_code.variable_id:
                if cube_link.primary_cube_variable_code.variable_id.domain_id:
                    domains.add(cube_link.primary_cube_variable_code.variable_id.domain_id)
            if cube_link.foreign_cube_variable_code and cube_link.foreign_cube_variable_code.variable_id:
                if cube_link.foreign_cube_variable_code.variable_id.domain_id:
                    domains.add(cube_link.foreign_cube_variable_code.variable_id.domain_id)

            # Filter members by these domains
            if domains:
                domain_filtered_members = list(
                    MEMBER.objects.filter(domain_id__in=domains)
                    .order_by('member_id')
                    .values_list('member_id', flat=True)
                )
        except CUBE_STRUCTURE_ITEM_LINK.DoesNotExist:
            # If cube link doesn't exist, return empty list
            domain_filtered_members = []

        return JsonResponse({
            'status': 'success',
            'primary_members': sorted(list(primary_members)),
            'foreign_members': sorted(list(foreign_members)),
            'all_members': domain_filtered_members
        })
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=400)


def add_member_link_ajax(request):
    """
    AJAX endpoint to add a new member link.
    """
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'POST method required'}, status=405)

    try:
        # Parse datetime fields with timezone awareness
        valid_from = None
        valid_to = None

        if request.POST.get('valid_from'):
            try:
                # Parse the datetime-local format (YYYY-MM-DDTHH:MM) and make it timezone-aware
                valid_from_str = request.POST.get('valid_from')
                # Handle both formats: YYYY-MM-DDTHH:MM or YYYY-MM-DD HH:MM:SS
                if 'T' in valid_from_str:
                    naive_dt = datetime.strptime(valid_from_str, '%Y-%m-%dT%H:%M')
                else:
                    naive_dt = datetime.strptime(valid_from_str, '%Y-%m-%d %H:%M:%S')
                valid_from = timezone.make_aware(naive_dt)
            except ValueError as e:
                return JsonResponse({
                    'status': 'error',
                    'message': f'Invalid valid_from format: {str(e)}'
                }, status=400)

        if request.POST.get('valid_to'):
            try:
                # Parse the datetime-local format (YYYY-MM-DDTHH:MM) and make it timezone-aware
                valid_to_str = request.POST.get('valid_to')
                # Handle both formats: YYYY-MM-DDTHH:MM or YYYY-MM-DD HH:MM:SS
                if 'T' in valid_to_str:
                    naive_dt = datetime.strptime(valid_to_str, '%Y-%m-%dT%H:%M')
                else:
                    naive_dt = datetime.strptime(valid_to_str, '%Y-%m-%d %H:%M:%S')
                valid_to = timezone.make_aware(naive_dt)
            except ValueError as e:
                return JsonResponse({
                    'status': 'error',
                    'message': f'Invalid valid_to format: {str(e)}'
                }, status=400)

        # Create new member link
        new_link = MEMBER_LINK.objects.create(
            cube_structure_item_link_id_id=request.POST.get('cube_structure_item_link_id'),
            primary_member_id_id=request.POST.get('primary_member_id'),
            foreign_member_id_id=request.POST.get('foreign_member_id'),
            is_linked=request.POST.get('is_linked', 'true').lower() == 'true',
            valid_from=valid_from,
            valid_to=valid_to,
        )

        return JsonResponse({
            'status': 'success',
            'message': 'Member link created successfully!',
            'link': {
                'cube_structure_item_link_id': new_link.cube_structure_item_link_id_id,
                'primary_member_id': new_link.primary_member_id_id,
                'foreign_member_id': new_link.foreign_member_id_id,
                'is_linked': new_link.is_linked,
                'valid_from': new_link.valid_from.isoformat() if new_link.valid_from else None,
                'valid_to': new_link.valid_to.isoformat() if new_link.valid_to else None,
            }
        })
    except Exception as e:
        # Better error logging
        import traceback
        error_details = traceback.format_exc()
        print(f"Error creating member link: {error_details}")  # Log to console

        return JsonResponse({
            'status': 'error',
            'message': f'Error creating member link: {str(e)}'
        }, status=400)


def delete_member_link(request, cube_structure_item_link_id, primary_member_id, foreign_member_id):
    """Delete a MEMBER_LINK record."""
    try:
        # Decode URL-encoded parameters (e.g., %20 -> space)
        cube_structure_item_link_id = unquote(cube_structure_item_link_id)
        primary_member_id = unquote(primary_member_id)
        foreign_member_id = unquote(foreign_member_id)

        link = get_object_or_404(
            MEMBER_LINK,
            cube_structure_item_link_id=cube_structure_item_link_id,
            primary_member_id=primary_member_id,
            foreign_member_id=foreign_member_id
        )
        link.delete()
        messages.success(request, 'Member link deleted successfully.')
        return JsonResponse({'status': 'success', 'message': 'Member link deleted successfully.'})
    except Exception as e:
        messages.error(request, f'Error deleting member link: {str(e)}')
        return JsonResponse({'status': 'error', 'message': str(e)}, status=400)


def edit_member_links_embed(request):
    """
    Embedded version of member links editor for iframe usage.
    Returns a lightweight template without base.html navigation.
    """
    all_cube_structure_item_links = CUBE_STRUCTURE_ITEM_LINK.objects.all().order_by('cube_structure_item_link_id')

    context = {
        'all_cube_structure_item_links': all_cube_structure_item_links,
    }

    return render(request, 'pybirdai/miscellaneous/member_links_embed.html', context)
