"""
Embed views for joins metadata editing in ANCRDT workflow dashboard.
These views provide iframe-compatible versions with AJAX-based filtering.

Linked Models:
- CUBE_LINK
- CUBE_STRUCTURE_ITEM_LINK
- CUBE
- CUBE_STRUCTURE_ITEM
- VARIABLE
"""

from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.clickjacking import xframe_options_exempt

from pybirdai.models.bird_meta_data_model import (
    CUBE_LINK,
    CUBE_STRUCTURE_ITEM_LINK,
    CUBE,
    CUBE_STRUCTURE_ITEM
)


@xframe_options_exempt
def edit_cube_links_embed(request):
    """Embed version of edit_cube_links for iframe usage with AJAX filtering"""
    # Get unique values for filters
    foreign_cubes = CUBE_LINK.objects.values_list('foreign_cube_id', flat=True).distinct()
    join_identifiers = CUBE_LINK.objects.values_list('join_identifier', flat=True).distinct()

    context = {
        'foreign_cubes': foreign_cubes,
        'join_identifiers': join_identifiers,
    }
    return render(request, 'pybirdai/miscellaneous/edit_cube_links_embed.html', context)


def api_cube_links_list(request):
    """API endpoint to get filtered cube links as JSON"""
    # Get filter values from request
    foreign_cube = request.GET.get('foreign_cube', '')
    join_identifier = request.GET.get('join_identifier', '')

    # Apply filters
    queryset = CUBE_LINK.objects.all().order_by('cube_link_id')
    if foreign_cube:
        queryset = queryset.filter(foreign_cube_id__cube_id=foreign_cube)
    if join_identifier:
        queryset = queryset.filter(join_identifier=join_identifier)

    # Convert to list of dictionaries
    links = []
    for link in queryset:
        links.append({
            'cube_link_id': link.cube_link_id,
            'code': link.code,
            'name': link.name,
            'description': link.description,
            'primary_cube_id': link.primary_cube_id.cube_id if link.primary_cube_id else None,
            'foreign_cube_id': link.foreign_cube_id.cube_id if link.foreign_cube_id else None,
            'join_identifier': link.join_identifier,
            'order_relevance': link.order_relevance,
        })

    response = JsonResponse({
        'status': 'success',
        'links': links,
        'count': len(links)
    })
    # Prevent browser caching to ensure fresh data after deletions
    response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response['Pragma'] = 'no-cache'
    response['Expires'] = '0'
    return response


def api_cube_links_filter_options(request):
    """API endpoint to get distinct filter options for cube links"""
    # Get distinct foreign cubes (follow FK to get cube_id string)
    foreign_cubes = CUBE_LINK.objects.values_list('foreign_cube_id__cube_id', flat=True).distinct().order_by('foreign_cube_id__cube_id')
    foreign_cubes_list = [cube for cube in foreign_cubes if cube]  # Filter out None values

    # Get distinct join identifiers
    join_identifiers = CUBE_LINK.objects.values_list('join_identifier', flat=True).distinct().order_by('join_identifier')
    join_identifiers_list = [identifier for identifier in join_identifiers if identifier]  # Filter out None values

    return JsonResponse({
        'status': 'success',
        'foreign_cubes': foreign_cubes_list,
        'join_identifiers': join_identifiers_list
    })


@xframe_options_exempt
def edit_cube_structure_item_links_embed(request):
    """Embed version of edit_cube_structure_item_links for iframe usage with AJAX filtering"""
    # Get unique cube links for filter dropdown
    unique_cube_links = CUBE_LINK.objects.values_list('cube_link_id', flat=True).distinct()

    context = {
        'unique_cube_links': unique_cube_links,
    }
    return render(request, 'pybirdai/miscellaneous/edit_cube_structure_item_links_embed.html', context)


def api_cube_structure_item_links_list(request):
    """API endpoint to get filtered cube structure item links as JSON"""
    # Get filter value from request
    cube_link = request.GET.get('cube_link', '')

    # Apply filter
    queryset = CUBE_STRUCTURE_ITEM_LINK.objects.all().order_by('cube_structure_item_link_id')
    if cube_link:
        queryset = queryset.filter(cube_link_id__cube_link_id=cube_link)

    # Convert to list of dictionaries
    links = []
    for link in queryset:
        links.append({
            'cube_structure_item_link_id': link.cube_structure_item_link_id,
            'cube_link_id': link.cube_link_id.cube_link_id if link.cube_link_id else None,
            'foreign_variable_id': link.foreign_cube_variable_code.variable_id.variable_id if link.foreign_cube_variable_code and link.foreign_cube_variable_code.variable_id else None,
            'primary_variable_id': link.primary_cube_variable_code.variable_id.variable_id if link.primary_cube_variable_code and link.primary_cube_variable_code.variable_id else None,
            'foreign_cube_id': link.cube_link_id.foreign_cube_id.cube_id if link.cube_link_id and link.cube_link_id.foreign_cube_id else None,
            'primary_cube_id': link.cube_link_id.primary_cube_id.cube_id if link.cube_link_id and link.cube_link_id.primary_cube_id else None,
        })

    response = JsonResponse({
        'status': 'success',
        'links': links,
        'count': len(links)
    })
    # Prevent browser caching to ensure fresh data after deletions
    response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response['Pragma'] = 'no-cache'
    response['Expires'] = '0'
    return response


def api_cube_structure_item_links_filter_options(request):
    """API endpoint to get distinct filter options for cube structure item links"""
    # Get distinct cube links (follow FK to get cube_link_id string)
    cube_links = CUBE_STRUCTURE_ITEM_LINK.objects.values_list('cube_link_id__cube_link_id', flat=True).distinct().order_by('cube_link_id__cube_link_id')
    cube_links_list = [link for link in cube_links if link]  # Filter out None values

    return JsonResponse({
        'status': 'success',
        'cube_links': cube_links_list
    })


# ============================================================================
# CUBE LINKS - Additional APIs for Add Functionality and Cascading Filters
# ============================================================================

def get_cubes_json(request):
    """API endpoint to get all cubes for dropdown population"""
    cubes = CUBE.objects.all().order_by('cube_id').values_list('cube_id', flat=True)
    return JsonResponse({
        'status': 'success',
        'cubes': list(cubes)
    })


def get_join_identifiers_for_cube(request, foreign_cube_id):
    """API endpoint to get join identifiers filtered by foreign_cube_id (cascading filter)"""
    join_identifiers = CUBE_LINK.objects.filter(
        foreign_cube_id=foreign_cube_id
    ).values_list('join_identifier', flat=True).distinct().order_by('join_identifier')

    return JsonResponse({
        'status': 'success',
        'join_identifiers': list(filter(None, join_identifiers))  # Filter out None values
    })


def add_cube_link_ajax(request):
    """API endpoint to add a new cube link via AJAX"""
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)

    try:
        # Get form data
        primary_cube_id = request.POST.get('primary_cube_id')
        foreign_cube_id = request.POST.get('foreign_cube_id')
        join_identifier = request.POST.get('join_identifier', '')
        code = request.POST.get('code', '')
        name = request.POST.get('name', '')
        description = request.POST.get('description', '')
        order_relevance = request.POST.get('order_relevance', None)
        cube_link_type = request.POST.get('cube_link_type', '')

        # Validate required fields
        if not primary_cube_id or not foreign_cube_id:
            return JsonResponse({
                'status': 'error',
                'message': 'Primary cube and foreign cube are required'
            }, status=400)

        # Get cube objects
        try:
            primary_cube = CUBE.objects.get(cube_id=primary_cube_id)
            foreign_cube = CUBE.objects.get(cube_id=foreign_cube_id)
        except CUBE.DoesNotExist:
            return JsonResponse({
                'status': 'error',
                'message': 'Invalid cube ID provided'
            }, status=400)

        # Generate cube_link_id
        cube_link_id = f"{foreign_cube_id}:{primary_cube_id}:{join_identifier}"

        # Check if cube link already exists
        if CUBE_LINK.objects.filter(cube_link_id=cube_link_id).exists():
            return JsonResponse({
                'status': 'error',
                'message': f'Cube link with ID {cube_link_id} already exists'
            }, status=400)

        # Create the cube link
        cube_link = CUBE_LINK.objects.create(
            cube_link_id=cube_link_id,
            primary_cube_id=primary_cube,
            foreign_cube_id=foreign_cube,
            join_identifier=join_identifier if join_identifier else None,
            code=code if code else None,
            name=name if name else None,
            description=description if description else None,
            order_relevance=int(order_relevance) if order_relevance else None,
            cube_link_type=cube_link_type if cube_link_type else None
        )

        return JsonResponse({
            'status': 'success',
            'message': f'Cube link {cube_link_id} created successfully',
            'cube_link_id': cube_link.cube_link_id
        })

    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': f'Error creating cube link: {str(e)}'
        }, status=500)


# ============================================================================
# CUBE STRUCTURE ITEM LINKS - Additional APIs for Add Functionality
# ============================================================================

def get_cube_links_json(request):
    """API endpoint to get all cube links for dropdown population"""
    cube_links = CUBE_LINK.objects.all().order_by('cube_link_id').values_list('cube_link_id', flat=True)
    return JsonResponse({
        'status': 'success',
        'cube_links': list(cube_links)
    })


def get_cube_structure_items_for_link(request, cube_link_id):
    """API endpoint to get cube structure items filtered by cube_link_id (cascading filter)"""
    try:
        # Get the cube link
        cube_link = CUBE_LINK.objects.get(cube_link_id=cube_link_id)

        # Get foreign cube structure items
        foreign_cube_structure_items = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id=cube_link.foreign_cube_id.cube_structure_id
        ).select_related('variable_id').order_by('variable_id__variable_id')

        # Get primary cube structure items
        primary_cube_structure_items = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id=cube_link.primary_cube_id.cube_structure_id
        ).select_related('variable_id').order_by('variable_id__variable_id')

        # Convert to lists
        foreign_items = [
            {
                'id': item.id,
                'variable_id': item.variable_id.variable_id if item.variable_id else None
            }
            for item in foreign_cube_structure_items
        ]

        primary_items = [
            {
                'id': item.id,
                'variable_id': item.variable_id.variable_id if item.variable_id else None
            }
            for item in primary_cube_structure_items
        ]

        return JsonResponse({
            'status': 'success',
            'foreign_items': foreign_items,
            'primary_items': primary_items,
            'foreign_cube_id': cube_link.foreign_cube_id.cube_id,
            'primary_cube_id': cube_link.primary_cube_id.cube_id
        })

    except CUBE_LINK.DoesNotExist:
        return JsonResponse({
            'status': 'error',
            'message': 'Cube link not found'
        }, status=404)
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': f'Error fetching cube structure items: {str(e)}'
        }, status=500)


def add_cube_structure_item_link_ajax(request):
    """API endpoint to add a new cube structure item link via AJAX"""
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)

    try:
        # Get form data
        cube_link_id = request.POST.get('cube_link_id')
        foreign_cube_variable_code_id = request.POST.get('foreign_cube_variable_code_id')
        primary_cube_variable_code_id = request.POST.get('primary_cube_variable_code_id')

        # Validate required fields
        if not cube_link_id or not foreign_cube_variable_code_id or not primary_cube_variable_code_id:
            return JsonResponse({
                'status': 'error',
                'message': 'All fields are required'
            }, status=400)

        # Get objects
        try:
            cube_link = CUBE_LINK.objects.get(cube_link_id=cube_link_id)
            foreign_item = CUBE_STRUCTURE_ITEM.objects.get(id=foreign_cube_variable_code_id)
            primary_item = CUBE_STRUCTURE_ITEM.objects.get(id=primary_cube_variable_code_id)
        except (CUBE_LINK.DoesNotExist, CUBE_STRUCTURE_ITEM.DoesNotExist):
            return JsonResponse({
                'status': 'error',
                'message': 'Invalid cube link or cube structure item ID provided'
            }, status=400)

        # Generate cube_structure_item_link_id
        foreign_var = foreign_item.variable_id.variable_id if foreign_item.variable_id else ''
        primary_var = primary_item.variable_id.variable_id if primary_item.variable_id else ''
        cube_structure_item_link_id = f"{cube_link_id}:{foreign_var}:{primary_var}"

        # Check if link already exists
        if CUBE_STRUCTURE_ITEM_LINK.objects.filter(cube_structure_item_link_id=cube_structure_item_link_id).exists():
            return JsonResponse({
                'status': 'error',
                'message': f'Cube structure item link with ID {cube_structure_item_link_id} already exists'
            }, status=400)

        # Create the cube structure item link
        structure_item_link = CUBE_STRUCTURE_ITEM_LINK.objects.create(
            cube_structure_item_link_id=cube_structure_item_link_id,
            cube_link_id=cube_link,
            foreign_cube_variable_code=foreign_item,
            primary_cube_variable_code=primary_item
        )

        return JsonResponse({
            'status': 'success',
            'message': f'Cube structure item link {cube_structure_item_link_id} created successfully',
            'cube_structure_item_link_id': structure_item_link.cube_structure_item_link_id
        })

    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'message': f'Error creating cube structure item link: {str(e)}'
        }, status=500)
