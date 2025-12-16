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
Member hierarchy editor views.
"""
import json
import logging

from django.http import JsonResponse
from django.shortcuts import render
from django.db import transaction

from pybirdai.models.bird_meta_data_model import (
    MEMBER, MEMBER_HIERARCHY, MEMBER_HIERARCHY_NODE, DOMAIN, SUBDOMAIN, SUBDOMAIN_ENUMERATION
)

logger = logging.getLogger(__name__)


def member_hierarchy_editor(request, hierarchy_id=None):
    """
    View function for rendering the member hierarchy editor page.

    Args:
        request: HTTP request object
        hierarchy_id: Optional hierarchy ID to display specific hierarchy

    Returns:
        Rendered template response with hierarchy data
    """
    from pybirdai.views.core.member_hierarchy_editor.django_hierarchy_integration import get_hierarchy_integration

    logger.info(f"Rendering member hierarchy editor page for hierarchy_id: {hierarchy_id}")

    # Get all member hierarchies for the dropdown
    hierarchies = MEMBER_HIERARCHY.objects.all().order_by('name')

    context = {
        'hierarchies': hierarchies,
        'selected_hierarchy_id': hierarchy_id
    }

    # If a specific hierarchy is selected, get its details
    if hierarchy_id:
        try:
            hierarchy = MEMBER_HIERARCHY.objects.get(member_hierarchy_id=hierarchy_id)

            # Get hierarchy data using the integration
            integration = get_hierarchy_integration()
            hierarchy_data = integration.get_hierarchy_by_id(hierarchy_id)
            context.update({
                'selected_hierarchy': hierarchy,
                'hierarchy_data_json': json.dumps(hierarchy_data),
                'hierarchy_info': hierarchy_data.get('hierarchy_info', {})
            })
            print(
                hierarchy_data.get('hierarchy_info', {}).get('hierarchy', {})
            )

        except MEMBER_HIERARCHY.DoesNotExist:
            logger.error(f"Member hierarchy {hierarchy_id} not found")
            context['error'] = f"Member hierarchy {hierarchy_id} not found"

    return render(request, 'pybirdai/miscellaneous/member_hierarchy_editor.html', context)


def add_member_to_hierarchy(request):
    """
    Endpoint for adding a member to a member hierarchy.

    Args:
        request: HTTP request object with POST data

    Returns:
        JSON response with status
    """
    logger.info("Handling add member to hierarchy request")
    if request.method != "POST":
        return JsonResponse({'status': 'error', 'message': 'Invalid request method'})

    try:
        data = json.loads(request.body)
        hierarchy_id = data.get('hierarchy_id')
        member_id = data.get('member_id')
        parent_member_id = data.get('parent_member_id')
        level = data.get('level', 1)
        comparator = data.get('comparator', '')
        operator = data.get('operator', '')

        # Get the hierarchy object
        hierarchy = MEMBER_HIERARCHY.objects.get(member_hierarchy_id=hierarchy_id)

        # Get the member object
        member = MEMBER.objects.get(member_id=member_id)

        # Get parent member if specified
        parent_member = None
        if parent_member_id:
            parent_member = MEMBER.objects.get(member_id=parent_member_id)

        # Create new hierarchy node
        new_node = MEMBER_HIERARCHY_NODE.objects.create(
            member_hierarchy_id=hierarchy,
            member_id=member,
            parent_member_id=parent_member,
            level=level,
            comparator=comparator,
            operator=operator
        )
        new_node.save()

        logger.info(f"Successfully added member {member_id} to hierarchy {hierarchy_id}")
        return JsonResponse({
            'status': 'success',
            'node_id': new_node.id,
            'message': 'Member added to hierarchy successfully'
        })

    except MEMBER_HIERARCHY.DoesNotExist:
        logger.error(f"Member hierarchy {hierarchy_id} not found")
        return JsonResponse({'status': 'error', 'message': 'Member hierarchy not found'})
    except MEMBER.DoesNotExist:
        logger.error(f"Member {member_id} not found")
        return JsonResponse({'status': 'error', 'message': 'Member not found'})
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error adding member to hierarchy: {str(e)}", exc_info=True)
        return SecureErrorHandler.secure_json_response(e, 'member hierarchy addition', request)


def delete_member_from_hierarchy(request):
    """
    Endpoint for deleting a member from a member hierarchy.
    Handles cascading deletion of child nodes.

    Args:
        request: HTTP request object with POST data

    Returns:
        JSON response with status
    """
    logger.info("Handling delete member from hierarchy request")
    if request.method != "POST":
        return JsonResponse({'status': 'error', 'message': 'Invalid request method'})

    try:
        data = json.loads(request.body)
        node_id = data.get('node_id')
        force_delete = data.get('force_delete', False)

        # Get the hierarchy node
        node = MEMBER_HIERARCHY_NODE.objects.get(id=node_id)

        # Check if this node has children
        children = MEMBER_HIERARCHY_NODE.objects.filter(parent_member_id=node.member_id)

        if children.exists() and not force_delete:
            # Return warning about children
            child_count = children.count()
            return JsonResponse({
                'status': 'warning',
                'message': f'This member has {child_count} child member(s). Do you want to delete them as well?',
                'has_children': True,
                'child_count': child_count
            })

        # Use atomic transaction for deletion
        with transaction.atomic():
            if children.exists():
                # Recursively delete all children
                def delete_children(parent_member):
                    child_nodes = MEMBER_HIERARCHY_NODE.objects.filter(
                        parent_member_id=parent_member,
                        member_hierarchy_id=node.member_hierarchy_id
                    )
                    for child_node in child_nodes:
                        delete_children(child_node.member_id)
                        child_node.delete()

                delete_children(node.member_id)

            # Delete the node itself
            node.delete()

        logger.info(f"Successfully deleted node {node_id} from hierarchy")
        return JsonResponse({
            'status': 'success',
            'message': 'Member deleted from hierarchy successfully'
        })

    except MEMBER_HIERARCHY_NODE.DoesNotExist:
        logger.error(f"Hierarchy node {node_id} not found")
        return JsonResponse({'status': 'error', 'message': 'Hierarchy node not found'})
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error deleting member from hierarchy: {str(e)}", exc_info=True)
        return SecureErrorHandler.secure_json_response(e, 'member hierarchy deletion', request)


def edit_hierarchy_node(request):
    """
    Endpoint for editing a hierarchy node (member, comparator, operator).

    Args:
        request: HTTP request object with POST data

    Returns:
        JSON response with status
    """
    logger.info("Handling edit hierarchy node request")
    if request.method != "POST":
        return JsonResponse({'status': 'error', 'message': 'Invalid request method'})

    try:
        data = json.loads(request.body)
        node_id = data.get('node_id')
        member_id = data.get('member_id')
        comparator = data.get('comparator', '')
        operator = data.get('operator', '')
        level = data.get('level')

        # Get the hierarchy node
        node = MEMBER_HIERARCHY_NODE.objects.get(id=node_id)

        # Update member if provided
        if member_id:
            member = MEMBER.objects.get(member_id=member_id)
            node.member_id = member

        # Update other fields
        if comparator is not None:
            node.comparator = comparator
        if operator is not None:
            node.operator = operator
        if level is not None:
            node.level = level

        node.save()

        logger.info(f"Successfully updated hierarchy node {node_id}")
        return JsonResponse({
            'status': 'success',
            'message': 'Hierarchy node updated successfully'
        })

    except MEMBER_HIERARCHY_NODE.DoesNotExist:
        logger.error(f"Hierarchy node {node_id} not found")
        return JsonResponse({'status': 'error', 'message': 'Hierarchy node not found'})
    except MEMBER.DoesNotExist:
        logger.error(f"Member {member_id} not found")
        return JsonResponse({'status': 'error', 'message': 'Member not found'})
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error editing hierarchy node: {str(e)}", exc_info=True)
        return SecureErrorHandler.secure_json_response(e, 'hierarchy node editing', request)


def get_members_by_domain(request, domain_id):
    """
    Get members for a specific domain (for dropdown population).

    Args:
        request: HTTP request object
        domain_id: ID of domain to get members for

    Returns:
        JSON response with members data
    """
    logger.info(f"Getting members for domain {domain_id}")
    try:
        domain = DOMAIN.objects.get(domain_id=domain_id)
        members = MEMBER.objects.filter(domain_id=domain).order_by('name')

        member_data = []
        for member in members:
            member_data.append({
                'member_id': member.member_id,
                'code': member.code,
                'name': member.name,
                'description': member.description
            })

        return JsonResponse({
            'status': 'success',
            'members': member_data
        })

    except DOMAIN.DoesNotExist:
        logger.error(f"Domain {domain_id} not found")
        return JsonResponse({'status': 'error', 'message': 'Domain not found'})
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error getting members by domain: {str(e)}", exc_info=True)
        return SecureErrorHandler.secure_json_response(e, 'domain members retrieval', request)


def get_subdomain_enumerations(request, subdomain_id):
    """
    Get subdomain enumerations for comparator/operator dropdowns.

    Args:
        request: HTTP request object
        subdomain_id: ID of subdomain to get enumerations for

    Returns:
        JSON response with enumeration data
    """
    logger.info(f"Getting subdomain enumerations for subdomain {subdomain_id}")
    try:
        subdomain = SUBDOMAIN.objects.get(subdomain_id=subdomain_id)
        enumerations = SUBDOMAIN_ENUMERATION.objects.filter(subdomain_id=subdomain).order_by('name')

        enum_data = []
        for enum in enumerations:
            enum_data.append({
                'enumeration_id': enum.enumeration_id,
                'code': enum.code,
                'name': enum.name,
                'description': enum.description
            })

        return JsonResponse({
            'status': 'success',
            'enumerations': enum_data
        })

    except SUBDOMAIN.DoesNotExist:
        logger.error(f"Subdomain {subdomain_id} not found")
        return JsonResponse({'status': 'error', 'message': 'Subdomain not found'})
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error getting subdomain enumerations: {str(e)}", exc_info=True)
        return SecureErrorHandler.secure_json_response(e, 'subdomain enumerations retrieval', request)


def get_hierarchy_json(request, hierarchy_id):
    """
    API endpoint to get hierarchy data in JSON format for the visual editor
    """
    from pybirdai.views.core.member_hierarchy_editor.django_hierarchy_integration import get_hierarchy_integration

    try:
        integration = get_hierarchy_integration()
        hierarchy_data = integration.get_hierarchy_by_id(hierarchy_id)
        return JsonResponse(hierarchy_data)
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error getting hierarchy JSON for {hierarchy_id}: {str(e)}")
        error_data = SecureErrorHandler.handle_exception(e, 'hierarchy JSON retrieval', request)
        return JsonResponse({'error': error_data['message']}, status=500)


def save_hierarchy_json(request):
    """
    API endpoint to save hierarchy data from the visual editor
    """
    from pybirdai.views.core.member_hierarchy_editor.django_hierarchy_integration import get_hierarchy_integration

    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)

    try:
        data = json.loads(request.body)
        hierarchy_id = data.get('hierarchy_id')
        visualization_data = data.get('data')

        integration = get_hierarchy_integration()

        success = integration.save_hierarchy_from_visualization(hierarchy_id, visualization_data)

        return JsonResponse({
            'success': success,
            'message': 'Hierarchy saved successfully' if success else 'Failed to save hierarchy'
        })
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error saving hierarchy: {str(e)}")
        error_data = SecureErrorHandler.handle_exception(e, 'hierarchy saving', request)
        return JsonResponse({'error': error_data['message']}, status=500)


def get_domain_members_json(request, domain_id):
    """
    API endpoint to get all members for a domain in JSON format
    """
    from pybirdai.views.core.member_hierarchy_editor.django_hierarchy_integration import get_hierarchy_integration

    try:
        integration = get_hierarchy_integration()
        members = integration.get_domain_members(domain_id)
        return JsonResponse({'members': members})
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error getting domain members for {domain_id}: {str(e)}")
        error_data = SecureErrorHandler.handle_exception(e, 'domain members retrieval', request)
        return JsonResponse({'error': error_data['message']}, status=500)


def get_available_hierarchies_json(request):
    """
    API endpoint to get all available hierarchies in JSON format
    """
    from pybirdai.views.core.member_hierarchy_editor.django_hierarchy_integration import get_hierarchy_integration

    try:
        integration = get_hierarchy_integration()
        hierarchies = integration.get_available_hierarchies()
        return JsonResponse({'hierarchies': hierarchies})
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error getting available hierarchies: {str(e)}")
        error_data = SecureErrorHandler.handle_exception(e, 'available hierarchies retrieval', request)
        return JsonResponse({'error': error_data['message']}, status=500)


def create_hierarchy_from_visualization(request):
    """
    API endpoint to create a new hierarchy from visualization data
    """
    from pybirdai.views.core.member_hierarchy_editor.django_hierarchy_integration import get_hierarchy_integration

    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)

    try:
        data = json.loads(request.body)
        hierarchy_id = data.get('hierarchy_id')
        hierarchy_name = data.get('name')
        domain_id = data.get('domain_id')
        description = data.get('description', '')
        visualization_data = data.get('data')

        if not hierarchy_name or not domain_id or not visualization_data:
            return JsonResponse({'error': 'Missing required fields'}, status=400)

        # Create new hierarchy
        try:
            domain = DOMAIN.objects.get(domain_id=domain_id)
        except DOMAIN.DoesNotExist:
            return JsonResponse({'error': 'Domain not found'}, status=404)

        hierarchy = MEMBER_HIERARCHY.objects.create(
            member_hierarchy_id=hierarchy_id,
            name=hierarchy_name,
            description=description,
            domain_id=domain
        )

        # Save the visualization data
        integration = get_hierarchy_integration()
        success = integration.save_hierarchy_from_visualization(hierarchy_id, visualization_data)

        if success:
            return JsonResponse({
                'success': True,
                'hierarchy_id': hierarchy_id,
                'message': 'Hierarchy created successfully'
            })
        else:
            # Clean up the hierarchy if saving failed
            hierarchy.delete()
            return JsonResponse({'error': 'Failed to save hierarchy data'}, status=500)

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error creating hierarchy: {str(e)}")
        error_data = SecureErrorHandler.handle_exception(e, 'hierarchy creation', request)
        return JsonResponse({'error': error_data['message']}, status=500)


def create_hierarchy_simple(request):
    """
    API endpoint to create a new empty MEMBER_HIERARCHY.
    Used when navigating from cube structure viewer to create a new hierarchy
    for a variable's domain.

    POST body:
    {
        "hierarchy_id": "MY_HIERARCHY_ID",
        "name": "My Hierarchy Name",
        "domain_id": "DOMAIN_ID",
        "description": "Optional description"
    }
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)

    try:
        data = json.loads(request.body)
        hierarchy_id = data.get('hierarchy_id')
        hierarchy_name = data.get('name')
        domain_id = data.get('domain_id')
        description = data.get('description', '')

        if not hierarchy_id:
            return JsonResponse({'error': 'hierarchy_id is required'}, status=400)

        if not hierarchy_name:
            return JsonResponse({'error': 'name is required'}, status=400)

        if not domain_id:
            return JsonResponse({'error': 'domain_id is required'}, status=400)

        # Check if hierarchy already exists
        if MEMBER_HIERARCHY.objects.filter(member_hierarchy_id=hierarchy_id).exists():
            return JsonResponse({
                'error': 'A hierarchy with this ID already exists'
            }, status=400)

        # Get the domain
        try:
            domain = DOMAIN.objects.get(domain_id=domain_id)
        except DOMAIN.DoesNotExist:
            return JsonResponse({'error': 'Domain not found'}, status=404)

        # Create the new hierarchy
        hierarchy = MEMBER_HIERARCHY.objects.create(
            member_hierarchy_id=hierarchy_id,
            name=hierarchy_name,
            code=hierarchy_id,  # Use ID as code by default
            description=description,
            domain_id=domain
        )

        logger.info(f"Created new hierarchy: {hierarchy_id} for domain {domain_id}")

        return JsonResponse({
            'success': True,
            'hierarchy_id': hierarchy_id,
            'message': 'Hierarchy created successfully'
        })

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error creating hierarchy: {str(e)}")
        error_data = SecureErrorHandler.handle_exception(e, 'hierarchy creation', request)
        return JsonResponse({'error': error_data['message']}, status=500)


def create_member_json(request):
    """
    API endpoint to create a new MEMBER in the database.
    Used by the hierarchy editor to create new members on the fly.
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required'}, status=405)

    try:
        data = json.loads(request.body)
        member_id = data.get('member_id')
        name = data.get('name')
        code = data.get('code')
        description = data.get('description')
        domain_id = data.get('domain_id')

        if not member_id or not name:
            return JsonResponse({'error': 'member_id and name are required'}, status=400)

        if not domain_id:
            return JsonResponse({'error': 'domain_id is required'}, status=400)

        # Check if member already exists
        if MEMBER.objects.filter(member_id=member_id).exists():
            return JsonResponse({'error': 'A member with this ID already exists'}, status=400)

        # Get the domain
        try:
            domain = DOMAIN.objects.get(domain_id=domain_id)
        except DOMAIN.DoesNotExist:
            return JsonResponse({'error': 'Domain not found'}, status=404)

        # Create the new member
        member = MEMBER.objects.create(
            member_id=member_id,
            name=name,
            code=code,
            description=description,
            domain_id=domain
        )

        logger.info(f"Created new member: {member_id} in domain {domain_id}")

        return JsonResponse({
            'success': True,
            'member_id': member_id,
            'message': 'Member created successfully'
        })

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error creating member: {str(e)}")
        error_data = SecureErrorHandler.handle_exception(e, 'member creation', request)
        return JsonResponse({'error': error_data['message']}, status=500)
