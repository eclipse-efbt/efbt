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
Semantic integration editor views.
"""
import json
import logging
from datetime import datetime
from typing import Any

from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.db import transaction
from django.db.models import Q

from pybirdai.models.bird_meta_data_model import (
    VARIABLE_MAPPING_ITEM, MEMBER_MAPPING_ITEM, MAPPING_DEFINITION, CUBE, VARIABLE, MEMBER
)
from pybirdai.context.sdd_context_django import SDDContext
from pybirdai.views.core.mapping_library import (
    build_mapping_results,
    get_reference_variables,
    get_source_variables,
    process_member_mappings,
    create_table_data,
)

logger = logging.getLogger(__name__)

DEFAULT_MAPPING_VALID_FROM = datetime.strptime("1999-01-01", "%Y-%m-%d")
DEFAULT_MAPPING_VALID_TO = datetime.strptime("9999-12-31", "%Y-%m-%d")


def _parse_variable_header(variable_label: str) -> tuple[str | None, str | None]:
    """Extract display name and code from a UI header label."""
    if not variable_label:
        return None, None

    label = variable_label.strip()
    if label.endswith(')') and ' (' in label:
        name, code = label.rsplit(' (', 1)
        return name.strip(), code[:-1].strip()

    return label, label


def _get_mapping_definition(mapping_identifier: str) -> MAPPING_DEFINITION:
    """Resolve a mapping definition from either code or mapping_id."""
    return MAPPING_DEFINITION.objects.select_related(
        'variable_mapping_id',
        'member_mapping_id',
    ).get(Q(mapping_id=mapping_identifier) | Q(code=mapping_identifier))


def semantic_integration_editor(request: Any, mapping_id: str = "") -> Any:
    """Semantic integration editor view.

    Optimized version with query optimization, caching, and pagination support.

    Args:
        request: HTTP request object
        mapping_id: Optional mapping identifier (from URL path or query param)

    Returns:
        Rendered template response
    """
    logger.info(f"Handling semantic integration editor request for mapping ID: {mapping_id}")
    domains = None

    # Support both URL path parameter and query parameter for mapping_id
    selected_mapping = mapping_id or request.GET.get('mapping_id', '')

    # Pagination parameters
    page = int(request.GET.get('page', 1))
    page_size = int(request.GET.get('page_size', 200))

    # Build the dropdown from the full mapping list. The editor becomes unusable
    # when a mapping is paged out of the select, which was happening for
    # DPM_CPS_MCB_MCY once the count exceeded the default page size.
    mapping_definitions = MAPPING_DEFINITION.objects.filter(
        member_mapping_id__isnull=False
    ).select_related(
        'member_mapping_id',
        'variable_mapping_id'
    )
    results, _ = build_mapping_results(
        mapping_definitions,
        page=1,
        page_size=mapping_definitions.count() or 1,
    )
    _, pagination_info = build_mapping_results(mapping_definitions, page=page, page_size=page_size)

    context = {
        "mapping_data": results,
        "pagination": pagination_info
    }

    # Get reference variables and source variables (now cached!)
    reference_variables = get_reference_variables()
    source_variables = get_source_variables()

    # Sort the keys for consistent display
    keys = sorted(reference_variables.keys())
    reference_variables = {k:reference_variables[k] for k in keys}

    # Sort the source variables by keys
    source_keys = sorted(source_variables.keys())
    source_variables = {k:source_variables[k] for k in source_keys}

    # Add to context for template access
    context["reference_variables"] = reference_variables
    context["source_variables"] = source_variables
    # Optimize CUBE query with only needed fields
    context["cubes"] = CUBE.objects.only('cube_id', 'name', 'code').order_by('name')

    if selected_mapping:
        logger.info(f"Processing selected mapping: {selected_mapping}")

        try:
            # Optimize query with select_related
            map_def = MAPPING_DEFINITION.objects.select_related(
                'member_mapping_id',
                'variable_mapping_id'
            ).get(code=selected_mapping)

            # Optimize member mapping items query
            member_mapping_items = MEMBER_MAPPING_ITEM.objects.select_related(
                'variable_id__domain_id',
                'member_id'
            ).filter(member_mapping_id=map_def.member_mapping_id)

            # Optimize variable items query
            var_items = VARIABLE_MAPPING_ITEM.objects.select_related(
                'variable_id__domain_id'
            ).filter(
                variable_mapping_id=map_def.variable_mapping_id
            ).order_by('is_source', 'variable_id__name')

            temp_items, unique_set, source_target = process_member_mappings(member_mapping_items, var_items)
            columns_of_table = sum(list(map(list,source_target.values())),[])
            logging.debug(str(columns_of_table))
            serialized_items_2 = {row_id: { k_:row_data['items'].get(k_)
                for k_ in columns_of_table}
                for row_id, row_data in temp_items.items()}
            table_data = create_table_data(serialized_items_2, columns_of_table)


            context.update({
                'table_data': table_data,
                "selected_mapping": selected_mapping,
                "uniques":unique_set,
                "domains":domains,
                "target_variable_headers": source_target["target"],
                "uniques_sources":{k:{kk:v[kk] for kk,_ in sorted(v.items(), key=lambda item: item[1])} for k,v in unique_set.items() if k in source_target["source"]},
                "uniques_targets":{k:{kk:v[kk] for kk,_ in sorted(v.items(), key=lambda item: item[1])} for k,v in unique_set.items() if k in source_target["target"]},
            })

        except MAPPING_DEFINITION.DoesNotExist:
            logger.warning(f"Mapping '{selected_mapping}' not found in database")
            from django.contrib import messages
            messages.warning(
                request,
                f"The mapping '{selected_mapping}' could not be found. "
                f"Please select a different mapping from the list."
            )
            # Clear selected_mapping and set empty defaults
            selected_mapping = None
            context.update({
                'table_data': {'headers': [], 'rows': []},
                'selected_mapping': None,
                'uniques': {},
                'domains': None,
                'target_variable_headers': [],
                'uniques_sources': {},
                'uniques_targets': {},
            })

    return render(request, 'pybirdai/miscellaneous/semantic_integration_editor.html', context)


def add_variable_endpoint(request: Any) -> JsonResponse:
    """Endpoint for adding variables.

    Args:
        request: HTTP request object

    Returns:
        JSON response with status
    """
    sdd_context = SDDContext()
    logger.info("Handling add variable endpoint request")
    if request.method != "POST":
        logger.warning("Invalid request method")
        return HttpResponseBadRequest('Invalid request method')

    try:
        data = json.loads(request.body)
        orig_mapping_id = data.get('mapping_id')
        member_mapping_row = data.get('member_mapping_row')
        variable = data.get('variable')
        members = data.get('members', [])
        is_source = data.get('is_source', 'true')

        variable_obj = VARIABLE.objects.get(variable_id=variable)

        if not orig_mapping_id:
            return JsonResponse({'status': 'error', 'message': 'mapping_id is required'}, status=400)

        mapping_def = _get_mapping_definition(orig_mapping_id)

        variable_mapping_item, created = VARIABLE_MAPPING_ITEM.objects.get_or_create(
            variable_mapping_id=mapping_def.variable_mapping_id,
            variable_id=variable_obj,
            defaults={
                'is_source': is_source,
                'valid_from': DEFAULT_MAPPING_VALID_FROM,
                'valid_to': DEFAULT_MAPPING_VALID_TO,
            }
        )
        fields_to_update = []
        if not created and str(variable_mapping_item.is_source).lower() != str(is_source).lower():
            variable_mapping_item.is_source = is_source
            fields_to_update.append('is_source')
        if not getattr(variable_mapping_item, 'valid_from', None):
            variable_mapping_item.valid_from = DEFAULT_MAPPING_VALID_FROM
            fields_to_update.append('valid_from')
        if not getattr(variable_mapping_item, 'valid_to', None):
            variable_mapping_item.valid_to = DEFAULT_MAPPING_VALID_TO
            fields_to_update.append('valid_to')
        if fields_to_update:
            variable_mapping_item.save(update_fields=fields_to_update)

        try:
            variable_mapping_list = sdd_context.variable_mapping_item_dictionary[
                mapping_def.variable_mapping_id.variable_mapping_id
            ]
            if variable_mapping_item not in variable_mapping_list:
                variable_mapping_list.append(variable_mapping_item)
        except KeyError:
            sdd_context.variable_mapping_item_dictionary[
                mapping_def.variable_mapping_id.variable_mapping_id
            ] = [variable_mapping_item]

        for member_id in members:
            if not member_id or member_id == "None":
                continue

            member_obj = MEMBER.objects.get(member_id=member_id)
            mapping_item, _ = MEMBER_MAPPING_ITEM.objects.update_or_create(
                member_mapping_id=mapping_def.member_mapping_id,
                member_mapping_row=member_mapping_row,
                variable_id=variable_obj,
                defaults={
                    'member_id': member_obj,
                    'is_source': is_source,
                }
            )

            try:
                member_mapping_list = sdd_context.member_mapping_items_dictionary[
                    mapping_item.member_mapping_id.member_mapping_id
                ]
                if mapping_item not in member_mapping_list:
                    member_mapping_list.append(mapping_item)
            except KeyError:
                sdd_context.member_mapping_items_dictionary[
                    mapping_item.member_mapping_id.member_mapping_id
                ] = [mapping_item]

        logger.info("Variable and members added successfully")
        return JsonResponse({'status': 'success'})

    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error adding variable: {str(e)}", exc_info=True)
        return SecureErrorHandler.secure_json_response(e, 'variable addition', request)


def edit_mapping_endpoint(request: Any) -> JsonResponse:
    """Endpoint for editing mappings.

    Args:
        request: HTTP request object

    Returns:
        JSON response with status
    """
    sdd_context = SDDContext()
    logger.info("Handling edit mapping endpoint request")
    if request.method != "POST":
        logger.warning("Invalid request method")
        return HttpResponseBadRequest('Invalid request method')

    try:
        data = json.loads(request.body)
        orig_mapping_id = data.get('mapping_id')
        source_data = data.get('source_data', {})
        target_data = data.get('target_data', {})

        print(source_data, target_data)

        # Get existing mapping if available
        if orig_mapping_id:
            with transaction.atomic():
                orig_mapping = MAPPING_DEFINITION.objects.get(mapping_id=orig_mapping_id)
                member_mapping = orig_mapping.member_mapping_id

                # Find the highest existing row number to determine the next row number
                existing_items = MEMBER_MAPPING_ITEM.objects.filter(member_mapping_id=member_mapping)

                if existing_items.exists():
                    last_member_mapping_row = str(max(int(item.member_mapping_row) for item in existing_items) + 1)
                else:
                    last_member_mapping_row = "1"

                logger.info(f"Adding new row {last_member_mapping_row} to existing mapping {orig_mapping.mapping_id}")

                # Add new source items to the existing mapping
                for variable_, member_ in zip(source_data["variabless"], source_data["members"]):
                    variable_id = variable_.split(" ")[-1].strip("(").rstrip(")")
                    variable_obj = VARIABLE.objects.get(code=variable_id)
                    member_obj = MEMBER.objects.get(member_id=member_)

                    new_mm_item =MEMBER_MAPPING_ITEM.objects.create(
                        member_mapping_id=member_mapping,
                        member_mapping_row=last_member_mapping_row,
                        variable_id=variable_obj,
                        member_id=member_obj,
                        is_source='true'
                    )
                    try:
                        member_mapping_list = sdd_context.member_mapping_items_dictionary[
                            new_mm_item.member_mapping_id.member_mapping_id]
                        member_mapping_list.append(new_mm_item)
                    except KeyError:
                        sdd_context.member_mapping_items_dictionary[
                            new_mm_item.member_mapping_id.member_mapping_id] = [new_mm_item]
                    logger.info(f"Added source item to existing mapping for row {last_member_mapping_row}")

                # Add new target items to the existing mapping
                for variable_, member_ in zip(target_data["variablses"], target_data["members"]):
                    variable_id = variable_.split(" ")[-1].strip("(").rstrip(")")
                    variable_obj = VARIABLE.objects.get(variable_id=variable_id)
                    member_obj = MEMBER.objects.get(member_id=member_)

                    new_mm_item = MEMBER_MAPPING_ITEM.objects.create(
                        member_mapping_id=member_mapping,
                        member_mapping_row=last_member_mapping_row,
                        variable_id=variable_obj,
                        member_id=member_obj,
                        is_source='false'
                    )
                    try:
                        member_mapping_list = sdd_context.member_mapping_items_dictionary[
                            new_mm_item.member_mapping_id.member_mapping_id]
                        member_mapping_list.append(new_mm_item)
                    except KeyError:
                        sdd_context.member_mapping_items_dictionary[
                            new_mm_item.member_mapping_id.member_mapping_id] = [new_mm_item]
                    logger.info(f"Added target item to existing mapping for row {last_member_mapping_row}")

        logger.info("Mapping updated successfully")
        return JsonResponse({'status': 'success'})

    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error updating mapping: {str(e)}", exc_info=True)
        return SecureErrorHandler.secure_json_response(e, 'mapping update', request)


def delete_target_variable(request: Any) -> JsonResponse:
    """Delete a target variable from the selected mapping across all rows."""
    logger.info("Handling delete target variable request")
    if request.method != "POST":
        logger.warning("Invalid request method")
        return HttpResponseBadRequest('Invalid request method')

    try:
        data = json.loads(request.body)
        mapping_id = data.get('mapping_id')
        variable_id = data.get('variable_id')
        if variable_id and variable_id.endswith(')') and ' (' in variable_id:
            variable_id = variable_id.rsplit('(', 1)[-1].rstrip(')')

        if not mapping_id or not variable_id:
            return JsonResponse({'success': False, 'error': 'mapping_id and variable_id are required'}, status=400)

        with transaction.atomic():
            mapping_def = _get_mapping_definition(mapping_id)
            variable_name, variable_code = _parse_variable_header(variable_id)
            variable = VARIABLE.objects.filter(
                Q(variable_id=variable_code) | Q(code=variable_code) | Q(name=variable_name, code=variable_code)
            ).first()
            if variable is None:
                raise VARIABLE.DoesNotExist(variable_code)

            deleted_variable_items, _ = VARIABLE_MAPPING_ITEM.objects.filter(
                variable_mapping_id=mapping_def.variable_mapping_id,
                variable_id=variable,
                is_source__iexact='false',
            ).delete()

            deleted_member_items, _ = MEMBER_MAPPING_ITEM.objects.filter(
                member_mapping_id=mapping_def.member_mapping_id,
                variable_id=variable,
                is_source__iexact='false',
            ).delete()

        logger.info(
            "Deleted target variable %s from mapping %s (%s variable items, %s member items)",
            variable_id, mapping_id, deleted_variable_items, deleted_member_items,
        )
        return JsonResponse({
            'success': True,
            'deleted_variable_items': deleted_variable_items,
            'deleted_member_items': deleted_member_items,
        })

    except MAPPING_DEFINITION.DoesNotExist:
        return JsonResponse({'success': False, 'error': f'Mapping {mapping_id} not found'}, status=404)
    except VARIABLE.DoesNotExist:
        return JsonResponse({'success': False, 'error': f'Variable {variable_id} not found'}, status=404)
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error deleting target variable: {str(e)}", exc_info=True)
        return SecureErrorHandler.secure_json_response(e, 'target variable deletion', request)


def get_domain_members(request, variable_id: str = ""):
    """Get domain members for a variable.

    Args:
        request: HTTP request object
        variable_id: ID of variable to get members for

    Returns:
        JSON response with members data
    """
    logger.info("Handling get domain members request")
    try:
        if not variable_id:
            logger.warning("No variable ID provided")
            return JsonResponse({'status': 'error', 'message': 'Variable ID required'})

        variable = VARIABLE.objects.get(variable_id=variable_id)
        domain = variable.domain_id
        members = MEMBER.objects.filter(domain_id=domain)

        member_data = []
        for member in members:
            member_data.append({
                'member_id': member.member_id,
                'code': member.code,
                'name': member.name
            })

        logger.info(f"Found {len(member_data)} members for variable {variable_id}")
        return JsonResponse({
            'status': 'success',
            'members': member_data
        })

    except VARIABLE.DoesNotExist:
        logger.error(f"Variable {variable_id} not found")
        return JsonResponse({
            'status': 'error',
            'message': f'Variable {variable_id} not found'
        })
    except Exception as e:
        logger.error(f"Error getting domain members: {str(e)}", exc_info=True)
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        })


def get_mapping_details(request, mapping_id):
    """Get mapping definition and related details.

    Args:
        request: HTTP request object
        mapping_id: ID of mapping to get details for

    Returns:
        JSON response with mapping data
    """
    logger.info(f"Handling get mapping details request for mapping {mapping_id}")
    try:
        # Get mapping definition
        mapping_def = MAPPING_DEFINITION.objects.get(mapping_id=mapping_id)
        logger.debug(f"Found mapping definition: {mapping_def.name}")

        # Get variable mapping and items
        variable_mapping = mapping_def.variable_mapping_id
        variable_mapping_items = VARIABLE_MAPPING_ITEM.objects.filter(
            variable_mapping_id=variable_mapping
        )
        logger.debug(f"Found {variable_mapping_items.count()} variable mapping items")

        # Get member mapping and items
        member_mapping = mapping_def.member_mapping_id
        member_mapping_items = MEMBER_MAPPING_ITEM.objects.filter(
            member_mapping_id=member_mapping
        )
        logger.debug(f"Found {member_mapping_items.count()} member mapping items")

        # Build response data
        mapping_data = {
            'mapping_definition': {
                'mapping_id': mapping_def.mapping_id,
                'code': mapping_def.code,
                'name': mapping_def.name,
            },
            'variable_mapping': {
                'variable_mapping_id': variable_mapping.variable_mapping_id,
                'code': variable_mapping.code,
                'name': variable_mapping.name,
                'items': []
            },
            'member_mapping': {
                'member_mapping_id': member_mapping.member_mapping_id,
                'code': member_mapping.code,
                'name': member_mapping.name,
                'items': []
            }
        }
        # Add variable mapping items
        for item in variable_mapping_items:
            mapping_data['variable_mapping']['items'].append({
                'source_variable': {
                    'variable_id': item.variable_id.variable_id,
                    'code': item.variable_id.code,
                    'name': item.variable_id.name
                },
                'target_variable': {
                    'variable_id': item.variable_id.variable_id,
                    'code': item.variable_mapping_id.code,
                    'name': item.variable_mapping_id.name
                }
            })

        # Add member mapping items
        for item in member_mapping_items:
            mapping_data['member_mapping']['items'].append({
                'member_mapping_row': item.member_mapping_row,
                'variable': {
                    'variable_id': item.variable_id.variable_id,
                    'code': item.variable_id.code,
                    'name': item.variable_id.name
                },
                'member': {
                    'member_id': item.member_id.member_id,
                    'code': item.member_id.code,
                    'name': item.member_id.name
                },
                'is_source': item.is_source
            })
        logger.info("Successfully retrieved mapping details")
        return JsonResponse({
            'status': 'success',
            'data': mapping_data
        })

    except MAPPING_DEFINITION.DoesNotExist:
        logger.error(f"Mapping {mapping_id} not found")
        return JsonResponse({
            'status': 'error',
            'message': f'Mapping {mapping_id} not found'
        })
    except Exception as e:
        logger.error(f"Error getting mapping details: {str(e)}", exc_info=True)
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        })
