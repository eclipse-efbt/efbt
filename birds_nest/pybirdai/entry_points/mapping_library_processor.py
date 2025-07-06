# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

from django.apps import AppConfig
import logging
from typing import Dict, List, Set, Tuple, Any, Optional

logger = logging.getLogger(__name__)


class MappingLibraryProcessorConfig(AppConfig):
    """
    Django AppConfig for Mapping Library Processing operations.
    Provides mapping definition, member mapping, and variable mapping functionality.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'pybirdai.entry_points.mapping_library_processor'
    verbose_name = 'Mapping Library Processor'

    def ready(self):
        """Initialize the mapping library processor when Django starts."""
        logger.info("Mapping Library Processor initialized")


def update_member_mapping_item(member_mapping, member_mapping_row: str, variable, member, is_source_str: str):
    """
    Updates or creates a member mapping item.
    
    Args:
        member_mapping: The member mapping object
        member_mapping_row (str): Row identifier
        variable: Variable object
        member: Member object
        is_source_str (str): String indicating if source mapping
        
    Returns:
        dict: Result with created or updated Member Mapping Item
    """
    logger.debug(f"Updating member mapping item: {member_mapping_row}")
    
    try:
        from pybirdai.bird_meta_data_model import MEMBER_MAPPING_ITEM
        
        mapping_item, created = MEMBER_MAPPING_ITEM.objects.update_or_create(
            member_mapping_id=member_mapping,
            member_mapping_row=member_mapping_row,
            variable_id=variable,
            defaults={
                'member_id': member,
                'is_source': is_source_str
            }
        )
        
        logger.debug(f"Member mapping item {'created' if created else 'updated'}: {member_mapping_row}")
        
        return {
            'success': True,
            'mapping_item': mapping_item,
            'created': created,
            'message': f'Member mapping item {"created" if created else "updated"} successfully'
        }
        
    except Exception as e:
        logger.error(f"Failed to update member mapping item: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to update member mapping item'
        }


def get_filtered_variable_items(variable_mapping_id: str):
    """
    Gets filtered variable items for a given mapping ID.
    
    Args:
        variable_mapping_id (str): ID of the variable mapping to filter
        
    Returns:
        dict: Result with filtered variable items
    """
    logger.debug(f"Getting filtered variable items for mapping ID: {variable_mapping_id}")
    
    try:
        from pybirdai.bird_meta_data_model import VARIABLE_MAPPING_ITEM
        
        var_items = VARIABLE_MAPPING_ITEM.objects.filter(variable_mapping_id=variable_mapping_id)
        source_vars = [item for item in var_items if item.is_source.lower() == 'true']
        target_vars = [item for item in var_items if item.is_source.lower() != 'true']
        
        logger.debug(f"Found {len(var_items)} total items, {len(source_vars)} source, {len(target_vars)} target")
        
        return {
            'success': True,
            'all_items': var_items,
            'source_items': source_vars,
            'target_items': target_vars,
            'counts': {
                'total': len(var_items),
                'source': len(source_vars),
                'target': len(target_vars)
            },
            'message': f'Found {len(var_items)} variable items'
        }
        
    except Exception as e:
        logger.error(f"Failed to get filtered variable items: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to get filtered variable items'
        }


def build_mapping_results(mapping_definitions):
    """
    Builds mapping results dictionary from mapping definitions.
    
    Args:
        mapping_definitions: List of mapping definition objects
        
    Returns:
        dict: Result with mapping results data
    """
    logger.debug("Building mapping results")
    
    try:
        results = {}
        processed_count = 0
        
        for map_def in mapping_definitions:
            if not map_def.member_mapping_id:
                continue
            
            if map_def.variable_mapping_id:
                var_items_result = get_filtered_variable_items(map_def.variable_mapping_id)
                if var_items_result.get('success'):
                    target_vars = var_items_result['target_items']
                    all_items = var_items_result['all_items']
                    
                    if len(target_vars) == 0 or len(all_items) == 1:
                        continue
            
            if map_def.mapping_id not in results:
                results[map_def.mapping_id] = {
                    "variable_mapping_id": map_def.variable_mapping_id.code if map_def.variable_mapping_id else None,
                    "has_member_mapping": True,
                    "member_mapping_id": {
                        "code": map_def.member_mapping_id.code,
                        "items": []
                    }
                }
                processed_count += 1
        
        logger.debug(f"Built mapping results for {processed_count} definitions")
        
        return {
            'success': True,
            'mapping_results': results,
            'processed_count': processed_count,
            'message': f'Built mapping results for {processed_count} definitions'
        }
        
    except Exception as e:
        logger.error(f"Failed to build mapping results: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to build mapping results'
        }


def get_source_target_variables(var_items):
    """
    Gets source and target variables from variable items.
    
    Args:
        var_items: List of variable mapping items
        
    Returns:
        dict: Result with source and target variable lists
    """
    logger.debug("Getting source and target variables")
    
    try:
        source_vars = [f"{item.variable_id.name} ({item.variable_id.code})" 
                      for item in var_items if item.is_source.lower() == 'true']
        target_vars = [f"{item.variable_id.name} ({item.variable_id.code})" 
                      for item in var_items if item.is_source.lower() != 'true']
        
        result_data = {
            "source": source_vars,
            "target": target_vars
        }
        
        logger.debug(f"Found {len(source_vars)} source and {len(target_vars)} target variables")
        
        return {
            'success': True,
            'variables': result_data,
            'counts': {
                'source': len(source_vars),
                'target': len(target_vars)
            },
            'message': f'Found {len(source_vars)} source and {len(target_vars)} target variables'
        }
        
    except Exception as e:
        logger.error(f"Failed to get source/target variables: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to get source/target variables'
        }


def initialize_unique_variable_set(member_mapping_items):
    """
    Initializes unique set of member mappings.
    
    Args:
        member_mapping_items: List of member mapping items
        
    Returns:
        dict: Result with unique variable sets
    """
    logger.debug("Initializing unique variable set")
    
    try:
        from pybirdai.bird_meta_data_model import MEMBER
        
        unique_set = {}
        
        for item in member_mapping_items:
            vars_ = f"{item.variable_id.name} ({item.variable_id.code})"
            if vars_ not in unique_set:
                members_dict = {}
                members = MEMBER.objects.filter(domain_id=item.variable_id.domain_id)
                
                for member in members:
                    members_dict[member.member_id] = f"{member.name} ({member.code})"
                
                unique_set[vars_] = members_dict
        
        logger.debug(f"Initialized unique set for {len(unique_set)} variables")
        
        return {
            'success': True,
            'unique_set': unique_set,
            'variable_count': len(unique_set),
            'message': f'Initialized unique set for {len(unique_set)} variables'
        }
        
    except Exception as e:
        logger.error(f"Failed to initialize unique variable set: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to initialize unique variable set'
        }


def build_temporary_mapping_items(member_mapping_items, unique_set: dict):
    """
    Builds temporary items dictionary from member mappings.
    
    Args:
        member_mapping_items: List of member mapping items
        unique_set (dict): Dictionary of unique variable sets
        
    Returns:
        dict: Result with temporary mapping items
    """
    logger.debug("Building temporary mapping items")
    
    try:
        temp_items = {}
        
        for item in member_mapping_items:
            if item.member_mapping_row not in temp_items:
                temp_items[item.member_mapping_row] = {
                    'has_source': False,
                    'has_target': False,
                    'items': {k: "None (None)" for k in unique_set}
                }
            
            vars_ = f"{item.variable_id.name} ({item.variable_id.code})"
            member_ = f"{item.member_id.name} ({item.member_id.code})"
            
            is_source = item.is_source.lower() == 'true'
            temp_items[item.member_mapping_row]['has_source' if is_source else 'has_target'] = True
            temp_items[item.member_mapping_row]['items'][vars_] = member_
        
        logger.debug(f"Built temporary items for {len(temp_items)} mapping rows")
        
        return {
            'success': True,
            'temporary_items': temp_items,
            'row_count': len(temp_items),
            'message': f'Built temporary items for {len(temp_items)} mapping rows'
        }
        
    except Exception as e:
        logger.error(f"Failed to build temporary mapping items: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to build temporary mapping items'
        }


def process_member_mappings(member_mapping_items, var_items):
    """
    Processes member mappings to create temporary items and unique sets.
    
    Args:
        member_mapping_items: List of member mapping items
        var_items: List of variable mapping items
        
    Returns:
        dict: Result with processed mapping data
    """
    logger.debug("Processing member mappings")
    
    try:
        # Get source/target variables
        source_target_result = get_source_target_variables(var_items)
        if not source_target_result.get('success'):
            return source_target_result
        
        # Initialize unique set
        unique_set_result = initialize_unique_variable_set(member_mapping_items)
        if not unique_set_result.get('success'):
            return unique_set_result
        
        # Build temporary items
        temp_items_result = build_temporary_mapping_items(
            member_mapping_items, 
            unique_set_result['unique_set']
        )
        if not temp_items_result.get('success'):
            return temp_items_result
        
        logger.debug("Member mappings processing completed successfully")
        
        return {
            'success': True,
            'temporary_items': temp_items_result['temporary_items'],
            'unique_set': unique_set_result['unique_set'],
            'source_target_variables': source_target_result['variables'],
            'statistics': {
                'row_count': temp_items_result['row_count'],
                'variable_count': unique_set_result['variable_count'],
                'source_count': source_target_result['counts']['source'],
                'target_count': source_target_result['counts']['target']
            },
            'message': 'Member mappings processed successfully'
        }
        
    except Exception as e:
        logger.error(f"Failed to process member mappings: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to process member mappings'
        }


def create_table_data_structure(serialized_items: dict, columns_of_table: list):
    """
    Creates table data structure from serialized items.
    
    Args:
        serialized_items (dict): Dictionary of serialized mapping items
        columns_of_table (list): List of table column names
        
    Returns:
        dict: Result with table data structure
    """
    logger.debug("Creating table data structure")
    
    try:
        table_data = {
            'headers': ["row_id"] + columns_of_table,
            'rows': []
        }
        
        for row_id, row_data in serialized_items.items():
            table_row = {"row_id": int(row_id)}
            table_row.update(row_data)
            table_data['rows'].append(table_row)
        
        # Sort the rows by row_id
        table_data["rows"] = sorted(table_data["rows"], key=lambda x: x["row_id"])
        
        logger.debug(f"Created table data with {len(table_data['rows'])} rows and {len(table_data['headers'])} columns")
        
        return {
            'success': True,
            'table_data': table_data,
            'row_count': len(table_data['rows']),
            'column_count': len(table_data['headers']),
            'message': f'Created table data with {len(table_data["rows"])} rows'
        }
        
    except Exception as e:
        logger.error(f"Failed to create table data structure: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to create table data structure'
        }


def cascade_member_mapping_changes(member_mapping_item):
    """
    Cascades changes from a new member mapping item through related mapping objects.
    
    Args:
        member_mapping_item: The source member mapping item
        
    Returns:
        dict: Result with cascaded changes information
    """
    logger.debug(f"Cascading member mapping changes for row: {member_mapping_item.member_mapping_row}")
    
    try:
        from pybirdai.bird_meta_data_model import MAPPING_DEFINITION
        
        # Create mapping definition
        mapping_def = MAPPING_DEFINITION.objects.create(
            member_mapping_id=member_mapping_item.member_mapping_id,
            name=f"Generated mapping for {member_mapping_item.member_mapping_row}",
            code=f"GEN_MAP_{member_mapping_item.member_mapping_row}"
        )
        
        logger.info(f"Created mapping definition: {mapping_def.code}")
        
        return {
            'success': True,
            'mapping_definition': mapping_def,
            'generated_code': mapping_def.code,
            'message': f'Mapping definition created successfully: {mapping_def.code}'
        }
        
    except Exception as e:
        logger.error(f"Failed to cascade member mapping changes: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to cascade member mapping changes'
        }


def add_variable_to_mapping(mapping_id: str, variable_code: str, is_source_str: str):
    """
    Adds a variable to an existing mapping.
    
    Args:
        mapping_id (str): Mapping identifier
        variable_code (str): Variable code to add
        is_source_str (str): String indicating if source variable
        
    Returns:
        dict: Result with added variable information
    """
    logger.debug(f"Adding variable to mapping: {variable_code}")
    
    try:
        from pybirdai.bird_meta_data_model import MAPPING_DEFINITION, VARIABLE, VARIABLE_MAPPING_ITEM
        
        mapping_def = MAPPING_DEFINITION.objects.get(code=mapping_id)
        variable = VARIABLE.objects.get(code=variable_code)
        
        mapping_item = VARIABLE_MAPPING_ITEM.objects.create(
            variable_mapping_id=mapping_def.variable_mapping_id,
            variable_id=variable,
            is_source=is_source_str
        )
        
        logger.info(f"Added variable {variable_code} to mapping {mapping_id}")
        
        return {
            'success': True,
            'variable': variable,
            'mapping_item': mapping_item,
            'mapping_id': mapping_id,
            'variable_code': variable_code,
            'is_source': is_source_str,
            'message': f'Variable {variable_code} added to mapping {mapping_id}'
        }
        
    except Exception as e:
        logger.error(f"Failed to add variable to mapping: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to add variable to mapping'
        }


def create_or_update_member(member_id: str, variable, domain):
    """
    Creates or updates a member object.
    
    Args:
        member_id (str): Member identifier
        variable: Variable object
        domain: Domain object
        
    Returns:
        dict: Result with created or updated Member object
    """
    logger.debug(f"Creating or updating member: {member_id}")
    
    try:
        from pybirdai.bird_meta_data_model import MEMBER
        
        try:
            member = MEMBER.objects.get(code=member_id, domain_id=domain)
            created = False
            logger.debug(f"Found existing member: {member_id}")
        except MEMBER.DoesNotExist:
            logger.info(f"Creating new member: {member_id}")
            member = MEMBER.objects.create(
                code=member_id,
                name=member_id,
                domain_id=domain
            )
            created = True
        
        return {
            'success': True,
            'member': member,
            'created': created,
            'member_id': member_id,
            'message': f'Member {member_id} {"created" if created else "updated"} successfully'
        }
        
    except Exception as e:
        logger.error(f"Failed to create/update member: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to create/update member'
        }


def get_source_variables():
    """
    Get all source variables from EBA maintenance agency.
    
    Returns:
        dict: Result with source variables data
    """
    logger.debug("Getting source variables from EBA maintenance agency")
    
    try:
        from pybirdai.bird_meta_data_model import VARIABLE, MEMBER
        
        source_variables = {}
        processed_count = 0
        
        for v in VARIABLE.objects.all():
            if v.maintenance_agency_id and "EBA" == v.maintenance_agency_id.code:
                domain = v.domain_id
                domain_members = {}
                members = MEMBER.objects.filter(domain_id=domain)
                
                if len(members):
                    for m in members:
                        domain_members[m.member_id] = {
                            'code': m.code,
                            'name': m.name
                        }
                    
                    source_variables[v.variable_id] = {
                        'domain': {
                            'id': domain.domain_id,
                            'code': domain.code,
                            'name': domain.name,
                            'members': domain_members
                        }
                    }
                    processed_count += 1
        
        logger.debug(f"Found {len(source_variables)} EBA source variables")
        
        return {
            'success': True,
            'source_variables': source_variables,
            'variable_count': len(source_variables),
            'processed_count': processed_count,
            'message': f'Found {len(source_variables)} EBA source variables'
        }
        
    except Exception as e:
        logger.error(f"Failed to get source variables: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to get source variables'
        }


def get_reference_variables():
    """
    Get all available variables from reference framework (REF maintenance agency).
    
    Returns:
        dict: Result with reference variables data
    """
    logger.debug("Getting reference variables from REF maintenance agency")
    
    try:
        from pybirdai.bird_meta_data_model import VARIABLE, MEMBER
        
        reference_variables = {}
        processed_count = 0
        
        for v in VARIABLE.objects.all():
            if v.maintenance_agency_id and "REF" == v.maintenance_agency_id.code:
                domain = v.domain_id
                domain_members = {}
                members = MEMBER.objects.filter(domain_id=domain)
                
                if len(members):
                    for m in members:
                        domain_members[m.member_id] = {
                            'code': m.code,
                            'name': m.name
                        }
                    
                    reference_variables[v.variable_id] = {
                        'domain': {
                            'id': domain.domain_id,
                            'code': domain.code,
                            'name': domain.name,
                            'members': domain_members
                        }
                    }
                    processed_count += 1
        
        logger.debug(f"Found {len(reference_variables)} REF reference variables")
        
        return {
            'success': True,
            'reference_variables': reference_variables,
            'variable_count': len(reference_variables),
            'processed_count': processed_count,
            'message': f'Found {len(reference_variables)} REF reference variables'
        }
        
    except Exception as e:
        logger.error(f"Failed to get reference variables: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to get reference variables'
        }


class MappingLibraryProcessor:
    """
    Main mapping library processor class providing high-level interface.
    Handles mapping definitions, member mappings, and variable mappings.
    """
    
    def __init__(self):
        """Initialize the mapping library processor."""
        logger.info("MappingLibraryProcessor initialized")
    
    def process_complete_mapping_workflow(self, mapping_definitions, include_variables=True):
        """
        Process complete mapping workflow including definitions, members, and variables.
        
        Args:
            mapping_definitions: List of mapping definition objects
            include_variables (bool): Whether to include variable processing
            
        Returns:
            dict: Complete workflow results
        """
        workflow_results = {
            'success': True,
            'steps_completed': [],
            'mapping_results': {},
            'source_variables': {},
            'reference_variables': {},
            'errors': []
        }
        
        try:
            # Step 1: Build mapping results
            mapping_result = build_mapping_results(mapping_definitions)
            if mapping_result.get('success'):
                workflow_results['mapping_results'] = mapping_result['mapping_results']
                workflow_results['steps_completed'].append('mapping_results')
            else:
                workflow_results['errors'].append(f"Mapping results failed: {mapping_result.get('error')}")
            
            # Step 2: Get source variables if requested
            if include_variables:
                source_vars_result = get_source_variables()
                if source_vars_result.get('success'):
                    workflow_results['source_variables'] = source_vars_result['source_variables']
                    workflow_results['steps_completed'].append('source_variables')
                else:
                    workflow_results['errors'].append(f"Source variables failed: {source_vars_result.get('error')}")
                
                # Step 3: Get reference variables
                ref_vars_result = get_reference_variables()
                if ref_vars_result.get('success'):
                    workflow_results['reference_variables'] = ref_vars_result['reference_variables']
                    workflow_results['steps_completed'].append('reference_variables')
                else:
                    workflow_results['errors'].append(f"Reference variables failed: {ref_vars_result.get('error')}")
            
            # Determine overall success
            workflow_results['success'] = len(workflow_results['errors']) == 0
            
        except Exception as e:
            workflow_results['success'] = False
            workflow_results['errors'].append(f"Workflow error: {str(e)}")
            logger.error(f"Mapping workflow error: {e}")
        
        return workflow_results


# Convenience function for backwards compatibility
def run_mapping_library_operations():
    """Get a configured mapping library processor instance."""
    return MappingLibraryProcessor()


# Export main functions for easy access
__all__ = [
    'MappingLibraryProcessorConfig',
    'update_member_mapping_item',
    'get_filtered_variable_items',
    'build_mapping_results',
    'get_source_target_variables',
    'initialize_unique_variable_set',
    'build_temporary_mapping_items',
    'process_member_mappings',
    'create_table_data_structure',
    'cascade_member_mapping_changes',
    'add_variable_to_mapping',
    'create_or_update_member',
    'get_source_variables',
    'get_reference_variables',
    'MappingLibraryProcessor',
    'run_mapping_library_operations'
]