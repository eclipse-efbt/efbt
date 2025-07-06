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

import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class ColumnIndexManagerProcessStep:
    """
    Process step for managing column indexes in clone mode operations.
    Refactored from utils.clone_mode.clone_mode_column_index to follow process step patterns.
    """
    
    def __init__(self, context=None):
        """
        Initialize the column index manager process step.
        
        Args:
            context: The context object containing configuration settings.
        """
        self.context = context
        
    def execute(self, operation: str = "get_indexes", table_name: str = None, 
                **kwargs) -> Dict[str, Any]:
        """
        Execute column index management operations.
        
        Args:
            operation (str): Operation type - "get_indexes", "set_indexes", "get_all_mappings"
            table_name (str): Table name for specific operations
            **kwargs: Additional parameters for specific operations
            
        Returns:
            dict: Result dictionary with success status and details
        """
        try:
            manager = ColumnIndexes()
            
            if operation == "get_indexes":
                if not table_name:
                    raise ValueError("table_name is required for get_indexes operation")
                
                result = manager.get_column_indexes(table_name)
                
                return {
                    'success': True,
                    'operation': 'get_indexes',
                    'table_name': table_name,
                    'indexes': result,
                    'message': f'Column indexes retrieved for {table_name}'
                }
            
            elif operation == "set_indexes":
                if not table_name:
                    raise ValueError("table_name is required for set_indexes operation")
                
                column_mappings = kwargs.get('column_mappings', {})
                manager.set_column_indexes(table_name, column_mappings)
                
                return {
                    'success': True,
                    'operation': 'set_indexes',
                    'table_name': table_name,
                    'mappings_set': len(column_mappings),
                    'message': f'Column indexes set for {table_name}'
                }
            
            elif operation == "get_all_mappings":
                result = manager.get_all_mappings()
                
                return {
                    'success': True,
                    'operation': 'get_all_mappings',
                    'total_tables': len(result),
                    'mappings': result,
                    'message': f'All column mappings retrieved for {len(result)} tables'
                }
            
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            if self.context:
                self.context.column_index_manager = manager
                
        except Exception as e:
            logger.error(f"Failed to execute column index manager: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Column index management operation failed'
            }


class ColumnIndexes:
    """
    Enhanced column index manager with process step integration.
    Refactored from utils.clone_mode.clone_mode_column_index.
    """
    
    def __init__(self):
        """Initialize the column indexes manager."""
        self.column_mappings = {}
        self._initialize_default_mappings()
        logger.info("ColumnIndexes manager initialized")
    
    def _initialize_default_mappings(self):
        """Initialize default column mappings for common tables."""
        # These are default mappings that can be overridden
        self.column_mappings = {
            'MEMBER': {
                'MAINTENANCE_AGENCY_ID': 0,
                'MEMBER_ID': 1,
                'CODE': 2,
                'NAME': 3,
                'DOMAIN_ID': 4,
                'DESCRIPTION': 5
            },
            'VARIABLE': {
                'MAINTENANCE_AGENCY_ID': 0,
                'VARIABLE_ID': 1,
                'CODE': 2,
                'NAME': 3,
                'DOMAIN_ID': 4,
                'DESCRIPTION': 5,
                'PRIMARY_CONCEPT': 6,
                'IS_DECOMPOSED': 7
            },
            'DOMAIN': {
                'MAINTENANCE_AGENCY_ID': 0,
                'DOMAIN_ID': 1,
                'NAME': 2,
                'IS_ENUMERATED': 3,
                'DESCRIPTION': 4,
                'DATA_TYPE': 5,
                'CODE': 6,
                'FACET_ID': 7,
                'IS_REFERENCE': 8
            },
            'SUBDOMAIN': {
                'MAINTENANCE_AGENCY_ID': 0,
                'SUBDOMAIN_ID': 1,
                'NAME': 2,
                'DOMAIN_ID': 3,
                'IS_LISTED': 4,
                'CODE': 5,
                'FACET_ID': 6,
                'DESCRIPTION': 7,
                'IS_NATURAL': 8
            },
            'CUBE': {
                'MAINTENANCE_AGENCY_ID': 0,
                'CUBE_ID': 1,
                'NAME': 2,
                'CODE': 3,
                'FRAMEWORK_ID': 4,
                'CUBE_STRUCTURE_ID': 5,
                'CUBE_TYPE': 6,
                'IS_ALLOWED': 7,
                'VALID_FROM': 8,
                'VALID_TO': 9,
                'VERSION': 10,
                'DESCRIPTION': 11,
                'PUBLISHED': 12,
                'DATASET_URL': 13,
                'FILTERS': 14,
                'DI_EXPORT': 15
            },
            'COMBINATION': {
                'COMBINATION_ID': 0,
                'CODE': 1,
                'NAME': 2,
                'MAINTENANCE_AGENCY_ID': 3,
                'VERSION': 4,
                'VALID_FROM': 5,
                'VALID_TO': 6,
                'METRIC': 7
            },
            'MAPPING_DEFINITION': {
                'MAINTENANCE_AGENCY_ID': 0,
                'MAPPING_ID': 1,
                'NAME': 2,
                'MAPPING_TYPE': 3,
                'CODE': 4,
                'ALGORITHM': 5,
                'MEMBER_MAPPING_ID': 6,
                'VARIABLE_MAPPING_ID': 7
            }
        }
    
    def get_column_indexes(self, table_name: str) -> Dict[str, int]:
        """
        Get column index mappings for a specific table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            dict: Column name to index mapping
        """
        table_key = table_name.upper()
        mappings = self.column_mappings.get(table_key, {})
        
        logger.debug(f"Retrieved {len(mappings)} column mappings for {table_name}")
        return mappings
    
    def set_column_indexes(self, table_name: str, column_mappings: Dict[str, int]):
        """
        Set column index mappings for a specific table.
        
        Args:
            table_name (str): Name of the table
            column_mappings (dict): Column name to index mapping
        """
        table_key = table_name.upper()
        self.column_mappings[table_key] = column_mappings.copy()
        
        logger.info(f"Set {len(column_mappings)} column mappings for {table_name}")
    
    def get_all_mappings(self) -> Dict[str, Dict[str, int]]:
        """
        Get all column mappings for all tables.
        
        Returns:
            dict: All table column mappings
        """
        logger.debug(f"Retrieved all column mappings for {len(self.column_mappings)} tables")
        return self.column_mappings.copy()
    
    def add_table_mapping(self, table_name: str, column_mappings: Dict[str, int]):
        """
        Add or update column mappings for a table.
        
        Args:
            table_name (str): Name of the table
            column_mappings (dict): Column name to index mapping
        """
        self.set_column_indexes(table_name, column_mappings)
    
    def remove_table_mapping(self, table_name: str) -> bool:
        """
        Remove column mappings for a table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            bool: True if mapping was removed, False if not found
        """
        table_key = table_name.upper()
        if table_key in self.column_mappings:
            del self.column_mappings[table_key]
            logger.info(f"Removed column mappings for {table_name}")
            return True
        else:
            logger.warning(f"No column mappings found for {table_name}")
            return False
    
    def get_column_index(self, table_name: str, column_name: str) -> Optional[int]:
        """
        Get the index for a specific column in a table.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column
            
        Returns:
            int or None: Column index or None if not found
        """
        table_mappings = self.get_column_indexes(table_name)
        column_key = column_name.upper()
        
        index = table_mappings.get(column_key)
        if index is not None:
            logger.debug(f"Column {column_name} in {table_name} is at index {index}")
        else:
            logger.warning(f"Column {column_name} not found in {table_name} mappings")
        
        return index
    
    def set_column_index(self, table_name: str, column_name: str, index: int):
        """
        Set the index for a specific column in a table.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column
            index (int): Column index
        """
        table_key = table_name.upper()
        column_key = column_name.upper()
        
        if table_key not in self.column_mappings:
            self.column_mappings[table_key] = {}
        
        self.column_mappings[table_key][column_key] = index
        logger.debug(f"Set column {column_name} in {table_name} to index {index}")
    
    def validate_mappings(self, table_name: str) -> Dict[str, Any]:
        """
        Validate column mappings for a table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            dict: Validation results
        """
        mappings = self.get_column_indexes(table_name)
        
        validation_result = {
            'table_name': table_name,
            'total_columns': len(mappings),
            'valid': True,
            'issues': []
        }
        
        if not mappings:
            validation_result['valid'] = False
            validation_result['issues'].append('No column mappings found')
            return validation_result
        
        # Check for duplicate indexes
        indexes = list(mappings.values())
        if len(indexes) != len(set(indexes)):
            validation_result['valid'] = False
            validation_result['issues'].append('Duplicate column indexes found')
        
        # Check for negative indexes
        negative_indexes = [idx for idx in indexes if idx < 0]
        if negative_indexes:
            validation_result['valid'] = False
            validation_result['issues'].append(f'Negative indexes found: {negative_indexes}')
        
        # Check for gaps in indexes
        if indexes:
            max_index = max(indexes)
            min_index = min(indexes)
            expected_indexes = set(range(min_index, max_index + 1))
            actual_indexes = set(indexes)
            missing_indexes = expected_indexes - actual_indexes
            
            if missing_indexes:
                validation_result['issues'].append(f'Index gaps found: {sorted(missing_indexes)}')
        
        logger.info(f"Validated mappings for {table_name}: {'Valid' if validation_result['valid'] else 'Invalid'}")
        return validation_result
    
    def export_mappings_to_dict(self) -> Dict[str, Any]:
        """
        Export all mappings to a dictionary for serialization.
        
        Returns:
            dict: Exportable mapping data
        """
        export_data = {
            'version': '1.0',
            'created_at': str(logger.handlers[0].formatter.formatTime() if logger.handlers else 'unknown'),
            'total_tables': len(self.column_mappings),
            'mappings': self.column_mappings.copy()
        }
        
        logger.info(f"Exported mappings for {len(self.column_mappings)} tables")
        return export_data
    
    def import_mappings_from_dict(self, import_data: Dict[str, Any]) -> bool:
        """
        Import mappings from a dictionary.
        
        Args:
            import_data (dict): Mapping data to import
            
        Returns:
            bool: True if import was successful
        """
        try:
            if 'mappings' not in import_data:
                raise ValueError("Import data must contain 'mappings' key")
            
            imported_mappings = import_data['mappings']
            
            # Validate imported data
            for table_name, mappings in imported_mappings.items():
                if not isinstance(mappings, dict):
                    raise ValueError(f"Invalid mappings for table {table_name}")
                
                for column_name, index in mappings.items():
                    if not isinstance(index, int):
                        raise ValueError(f"Invalid index for {table_name}.{column_name}: {index}")
            
            # Import the mappings
            self.column_mappings.update(imported_mappings)
            
            logger.info(f"Imported mappings for {len(imported_mappings)} tables")
            return True
            
        except Exception as e:
            logger.error(f"Failed to import mappings: {e}")
            return False