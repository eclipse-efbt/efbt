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

import unidecode
import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class UtilsProcessorProcessStep:
    """
    Process step for general utility operations.
    Refactored from utils.utils to follow process step patterns.
    """
    
    def __init__(self, context=None):
        """
        Initialize the utils processor process step.
        
        Args:
            context: The context object containing configuration settings.
        """
        self.context = context
        
    def execute(self, operation: str = "string_processing", data: Any = None, 
                **kwargs) -> Dict[str, Any]:
        """
        Execute utility processing operations.
        
        Args:
            operation (str): Operation type - "string_processing", "unique_value", etc.
            data: Data to process
            **kwargs: Additional parameters for specific operations
            
        Returns:
            dict: Result dictionary with success status and details
        """
        try:
            utils = Utils()
            
            if operation == "unique_value":
                enum_obj = kwargs.get('enum_obj')
                adapted_value = kwargs.get('adapted_value')
                
                if not enum_obj or not adapted_value:
                    raise ValueError("enum_obj and adapted_value are required for unique_value operation")
                
                result = utils.unique_value(enum_obj, adapted_value)
                
                return {
                    'success': True,
                    'operation': 'unique_value',
                    'result': result,
                    'original_value': adapted_value,
                    'message': f'Generated unique value: {result}'
                }
            
            elif operation == "unique_name":
                enum_obj = kwargs.get('enum_obj')
                enum_used_name = kwargs.get('enum_used_name')
                
                if not enum_obj or not enum_used_name:
                    raise ValueError("enum_obj and enum_used_name are required for unique_name operation")
                
                result = utils.unique_name(enum_obj, enum_used_name)
                
                return {
                    'success': True,
                    'operation': 'unique_name',
                    'result': result,
                    'original_name': enum_used_name,
                    'message': f'Generated unique name: {result}'
                }
            
            elif operation == "string_processing":
                text = kwargs.get('text', data)
                if not text:
                    raise ValueError("text is required for string_processing operation")
                
                result = utils.process_string(text, **kwargs)
                
                return {
                    'success': True,
                    'operation': 'string_processing',
                    'result': result,
                    'original_text': text,
                    'message': f'String processed successfully'
                }
            
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            if self.context:
                self.context.utils_processor = utils
                
        except Exception as e:
            logger.error(f"Failed to execute utils processor: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Utils processor execution failed'
            }


class Utils:
    """
    Utility class for string processing and data manipulation.
    Refactored from utils.utils but keeping core functionality.
    """
    
    @classmethod
    def unique_value(cls, the_enum, adapted_value):
        """
        Generate a unique value for an enumeration.
        
        If the adapted value already exists in the enum then append it with _x2
        If that string appended with _x2 already exists, then append with _x3 instead
        If that exists then _x4 etc.
        
        Args:
            the_enum: The enumeration object
            adapted_value (str): The value to make unique
            
        Returns:
            str: Unique adapted value
        """
        new_adapted_value = adapted_value
        if cls.contains_literal(the_enum.eLiterals, adapted_value):
            new_adapted_value = adapted_value + "_x2"
        
        counter = 1
        finished = False
        # Within the bird data model there is re-use of the same id or name
        # for multiple members, which is not ideal. For a very small number
        # of domains this is in the hundreds or over one thousand,
        # which is why we need a high limit here.
        # It would be better if BIRD addressed this repetition.
        # It is particularly noticeable in NUTS and NACE codes.
        # This high limit increases the processing time from under 1 minute
        # to a few minutes for the full BIRD data model.
        limit = 32
        
        while (counter < limit) and not finished:
            counter = counter + 1
            if cls.contains_literal(the_enum.eLiterals, adapted_value + "_x" + str(counter)):
                new_adapted_value = adapted_value + "_x" + str(counter+1)
            else:
                finished = True

        return new_adapted_value

    @classmethod
    def unique_name(cls, the_enum, enum_used_name):
        """
        Generate a unique name for an enumeration.
        
        If the adapted name already exists in the enum then append it with _x2
        If that string appended with _x2 already exists, then append with _x3 instead
        If that exists then _x4 etc.
        
        Args:
            the_enum: The enumeration object
            enum_used_name (str): The name to make unique
            
        Returns:
            str: Unique adapted name
        """
        new_adapted_name = enum_used_name
        counter = 1
        finished = False
        limit = 32
        
        if cls.contains_name(the_enum.eLiterals, enum_used_name):
            new_adapted_name = enum_used_name + "_x2"

        while (counter < limit) and not finished:
            counter = counter + 1
            if cls.contains_name(the_enum.eLiterals, enum_used_name + "_x" + str(counter)):
                new_adapted_name = enum_used_name + "_x" + str(counter+1)
            else:
                finished = True

        return new_adapted_name

    @classmethod
    def contains_literal(cls, literals, value):
        """
        Check if a literal value exists in the collection.
        
        Args:
            literals: Collection of literal objects
            value (str): Value to check for
            
        Returns:
            bool: True if value exists, False otherwise
        """
        for literal in literals:
            if hasattr(literal, 'literal') and literal.literal == value:
                return True
            elif hasattr(literal, 'value') and literal.value == value:
                return True
        return False

    @classmethod
    def contains_name(cls, literals, name):
        """
        Check if a name exists in the collection.
        
        Args:
            literals: Collection of literal objects
            name (str): Name to check for
            
        Returns:
            bool: True if name exists, False otherwise
        """
        for literal in literals:
            if hasattr(literal, 'name') and literal.name == name:
                return True
        return False

    @classmethod
    def process_string(cls, text: str, remove_accents: bool = True, 
                      to_lower: bool = False, to_upper: bool = False) -> str:
        """
        Process a string with various transformations.
        
        Args:
            text (str): Text to process
            remove_accents (bool): Whether to remove accents using unidecode
            to_lower (bool): Whether to convert to lowercase
            to_upper (bool): Whether to convert to uppercase
            
        Returns:
            str: Processed text
        """
        if not text:
            return text
        
        result = text
        
        if remove_accents:
            result = unidecode.unidecode(result)
        
        if to_lower:
            result = result.lower()
        elif to_upper:
            result = result.upper()
        
        return result

    @classmethod
    def sanitize_identifier(cls, identifier: str) -> str:
        """
        Sanitize a string to be used as a valid identifier.
        
        Args:
            identifier (str): String to sanitize
            
        Returns:
            str: Sanitized identifier
        """
        if not identifier:
            return ""
        
        # Remove accents and convert to ASCII
        sanitized = unidecode.unidecode(identifier)
        
        # Replace non-alphanumeric characters with underscores
        sanitized = ''.join(c if c.isalnum() else '_' for c in sanitized)
        
        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = 'id_' + sanitized
        
        # Remove multiple consecutive underscores
        while '__' in sanitized:
            sanitized = sanitized.replace('__', '_')
        
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        
        return sanitized or 'unnamed'

    @classmethod
    def truncate_string(cls, text: str, max_length: int = 100, 
                       suffix: str = "...") -> str:
        """
        Truncate a string to a maximum length.
        
        Args:
            text (str): Text to truncate
            max_length (int): Maximum length including suffix
            suffix (str): Suffix to add when truncating
            
        Returns:
            str: Truncated text
        """
        if not text or len(text) <= max_length:
            return text
        
        return text[:max_length - len(suffix)] + suffix

    @classmethod
    def validate_and_clean_data(cls, data: Dict[str, Any], 
                               required_fields: List[str] = None,
                               clean_strings: bool = True) -> Dict[str, Any]:
        """
        Validate and clean data dictionary.
        
        Args:
            data (dict): Data to validate and clean
            required_fields (list): List of required field names
            clean_strings (bool): Whether to clean string values
            
        Returns:
            dict: Cleaned data dictionary
            
        Raises:
            ValueError: If required fields are missing
        """
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary")
        
        if required_fields:
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")
        
        cleaned_data = {}
        
        for key, value in data.items():
            if isinstance(value, str) and clean_strings:
                cleaned_data[key] = cls.process_string(value, remove_accents=True).strip()
            else:
                cleaned_data[key] = value
        
        return cleaned_data