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
#    Security fixes for information disclosure through exceptions
#

import logging
from typing import Any, Dict, Optional
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponseServerError
from django.db import DatabaseError, IntegrityError, OperationalError
from django.contrib import messages

logger = logging.getLogger(__name__)

class SecureErrorHandler:
    """
    Utility class for secure error handling that prevents information disclosure.
    """
    
    # Generic error messages that don't reveal implementation details
    GENERIC_MESSAGES = {
        'database_error': 'A database error occurred. Please try again later.',
        'file_error': 'File operation failed. Please check the file and try again.',
        'validation_error': 'Invalid input provided. Please correct and try again.',
        'permission_error': 'Access denied. You do not have permission to perform this operation.',
        'not_found': 'The requested resource was not found.',
        'internal_error': 'An internal error occurred. Please try again later.',
        'upload_error': 'File upload failed. Please check the file and try again.',
        'processing_error': 'Processing failed. Please try again later.',
        'configuration_error': 'Configuration error. Please contact the administrator.',
        'timeout_error': 'The operation timed out. Please try again.',
    }
    
    @staticmethod
    def handle_exception(exception: Exception, context: Optional[str] = None, 
                        request=None) -> Dict[str, Any]:
        """
        Handle exception securely by logging full details internally and returning safe message.
        
        Args:
            exception: The exception that occurred
            context: Optional context description for logging
            request: Django request object (optional)
            
        Returns:
            Dict with safe error information for user
        """
        # Log full exception details for developers (not visible to users)
        log_message = f"Exception in {context or 'unknown context'}: {str(exception)}"
        logger.error(log_message, exc_info=True)
        
        # Determine appropriate user-safe message based on exception type
        if isinstance(exception, (DatabaseError, IntegrityError, OperationalError)):
            user_message = SecureErrorHandler.GENERIC_MESSAGES['database_error']
        elif isinstance(exception, FileNotFoundError):
            user_message = SecureErrorHandler.GENERIC_MESSAGES['file_error']
        elif isinstance(exception, PermissionError):
            user_message = SecureErrorHandler.GENERIC_MESSAGES['permission_error']
        elif isinstance(exception, ValueError):
            user_message = SecureErrorHandler.GENERIC_MESSAGES['validation_error']
        elif isinstance(exception, TimeoutError):
            user_message = SecureErrorHandler.GENERIC_MESSAGES['timeout_error']
        else:
            user_message = SecureErrorHandler.GENERIC_MESSAGES['internal_error']
        
        return {
            'status': 'error',
            'message': user_message,
            'error_id': id(exception)  # Can be used for support ticket reference
        }
    
    @staticmethod
    def secure_json_response(exception: Exception, context: Optional[str] = None,
                           request=None) -> JsonResponse:
        """
        Return a secure JSON error response.
        
        Args:
            exception: The exception that occurred
            context: Optional context description
            request: Django request object (optional)
            
        Returns:
            JsonResponse with safe error message
        """
        error_data = SecureErrorHandler.handle_exception(exception, context, request)
        return JsonResponse(error_data, status=500)
    
    @staticmethod
    def secure_http_response(exception: Exception, context: Optional[str] = None,
                           request=None) -> HttpResponseServerError:
        """
        Return a secure HTTP error response.
        
        Args:
            exception: The exception that occurred
            context: Optional context description
            request: Django request object (optional)
            
        Returns:
            HttpResponseServerError with safe error message
        """
        error_data = SecureErrorHandler.handle_exception(exception, context, request)
        return HttpResponseServerError(error_data['message'])
    
    @staticmethod
    def secure_message(request, exception: Exception, context: Optional[str] = None):
        """
        Add a secure error message to Django's messages framework.
        
        Args:
            request: Django request object
            exception: The exception that occurred
            context: Optional context description
        """
        error_data = SecureErrorHandler.handle_exception(exception, context, request)
        messages.error(request, error_data['message'])
    
    @staticmethod
    def log_security_event(event_type: str, details: str, request=None, 
                          severity: str = 'WARNING'):
        """
        Log security-related events for monitoring and alerting.
        
        Args:
            event_type: Type of security event (e.g., 'path_traversal_attempt')
            details: Details of the event
            request: Django request object (optional)
            severity: Log severity level
        """
        log_message = f"SECURITY EVENT [{event_type}]: {details}"
        
        if request:
            log_message += f" - IP: {request.META.get('REMOTE_ADDR', 'unknown')}"
            log_message += f" - User: {getattr(request.user, 'username', 'anonymous')}"
        
        getattr(logger, severity.lower())(log_message)
    
    @staticmethod
    def sanitize_user_input_error(user_input: str, max_length: int = 50) -> str:
        """
        Sanitize user input for safe inclusion in error messages.
        
        Args:
            user_input: User-provided input
            max_length: Maximum length to include in error message
            
        Returns:
            Sanitized input safe for error messages
        """
        if not user_input:
            return "[empty]"
        
        # Truncate and remove potentially dangerous characters
        safe_input = str(user_input)[:max_length]
        # Remove HTML/script tags and path separators
        safe_input = safe_input.replace('<', '&lt;').replace('>', '&gt;')
        safe_input = safe_input.replace('/', '_').replace('\\', '_')
        safe_input = safe_input.replace('..', '_')
        
        return safe_input

class DatabaseErrorHandler:
    """
    Specialized handler for database-related errors.
    """
    
    @staticmethod
    def handle_database_error(exception: Exception, operation: str = "database operation",
                            request=None) -> Dict[str, Any]:
        """
        Handle database errors securely.
        
        Args:
            exception: Database exception
            operation: Description of the database operation
            request: Django request object (optional)
            
        Returns:
            Safe error response data
        """
        # Log detailed database error for developers
        logger.error(f"Database error during {operation}: {str(exception)}", 
                    exc_info=True)
        
        # Return generic message to prevent database schema disclosure
        return {
            'status': 'error',
            'message': 'Database operation failed. Please try again later.',
            'operation': operation.replace(str(exception), '[redacted]')
        }
    
    @staticmethod
    def handle_integrity_error(exception: IntegrityError, context: str = "operation",
                             request=None) -> Dict[str, Any]:
        """
        Handle database integrity errors (like unique constraint violations).
        
        Args:
            exception: IntegrityError exception
            context: Context of the operation
            request: Django request object (optional)
            
        Returns:
            Safe error response data
        """
        logger.error(f"Integrity error during {context}: {str(exception)}", 
                    exc_info=True)
        
        # Don't reveal constraint names or table structures
        return {
            'status': 'error',
            'message': 'Data validation failed. The operation conflicts with existing data.',
            'context': context
        }

class FileOperationErrorHandler:
    """
    Specialized handler for file operation errors.
    """
    
    @staticmethod
    def handle_file_error(exception: Exception, operation: str = "file operation",
                         filename: Optional[str] = None, request=None) -> Dict[str, Any]:
        """
        Handle file operation errors securely.
        
        Args:
            exception: File operation exception
            operation: Description of the file operation
            filename: Optional filename (will be sanitized)
            request: Django request object (optional)
            
        Returns:
            Safe error response data
        """
        # Log detailed error for developers (with full path if needed)
        log_msg = f"File error during {operation}: {str(exception)}"
        if filename:
            log_msg += f" - File: {filename}"
        logger.error(log_msg, exc_info=True)
        
        # Return safe message without revealing file paths
        safe_filename = SecureErrorHandler.sanitize_user_input_error(filename) if filename else "file"
        
        if isinstance(exception, FileNotFoundError):
            message = f"The requested {safe_filename} was not found."
        elif isinstance(exception, PermissionError):
            message = f"Access denied to {safe_filename}."
        else:
            message = f"File operation failed for {safe_filename}."
        
        return {
            'status': 'error',
            'message': message,
            'operation': operation
        }