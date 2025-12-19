# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""
Standardized JSON Response Helpers

This module provides helper functions and classes for creating consistent
JSON responses across all API endpoints. Using these helpers ensures:

1. Consistent response structure across all endpoints
2. Proper error code categorization
3. Easy client-side error handling
4. Standardized success/error patterns

Response Structure:
{
    "success": bool,
    "data": dict | null,
    "error": {
        "message": str,
        "code": str
    } | null,
    **additional_fields
}

Error Codes:
- VALIDATION_ERROR: Input validation failed
- AUTH_ERROR: Authentication or authorization failed
- NOT_FOUND: Resource not found
- CONFLICT: Resource conflict (e.g., already exists)
- RATE_LIMIT: Rate limit exceeded
- SERVER_ERROR: Internal server error
- NETWORK_ERROR: External service connection failed
- TIMEOUT_ERROR: Operation timed out
- UNKNOWN_ERROR: Uncategorized error
"""

from django.http import JsonResponse
from typing import Dict, Any, Optional


class ErrorCode:
    """Standard error codes for API responses."""
    VALIDATION_ERROR = "VALIDATION_ERROR"
    AUTH_ERROR = "AUTH_ERROR"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    RATE_LIMIT = "RATE_LIMIT"
    SERVER_ERROR = "SERVER_ERROR"
    NETWORK_ERROR = "NETWORK_ERROR"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    BAD_REQUEST = "BAD_REQUEST"


class StandardJsonResponse:
    """
    Helper class for creating standardized JSON responses.

    Usage:
        # Success response
        return StandardJsonResponse.success(
            data={'user': 'john'},
            message='User fetched successfully'
        )

        # Error response
        return StandardJsonResponse.error(
            message='User not found',
            code=ErrorCode.NOT_FOUND,
            status=404
        )

        # Error with details
        return StandardJsonResponse.error(
            message='Validation failed',
            code=ErrorCode.VALIDATION_ERROR,
            details={'field': 'email', 'issue': 'Invalid format'},
            status=400
        )
    """

    @staticmethod
    def success(
        data: Optional[Dict[str, Any]] = None,
        message: Optional[str] = None,
        **kwargs
    ) -> JsonResponse:
        """
        Create a successful JSON response.

        Args:
            data: Response data payload
            message: Optional success message
            **kwargs: Additional fields to include in response

        Returns:
            JsonResponse with success=True
        """
        response = {
            'success': True,
            'data': data,
            'error': None
        }

        if message:
            response['message'] = message

        response.update(kwargs)

        return JsonResponse(response)

    @staticmethod
    def error(
        message: str,
        code: str = ErrorCode.UNKNOWN_ERROR,
        status: int = 400,
        details: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> JsonResponse:
        """
        Create an error JSON response.

        Args:
            message: Human-readable error message
            code: Error code from ErrorCode class
            status: HTTP status code (default: 400)
            details: Optional additional error details
            **kwargs: Additional fields to include in response

        Returns:
            JsonResponse with success=False
        """
        error_obj = {
            'message': message,
            'code': code
        }

        if details:
            error_obj['details'] = details

        response = {
            'success': False,
            'data': None,
            'error': error_obj
        }

        response.update(kwargs)

        return JsonResponse(response, status=status)

    @staticmethod
    def validation_error(
        message: str,
        field: Optional[str] = None,
        **kwargs
    ) -> JsonResponse:
        """
        Create a validation error response.

        Args:
            message: Validation error message
            field: Optional field name that failed validation
            **kwargs: Additional fields

        Returns:
            JsonResponse with validation error
        """
        details = {'field': field} if field else None
        return StandardJsonResponse.error(
            message=message,
            code=ErrorCode.VALIDATION_ERROR,
            status=400,
            details=details,
            **kwargs
        )

    @staticmethod
    def not_found(
        message: str = "Resource not found",
        resource_type: Optional[str] = None,
        **kwargs
    ) -> JsonResponse:
        """
        Create a not found error response.

        Args:
            message: Error message
            resource_type: Type of resource that wasn't found
            **kwargs: Additional fields

        Returns:
            JsonResponse with 404 status
        """
        details = {'resource_type': resource_type} if resource_type else None
        return StandardJsonResponse.error(
            message=message,
            code=ErrorCode.NOT_FOUND,
            status=404,
            details=details,
            **kwargs
        )

    @staticmethod
    def auth_error(
        message: str = "Authentication required",
        **kwargs
    ) -> JsonResponse:
        """
        Create an authentication error response.

        Args:
            message: Error message
            **kwargs: Additional fields

        Returns:
            JsonResponse with 401 status
        """
        return StandardJsonResponse.error(
            message=message,
            code=ErrorCode.AUTH_ERROR,
            status=401,
            **kwargs
        )

    @staticmethod
    def permission_denied(
        message: str = "Permission denied",
        **kwargs
    ) -> JsonResponse:
        """
        Create a permission denied error response.

        Args:
            message: Error message
            **kwargs: Additional fields

        Returns:
            JsonResponse with 403 status
        """
        return StandardJsonResponse.error(
            message=message,
            code=ErrorCode.PERMISSION_DENIED,
            status=403,
            **kwargs
        )

    @staticmethod
    def server_error(
        message: str = "An internal error occurred",
        **kwargs
    ) -> JsonResponse:
        """
        Create a server error response.

        Args:
            message: Error message
            **kwargs: Additional fields

        Returns:
            JsonResponse with 500 status
        """
        return StandardJsonResponse.error(
            message=message,
            code=ErrorCode.SERVER_ERROR,
            status=500,
            **kwargs
        )

    @staticmethod
    def from_exception(
        exception: Exception,
        default_message: str = "An error occurred"
    ) -> JsonResponse:
        """
        Create an error response from an exception.

        Maps common exception types to appropriate error codes and statuses.

        Args:
            exception: The exception that occurred
            default_message: Default message if exception has no str representation

        Returns:
            JsonResponse with appropriate error
        """
        import requests.exceptions

        message = str(exception) if str(exception) else default_message

        # Map exception types to error codes and statuses
        if isinstance(exception, requests.exceptions.Timeout):
            return StandardJsonResponse.error(
                message="Request timed out",
                code=ErrorCode.TIMEOUT_ERROR,
                status=504
            )
        elif isinstance(exception, requests.exceptions.ConnectionError):
            return StandardJsonResponse.error(
                message="Connection failed",
                code=ErrorCode.NETWORK_ERROR,
                status=503
            )
        elif isinstance(exception, ValueError):
            return StandardJsonResponse.error(
                message=message,
                code=ErrorCode.VALIDATION_ERROR,
                status=400
            )
        elif isinstance(exception, PermissionError):
            return StandardJsonResponse.error(
                message=message,
                code=ErrorCode.PERMISSION_DENIED,
                status=403
            )
        elif isinstance(exception, FileNotFoundError):
            return StandardJsonResponse.error(
                message=message,
                code=ErrorCode.NOT_FOUND,
                status=404
            )
        else:
            return StandardJsonResponse.error(
                message=message,
                code=ErrorCode.SERVER_ERROR,
                status=500
            )
