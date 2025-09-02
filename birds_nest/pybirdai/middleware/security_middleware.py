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
#    Security middleware for information disclosure prevention
#

import logging
from django.http import JsonResponse, HttpResponseServerError
from django.conf import settings
from django.utils.deprecation import MiddlewareMixin

logger = logging.getLogger(__name__)

class SecurityErrorMiddleware(MiddlewareMixin):
    """
    Middleware to prevent information disclosure through error responses,
    especially in production environments.
    """
    
    def process_exception(self, request, exception):
        """
        Process exceptions before they are handled by Django's error handling.
        This prevents sensitive information from being exposed to users.
        """
        # Log the full exception details for developers
        logger.error(
            f"Unhandled exception in {request.path}: {str(exception)}",
            exc_info=True,
            extra={
                'request_path': request.path,
                'request_method': request.method,
                'user': getattr(request.user, 'username', 'anonymous') if hasattr(request, 'user') else 'unknown',
                'remote_addr': request.META.get('REMOTE_ADDR', 'unknown'),
                'user_agent': request.META.get('HTTP_USER_AGENT', 'unknown')[:200]
            }
        )
        
        # In production, return generic error messages
        if not settings.DEBUG:
            # Check if this is an AJAX request
            is_ajax = (
                request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest' or
                request.content_type == 'application/json' or
                'application/json' in request.META.get('HTTP_ACCEPT', '')
            )
            
            if is_ajax:
                return JsonResponse({
                    'status': 'error',
                    'message': 'An internal error occurred. Please try again later.',
                    'error_code': 'INTERNAL_ERROR'
                }, status=500)
            else:
                return HttpResponseServerError(
                    'An internal error occurred. Please try again later.'
                )
        
        # In debug mode, let Django handle the exception normally
        return None
    
    def process_response(self, request, response):
        """
        Process responses to ensure no sensitive information is leaked.
        """
        # Check for responses that might contain sensitive error information
        if response.status_code >= 400:
            # Log security-relevant 4xx errors
            if response.status_code in [401, 403, 404]:
                logger.warning(
                    f"Security-relevant response {response.status_code} for {request.path}",
                    extra={
                        'request_path': request.path,
                        'status_code': response.status_code,
                        'user': getattr(request.user, 'username', 'anonymous') if hasattr(request, 'user') else 'unknown',
                        'remote_addr': request.META.get('REMOTE_ADDR', 'unknown')
                    }
                )
        
        return response

class SecurityHeadersMiddleware(MiddlewareMixin):
    """
    Middleware to add security headers to responses.
    """
    
    def process_response(self, request, response):
        """
        Add security headers to prevent information disclosure and other attacks.
        """
        # Prevent information disclosure through server headers
        if 'Server' in response:
            del response['Server']
        
        # Add security headers if not already present
        security_headers = {
            'X-Content-Type-Options': 'nosniff',
            'X-Frame-Options': 'SAMEORIGIN',
            'X-XSS-Protection': '1; mode=block',
            'Referrer-Policy': 'strict-origin-when-cross-origin',
        }
        
        for header, value in security_headers.items():
            if header not in response:
                response[header] = value
        
        # In production, add additional security headers
        if not settings.DEBUG:
            if 'Content-Security-Policy' not in response:
                # Basic CSP to prevent inline scripts (can be customized)
                response['Content-Security-Policy'] = (
                    "default-src 'self'; "
                    "script-src 'self' 'unsafe-inline'; "
                    "style-src 'self' 'unsafe-inline'; "
                    "img-src 'self' data:; "
                    "font-src 'self';"
                )
        
        return response

class RequestLoggingMiddleware(MiddlewareMixin):
    """
    Middleware to log suspicious requests that might be security-related.
    """
    
    SUSPICIOUS_PATTERNS = [
        '../', '..\\', '.env', 'wp-admin', 'admin.php', 'eval(',
        '<script', 'javascript:', 'vbscript:', 'onload=', 'onerror=',
        'SELECT * FROM', 'UNION SELECT', 'DROP TABLE', 'INSERT INTO'
    ]
    
    def process_request(self, request):
        """
        Log potentially suspicious requests for security monitoring.
        """
        # Check for suspicious patterns in the request
        suspicious_found = []
        
        # Check URL path
        for pattern in self.SUSPICIOUS_PATTERNS:
            if pattern.lower() in request.path.lower():
                suspicious_found.append(f"path:{pattern}")
        
        # Check query string
        query_string = request.META.get('QUERY_STRING', '')
        for pattern in self.SUSPICIOUS_PATTERNS:
            if pattern.lower() in query_string.lower():
                suspicious_found.append(f"query:{pattern}")
        
        # Check POST data (if it's form data)
        if request.method == 'POST' and request.content_type == 'application/x-www-form-urlencoded':
            try:
                post_data = request.body.decode('utf-8', errors='ignore').lower()
                for pattern in self.SUSPICIOUS_PATTERNS:
                    if pattern.lower() in post_data:
                        suspicious_found.append(f"post:{pattern}")
            except:
                pass  # Ignore decoding errors
        
        # Log suspicious requests
        if suspicious_found:
            logger.warning(
                f"Suspicious request detected: {', '.join(suspicious_found)}",
                extra={
                    'request_path': request.path,
                    'request_method': request.method,
                    'remote_addr': request.META.get('REMOTE_ADDR', 'unknown'),
                    'user_agent': request.META.get('HTTP_USER_AGENT', 'unknown')[:200],
                    'suspicious_patterns': suspicious_found
                }
            )
        
        return None