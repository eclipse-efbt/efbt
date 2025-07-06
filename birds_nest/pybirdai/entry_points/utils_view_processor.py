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
from django.http import JsonResponse, HttpResponse
import logging

logger = logging.getLogger(__name__)


class UtilsViewProcessorConfig(AppConfig):
    """
    Django AppConfig for Utils View Processing operations.
    Provides view utility functions and helper methods for Django views.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'pybirdai.entry_points.utils_view_processor'
    verbose_name = 'Utils View Processor'

    def ready(self):
        """Initialize the utils view processor when Django starts."""
        logger.info("Utils View Processor initialized")


def create_success_response(data=None, message="Success", status=200):
    """
    Create a standardized success response.
    
    Args:
        data: Optional data to include in response
        message (str): Success message
        status (int): HTTP status code
        
    Returns:
        JsonResponse: Formatted success response
    """
    try:
        response_data = {
            'success': True,
            'message': message,
            'status': status
        }
        
        if data is not None:
            response_data['data'] = data
        
        logger.debug(f"Created success response: {message}")
        return JsonResponse(response_data, status=status)
        
    except Exception as e:
        logger.error(f"Failed to create success response: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e),
            'message': 'Failed to create response'
        }, status=500)


def create_error_response(error_message, status=400, error_code=None):
    """
    Create a standardized error response.
    
    Args:
        error_message (str): Error message
        status (int): HTTP status code
        error_code (str): Optional error code
        
    Returns:
        JsonResponse: Formatted error response
    """
    try:
        response_data = {
            'success': False,
            'error': error_message,
            'status': status
        }
        
        if error_code:
            response_data['error_code'] = error_code
        
        logger.warning(f"Created error response: {error_message}")
        return JsonResponse(response_data, status=status)
        
    except Exception as e:
        logger.error(f"Failed to create error response: {e}")
        return JsonResponse({
            'success': False,
            'error': 'Internal server error',
            'message': 'Failed to create error response'
        }, status=500)


def validate_request_data(request, required_fields=None, optional_fields=None):
    """
    Validate incoming request data.
    
    Args:
        request: Django request object
        required_fields (list): List of required field names
        optional_fields (list): List of optional field names
        
    Returns:
        dict: Validation result with success status and data
    """
    try:
        if required_fields is None:
            required_fields = []
        if optional_fields is None:
            optional_fields = []
        
        # Get request data based on method
        if request.method == 'POST':
            data = request.POST.dict()
        elif request.method == 'GET':
            data = request.GET.dict()
        else:
            try:
                import json
                data = json.loads(request.body.decode('utf-8'))
            except:
                data = {}
        
        validation_result = {
            'valid': True,
            'data': data,
            'missing_fields': [],
            'errors': []
        }
        
        # Check required fields
        for field in required_fields:
            if field not in data or not data[field]:
                validation_result['missing_fields'].append(field)
                validation_result['valid'] = False
        
        # Validate field formats if needed
        validated_data = {}
        for field, value in data.items():
            if field in required_fields + optional_fields:
                validated_data[field] = value
        
        validation_result['validated_data'] = validated_data
        
        if not validation_result['valid']:
            validation_result['errors'].append(f"Missing required fields: {', '.join(validation_result['missing_fields'])}")
        
        logger.debug(f"Request validation: {'Valid' if validation_result['valid'] else 'Invalid'}")
        
        return validation_result
        
    except Exception as e:
        logger.error(f"Request validation failed: {e}")
        return {
            'valid': False,
            'error': str(e),
            'errors': [f"Validation error: {str(e)}"]
        }


def handle_file_upload(request, file_field_name='file', allowed_extensions=None):
    """
    Handle file upload from request.
    
    Args:
        request: Django request object
        file_field_name (str): Name of file field in request
        allowed_extensions (list): List of allowed file extensions
        
    Returns:
        dict: Upload result with success status and file info
    """
    try:
        if allowed_extensions is None:
            allowed_extensions = ['.csv', '.txt', '.json', '.xml']
        
        upload_result = {
            'success': False,
            'file_info': {},
            'errors': []
        }
        
        if file_field_name not in request.FILES:
            upload_result['errors'].append(f"No file found in field '{file_field_name}'")
            return upload_result
        
        uploaded_file = request.FILES[file_field_name]
        
        # Validate file extension
        file_extension = uploaded_file.name.split('.')[-1].lower()
        if f'.{file_extension}' not in allowed_extensions:
            upload_result['errors'].append(f"File extension '.{file_extension}' not allowed")
            return upload_result
        
        # Get file info
        upload_result['file_info'] = {
            'name': uploaded_file.name,
            'size': uploaded_file.size,
            'content_type': uploaded_file.content_type,
            'extension': file_extension
        }
        
        upload_result['success'] = True
        upload_result['file'] = uploaded_file
        
        logger.info(f"File upload successful: {uploaded_file.name}")
        return upload_result
        
    except Exception as e:
        logger.error(f"File upload failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'errors': [f"Upload error: {str(e)}"]
        }


def paginate_queryset(queryset, page_number=1, page_size=50):
    """
    Paginate a Django queryset.
    
    Args:
        queryset: Django queryset to paginate
        page_number (int): Page number to retrieve
        page_size (int): Number of items per page
        
    Returns:
        dict: Pagination result with paginated data
    """
    try:
        from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
        
        paginator = Paginator(queryset, page_size)
        
        try:
            page = paginator.page(page_number)
        except PageNotAnInteger:
            page = paginator.page(1)
        except EmptyPage:
            page = paginator.page(paginator.num_pages)
        
        pagination_result = {
            'success': True,
            'page_data': page.object_list,
            'pagination_info': {
                'current_page': page.number,
                'total_pages': paginator.num_pages,
                'page_size': page_size,
                'total_items': paginator.count,
                'has_previous': page.has_previous(),
                'has_next': page.has_next()
            }
        }
        
        if page.has_previous():
            pagination_result['pagination_info']['previous_page'] = page.previous_page_number()
        if page.has_next():
            pagination_result['pagination_info']['next_page'] = page.next_page_number()
        
        logger.debug(f"Paginated queryset: page {page.number} of {paginator.num_pages}")
        
        return pagination_result
        
    except Exception as e:
        logger.error(f"Pagination failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Pagination failed'
        }


def format_model_data_for_display(model_instance, fields=None, exclude_fields=None):
    """
    Format model instance data for display in views.
    
    Args:
        model_instance: Django model instance
        fields (list): Optional list of fields to include
        exclude_fields (list): Optional list of fields to exclude
        
    Returns:
        dict: Formatted model data
    """
    try:
        if fields is None:
            fields = []
        if exclude_fields is None:
            exclude_fields = ['password', 'token', 'secret']
        
        formatted_data = {}
        model_fields = model_instance._meta.get_fields()
        
        for field in model_fields:
            field_name = field.name
            
            # Skip excluded fields
            if field_name in exclude_fields:
                continue
            
            # Include only specified fields if provided
            if fields and field_name not in fields:
                continue
            
            try:
                field_value = getattr(model_instance, field_name)
                
                # Format special field types
                if hasattr(field_value, 'strftime'):  # DateTime fields
                    formatted_data[field_name] = field_value.strftime('%Y-%m-%d %H:%M:%S')
                elif hasattr(field_value, 'all'):  # Related managers
                    formatted_data[field_name] = [str(item) for item in field_value.all()]
                elif hasattr(field_value, 'pk'):  # Foreign keys
                    formatted_data[field_name] = {
                        'id': field_value.pk,
                        'display': str(field_value)
                    }
                else:
                    formatted_data[field_name] = field_value
                    
            except Exception as field_error:
                logger.warning(f"Failed to format field {field_name}: {field_error}")
                formatted_data[field_name] = None
        
        logger.debug(f"Formatted model data for {model_instance.__class__.__name__}")
        
        return {
            'success': True,
            'formatted_data': formatted_data,
            'model_name': model_instance.__class__.__name__
        }
        
    except Exception as e:
        logger.error(f"Model data formatting failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Model data formatting failed'
        }


def create_context_data(request, additional_data=None):
    """
    Create context data for template rendering.
    
    Args:
        request: Django request object
        additional_data (dict): Additional data to include in context
        
    Returns:
        dict: Context data for template rendering
    """
    try:
        context_data = {
            'request': request,
            'user': request.user if hasattr(request, 'user') else None,
            'request_method': request.method,
            'is_ajax': request.headers.get('X-Requested-With') == 'XMLHttpRequest'
        }
        
        # Add additional data if provided
        if additional_data and isinstance(additional_data, dict):
            context_data.update(additional_data)
        
        logger.debug("Created context data for template rendering")
        
        return context_data
        
    except Exception as e:
        logger.error(f"Context data creation failed: {e}")
        return {
            'error': str(e),
            'message': 'Context data creation failed'
        }


def handle_ajax_request(request, success_callback=None, error_callback=None):
    """
    Handle AJAX requests with standardized responses.
    
    Args:
        request: Django request object
        success_callback: Function to call on success
        error_callback: Function to call on error
        
    Returns:
        JsonResponse: Standardized AJAX response
    """
    try:
        if not request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return create_error_response("Not an AJAX request", status=400)
        
        result = {
            'success': True,
            'data': {},
            'message': 'AJAX request processed successfully'
        }
        
        # Execute success callback if provided
        if success_callback and callable(success_callback):
            try:
                callback_result = success_callback(request)
                if isinstance(callback_result, dict):
                    result.update(callback_result)
            except Exception as callback_error:
                logger.error(f"Success callback failed: {callback_error}")
                if error_callback and callable(error_callback):
                    error_result = error_callback(request, callback_error)
                    if isinstance(error_result, dict):
                        result.update(error_result)
                        result['success'] = False
        
        logger.debug("AJAX request handled successfully")
        
        if result.get('success'):
            return create_success_response(result.get('data'), result.get('message'))
        else:
            return create_error_response(result.get('error', 'AJAX request failed'))
        
    except Exception as e:
        logger.error(f"AJAX request handling failed: {e}")
        return create_error_response(f"AJAX request failed: {str(e)}", status=500)


class UtilsViewProcessor:
    """
    Main utils view processor class providing high-level view utility interface.
    """
    
    def __init__(self):
        """Initialize the utils view processor."""
        logger.info("UtilsViewProcessor initialized")
    
    def process_form_submission(self, request, form_class, success_url=None):
        """
        Process form submission with validation.
        
        Args:
            request: Django request object
            form_class: Django form class to use
            success_url (str): URL to redirect on success
            
        Returns:
            dict: Form processing results
        """
        try:
            form_result = {
                'success': False,
                'form': None,
                'errors': {},
                'cleaned_data': {}
            }
            
            if request.method == 'POST':
                form = form_class(request.POST, request.FILES)
                
                if form.is_valid():
                    form_result['success'] = True
                    form_result['cleaned_data'] = form.cleaned_data
                    form_result['form'] = form
                    
                    if success_url:
                        form_result['redirect_url'] = success_url
                    
                    logger.info("Form submission processed successfully")
                else:
                    form_result['errors'] = form.errors
                    form_result['form'] = form
                    logger.warning("Form validation failed")
            else:
                form_result['form'] = form_class()
            
            return form_result
            
        except Exception as e:
            logger.error(f"Form submission processing failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Form processing failed'
            }
    
    def create_table_view_data(self, queryset, columns=None, page_number=1, page_size=25):
        """
        Create data for table view rendering.
        
        Args:
            queryset: Django queryset
            columns (list): List of column names to display
            page_number (int): Page number for pagination
            page_size (int): Number of items per page
            
        Returns:
            dict: Table view data
        """
        try:
            # Paginate queryset
            pagination_result = paginate_queryset(queryset, page_number, page_size)
            
            if not pagination_result.get('success'):
                return pagination_result
            
            # Format data for display
            formatted_rows = []
            for item in pagination_result['page_data']:
                formatted_item = format_model_data_for_display(item, fields=columns)
                if formatted_item.get('success'):
                    formatted_rows.append(formatted_item['formatted_data'])
            
            table_data = {
                'success': True,
                'rows': formatted_rows,
                'columns': columns or [],
                'pagination': pagination_result['pagination_info']
            }
            
            logger.debug(f"Created table view data with {len(formatted_rows)} rows")
            
            return table_data
            
        except Exception as e:
            logger.error(f"Table view data creation failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Table view data creation failed'
            }


# Convenience function for backwards compatibility
def run_utils_view_operations():
    """Get a configured utils view processor instance."""
    return UtilsViewProcessor()


# Export main functions for easy access
__all__ = [
    'UtilsViewProcessorConfig',
    'create_success_response',
    'create_error_response',
    'validate_request_data',
    'handle_file_upload',
    'paginate_queryset',
    'format_model_data_for_display',
    'create_context_data',
    'handle_ajax_request',
    'UtilsViewProcessor',
    'run_utils_view_operations'
]