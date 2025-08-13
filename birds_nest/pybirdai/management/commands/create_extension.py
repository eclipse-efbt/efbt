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

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from pathlib import Path
import os
import datetime
import re

class Command(BaseCommand):
    help = 'Create a new BIRD extension scaffold in the extensions directory'

    def add_arguments(self, parser):
        parser.add_argument(
            '--name',
            type=str,
            required=True,
            help='Name of the extension (e.g., "risk_analytics")'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Overwrite existing extension if it exists'
        )

    def handle(self, *args, **options):
        extension_name = options['name']
        
        # Validate extension name
        if not re.match(r'^[a-z_][a-z0-9_]*$', extension_name):
            raise CommandError(
                'Extension name must be a valid Python identifier (lowercase, underscores allowed)'
            )

        # Set up paths
        base_dir = Path(settings.BASE_DIR)
        extensions_dir = base_dir / 'extensions'
        extension_path = extensions_dir / extension_name

        # Check if extension already exists
        if extension_path.exists() and not options['force']:
            raise CommandError(
                f'Extension "{extension_name}" already exists. Use --force to overwrite.'
            )

        # Create extensions directory if it doesn't exist
        extensions_dir.mkdir(exist_ok=True)

        # Remove existing extension if force is used
        if extension_path.exists() and options['force']:
            import shutil
            shutil.rmtree(extension_path)
            self.stdout.write(f'Removed existing extension "{extension_name}"')

        try:
            self.stdout.write(f'Creating extension "{extension_name}"...')

            # Create extension directory structure
            extension_path.mkdir()
            (extension_path / 'templates' / extension_name).mkdir(parents=True)
            (extension_path / 'static' / extension_name / 'css').mkdir(parents=True)
            (extension_path / 'static' / extension_name / 'js').mkdir(parents=True)
            (extension_path / 'process_steps').mkdir()
            (extension_path / 'tests').mkdir()

            # Get template variables
            context = self._get_template_context(extension_name, options)

            # Generate all extension files
            self._create_init_file(extension_path, context)
            self._create_views_file(extension_path, context)
            self._create_urls_file(extension_path, context)
            self._create_entry_point_file(extension_path, context)
            self._create_configuration_file(extension_path, context)
            self._create_models_file(extension_path, context)
            self._create_manifest_file(extension_path, context)
            self._create_templates(extension_path, context)
            self._create_static_files(extension_path, context)
            self._create_process_steps(extension_path, context)
            self._create_tests(extension_path, context)
            self._create_readme(extension_path, context)

            self.stdout.write(
                self.style.SUCCESS(
                    f'✅ Extension "{extension_name}" created successfully!\n\n'
                    f'Location: {extension_path}\n\n'
                    f'Next steps:\n'
                    f'  1. Implement your business logic in process_steps/\n'
                    f'  2. Add your views and templates\n'
                    f'  3. Test with: python manage.py runserver\n'
                    f'  4. Access at: http://localhost:8000/pybirdai/extensions/{extension_name}/\n'
                    f'  5. Package with: python manage.py package_extension --name {extension_name}'
                )
            )

        except Exception as e:
            # Clean up on failure
            if extension_path.exists():
                import shutil
                shutil.rmtree(extension_path)
            raise CommandError(f'Failed to create extension: {str(e)}')

    def _get_template_context(self, extension_name, options):
        """Get template context variables"""
        # Use sensible defaults directly
        display_name = extension_name.replace('_', ' ').title()
        description = f'{display_name} extension for BIRD Bench'
        
        return {
            'extension_name': extension_name,
            'display_name': display_name,
            'description': description,
            'author': 'Extension Developer',
            'author_email': 'developer@example.com',
            'version': '1.0.0',
            'license': 'EPL-2.0',
            'extension_type': 'public',
            'year': datetime.datetime.now().year,
            'date': datetime.datetime.now().isoformat(),
            'class_name': ''.join(word.capitalize() for word in extension_name.split('_'))
        }

    def _create_init_file(self, extension_path, context):
        """Create __init__.py file"""
        content = f'''"""
{context['description']}
"""

__version__ = "{context['version']}"
__author__ = "{context['author']}"
'''
        self._write_file(extension_path / '__init__.py', content)

    def _create_views_file(self, extension_path, context):
        """Create views.py file"""
        content = f'''from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib import messages
import json
from .extension_configuration import verify_license

def {context['extension_name']}_dashboard(request):
    """Main dashboard for {context['display_name']}"""
    if not verify_license():
        messages.error(request, "Invalid license for {context['display_name']} extension")
        return render(request, 'pybirdai/home.html')
    
    context = {{
        'extension_name': '{context['display_name']}',
        'version': '{context['version']}',
        'description': '{context['description']}'
    }}
    return render(request, '{context['extension_name']}/dashboard.html', context)

@csrf_exempt
def api_endpoint(request):
    """API endpoint for {context['display_name']}"""
    if not verify_license():
        return JsonResponse({{'error': 'Invalid license'}}, status=403)
    
    if request.method != 'POST':
        return JsonResponse({{'error': 'Method not allowed'}}, status=405)
    
    try:
        # Add your API logic here
        data = json.loads(request.body)
        
        # Example processing
        result = {{
            'status': 'success',
            'message': 'API endpoint working',
            'received_data': data
        }}
        
        return JsonResponse(result)
        
    except Exception as e:
        return JsonResponse({{'status': 'error', 'message': str(e)}}, status=500)
'''
        self._write_file(extension_path / 'views.py', content)

    def _create_urls_file(self, extension_path, context):
        """Create urls.py file"""
        content = f'''from django.urls import path
from . import views

app_name = '{context['extension_name']}'

urlpatterns = [
    path('', views.{context['extension_name']}_dashboard, name='dashboard'),
    path('api/', views.api_endpoint, name='api'),
]
'''
        self._write_file(extension_path / 'urls.py', content)

    def _create_entry_point_file(self, extension_path, context):
        """Create entry_point.py file"""
        content = f'''"""
Extension Entry Point
Registers the {context['display_name']} extension with Django
"""

from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)

class {context['class_name']}Config(AppConfig):
    """Django App configuration for {context['display_name']} extension"""
    
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'extensions.{context['extension_name']}'
    verbose_name = '{context['display_name']} Extension'
    
    def ready(self):
        """Called when Django starts"""
        from .extension_configuration import verify_license, initialize_extension
        
        try:
            # Verify license on startup
            if not verify_license():
                logger.warning("{context['display_name']}: License verification failed")
            else:
                logger.info("{context['display_name']}: License verified successfully")
            
            # Initialize extension components
            initialize_extension()
            
            logger.info(f"{{self.verbose_name}} loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize {{self.verbose_name}}: {{str(e)}}")
'''
        self._write_file(extension_path / 'entry_point.py', content)

    def _create_configuration_file(self, extension_path, context):
        """Create extension_configuration.py file"""
        content = f'''"""
Extension Configuration and License Verification
Handles licensing, configuration, and initialization for {context['display_name']}
"""

import os
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

# Extension configuration
EXTENSION_CONFIG = {{
    'name': '{context['extension_name']}',
    'version': '{context['version']}',
    'type': '{context['extension_type']}',
    'author': '{context['author']}',
}}

def verify_license():
    """Verify license for the extension"""
    # For development, always return True in DEBUG mode
    if settings.DEBUG:
        return True
    
    # In production, implement actual license verification
    # This would typically make an API call to a license server
    try:
        license_key = getattr(settings, f'{context['extension_name'].upper()}_LICENSE_KEY', 
                             os.environ.get(f'{context['extension_name'].upper()}_LICENSE_KEY'))
        
        if not license_key:
            logger.warning("No license key found for {context['extension_name']}")
            return False
        
        # TODO: Implement actual license verification API call
        # For now, return True if license key exists
        return bool(license_key)
        
    except Exception as e:
        logger.error(f"License verification error: {{str(e)}}")
        return False

def initialize_extension():
    """Initialize extension components"""
    logger.info(f"Initializing {{EXTENSION_CONFIG['name']}} v{{EXTENSION_CONFIG['version']}}")
    
    # Add any initialization logic here
    # Examples:
    # - Load extension-specific settings
    # - Initialize database connections
    # - Set up scheduled tasks
    # - Register signal handlers
    
    logger.info("Extension initialization complete")

def get_configuration():
    """Get extension configuration"""
    config = EXTENSION_CONFIG.copy()
    config['licensed'] = verify_license()
    config['debug_mode'] = settings.DEBUG
    return config
'''
        self._write_file(extension_path / 'extension_configuration.py', content)

    def _create_models_file(self, extension_path, context):
        """Create extension_models.py file"""
        content = f'''"""
Django Models for {context['display_name']} Extension
Define your database models here
"""

from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone

class {context['class_name']}Data(models.Model):
    """Example model for {context['display_name']} extension"""
    
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='{context['extension_name']}_data')
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)
    
    class Meta:
        db_table = '{context['extension_name']}_data'
        ordering = ['-created_at']
        verbose_name = '{context['display_name']} Data'
        verbose_name_plural = '{context['display_name']} Data'
    
    def __str__(self):
        return f"{{self.name}} ({{self.created_at.strftime('%Y-%m-%d')}})"

# Add more models as needed for your extension
'''
        self._write_file(extension_path / 'extension_models.py', content)

    def _create_manifest_file(self, extension_path, context):
        """Create extension_manifest.yaml file"""
        content = f'''name: {context['extension_name']}
version: {context['version']}
type: {context['extension_type']}
description: {context['description']}

metadata:
  display_name: {context['display_name']}
  author: {context['author']}
  author_email: {context['author_email']}
  license_type: {context['license']}
  created_date: "{context['date']}"

dependencies:
  python:
    - django>=4.0
  django:
    min_version: "4.0"
    max_version: "5.1"
  efbt_core:
    min_version: "2.0.0"

configuration:
  settings_prefix: {context['extension_name'].upper()}_
  environment_variables:
    - {context['extension_name'].upper()}_LICENSE_KEY
  database:
    requires_migration: true
    tables:
      - {context['extension_name']}_data

api:
  version: v1
  base_path: /pybirdai/extensions/{context['extension_name']}/api/
  endpoints:
    - path: /
      method: POST
      description: Main API endpoint

support:
  email: {context['author_email']}
  documentation: https://github.com/eclipse-efbt/efbt/wiki
'''
        self._write_file(extension_path / 'extension_manifest.yaml', content)

    def _create_templates(self, extension_path, context):
        """Create template files"""
        dashboard_template = f'''{{% extends 'base.html' %}}

{{% block title %}}{context['display_name']} - BIRD Bench{{% endblock %}}

{{% block content %}}
<div class="container-fluid">
    <div class="row">
        <div class="col-md-12">
            <h1 class="page-header">
                <i class="fas fa-puzzle-piece"></i> {{{{ extension_name }}}}
                <span class="badge badge-info">v{{{{ version }}}}</span>
            </h1>
            
            <div class="alert alert-info">
                <strong>Extension:</strong> {{{{ description }}}}
            </div>
        </div>
    </div>
    
    <div class="row mt-4">
        <div class="col-md-6">
            <div class="card">
                <div class="card-header">
                    <h5>Extension Information</h5>
                </div>
                <div class="card-body">
                    <p><strong>Name:</strong> {{{{ extension_name }}}}</p>
                    <p><strong>Version:</strong> {{{{ version }}}}</p>
                    <p><strong>Description:</strong> {{{{ description }}}}</p>
                </div>
            </div>
        </div>
        
        <div class="col-md-6">
            <div class="card">
                <div class="card-header">
                    <h5>API Test</h5>
                </div>
                <div class="card-body">
                    <button id="testApiButton" class="btn btn-primary">
                        Test API Endpoint
                    </button>
                    <div id="apiResult" class="mt-3"></div>
                </div>
            </div>
        </div>
    </div>
</div>

<script>
document.getElementById('testApiButton').addEventListener('click', function() {{
    fetch('{{% url '{context['extension_name']}:api' %}}', {{
        method: 'POST',
        headers: {{
            'Content-Type': 'application/json',
            'X-CSRFToken': document.querySelector('[name=csrfmiddlewaretoken]').value
        }},
        body: JSON.stringify({{message: 'Hello from {context['display_name']}!'}})
    }})
    .then(response => response.json())
    .then(data => {{
        document.getElementById('apiResult').innerHTML = 
            '<div class="alert alert-success">API Response: ' + JSON.stringify(data, null, 2) + '</div>';
    }})
    .catch(error => {{
        document.getElementById('apiResult').innerHTML = 
            '<div class="alert alert-danger">Error: ' + error.message + '</div>';
    }});
}});
</script>
{{% endblock %}}
'''
        self._write_file(extension_path / 'templates' / context['extension_name'] / 'dashboard.html', dashboard_template)

    def _create_static_files(self, extension_path, context):
        """Create static CSS and JS files"""
        css_content = f'''/* {context['display_name']} Extension Styles */

.{context['extension_name']}-dashboard {{
    padding: 20px;
}}

.card {{
    margin-bottom: 20px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}}

.card-header {{
    background-color: #f8f9fa;
    font-weight: 600;
}}

.btn-primary {{
    background-color: #007bff;
    border-color: #007bff;
}}

.btn-primary:hover {{
    background-color: #0056b3;
    border-color: #004085;
}}
'''
        self._write_file(extension_path / 'static' / context['extension_name'] / 'css' / 'dashboard.css', css_content)

        js_content = f'''// {context['display_name']} Extension JavaScript

document.addEventListener('DOMContentLoaded', function() {{
    console.log('{context['display_name']} Extension loaded');
}});

// Helper function to get CSRF token
function getCSRFToken() {{
    const tokenElement = document.querySelector('[name=csrfmiddlewaretoken]');
    return tokenElement ? tokenElement.value : '';
}}

// API helper function
async function callExtensionAPI(data) {{
    try {{
        const response = await fetch('api/', {{
            method: 'POST',
            headers: {{
                'Content-Type': 'application/json',
                'X-CSRFToken': getCSRFToken()
            }},
            body: JSON.stringify(data)
        }});
        
        if (!response.ok) {{
            throw new Error(`HTTP error! status: ${{response.status}}`);
        }}
        
        return await response.json();
    }} catch (error) {{
        console.error('API call failed:', error);
        throw error;
    }}
}}
'''
        self._write_file(extension_path / 'static' / context['extension_name'] / 'js' / 'dashboard.js', js_content)

    def _create_process_steps(self, extension_path, context):
        """Create process steps directory and example file"""
        content = f'''"""
Business Logic for {context['display_name']}
Implement your core functionality here
"""

import logging

logger = logging.getLogger(__name__)

class {context['class_name']}Processor:
    """Main processor for {context['display_name']} extension"""
    
    def __init__(self):
        self.name = "{context['extension_name']}"
        self.version = "{context['version']}"
        logger.info(f"Initialized {{self.name}} processor v{{self.version}}")
    
    def process_data(self, data):
        """
        Process data using extension logic
        
        Args:
            data: Input data to process
            
        Returns:
            dict: Processed results
        """
        try:
            # Implement your business logic here
            result = {{
                'input_data': data,
                'processed_at': self._get_timestamp(),
                'processor': self.name,
                'status': 'success'
            }}
            
            # Add your processing logic
            result['processed_data'] = self._perform_processing(data)
            
            return result
            
        except Exception as e:
            logger.error(f"Processing error: {{str(e)}}")
            return {{
                'status': 'error',
                'error_message': str(e),
                'processor': self.name
            }}
    
    def _perform_processing(self, data):
        """Implement your specific processing logic here"""
        # This is where you'd implement the core functionality
        # For example: calculations, transformations, analysis, etc.
        
        # Example processing
        if isinstance(data, dict):
            return {{key: f"processed_{{value}}" for key, value in data.items()}}
        elif isinstance(data, list):
            return [f"processed_{{item}}" for item in data]
        else:
            return f"processed_{{data}}"
    
    def _get_timestamp(self):
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()
'''
        
        # Create __init__.py for process_steps
        self._write_file(extension_path / 'process_steps' / '__init__.py', 
                        f'"""\nBusiness logic for {context["display_name"]}\n"""')
        
        # Create the main processor file
        self._write_file(extension_path / 'process_steps' / f'{context["extension_name"]}_processor.py', content)

    def _create_tests(self, extension_path, context):
        """Create test files"""
        test_content = f'''"""
Tests for {context['display_name']} Extension
"""

from django.test import TestCase, Client
from django.urls import reverse
import json

class Test{context['class_name']}Extension(TestCase):
    """Test cases for {context['display_name']} extension"""
    
    def setUp(self):
        self.client = Client()
    
    def test_dashboard_view(self):
        """Test the dashboard view loads correctly"""
        url = reverse('{context['extension_name']}:dashboard')
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, '{context['display_name']}')
    
    def test_api_endpoint(self):
        """Test the API endpoint"""
        url = reverse('{context['extension_name']}:api')
        data = {{'test': 'data'}}
        
        response = self.client.post(
            url,
            data=json.dumps(data),
            content_type='application/json'
        )
        
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.content)
        self.assertEqual(result['status'], 'success')

class Test{context['class_name']}Processor(TestCase):
    """Test cases for the business logic processor"""
    
    def test_processor_initialization(self):
        """Test processor initializes correctly"""
        from ..process_steps.{context['extension_name']}_processor import {context['class_name']}Processor
        
        processor = {context['class_name']}Processor()
        self.assertEqual(processor.name, '{context['extension_name']}')
        self.assertEqual(processor.version, '{context['version']}')
    
    def test_data_processing(self):
        """Test data processing functionality"""
        from ..process_steps.{context['extension_name']}_processor import {context['class_name']}Processor
        
        processor = {context['class_name']}Processor()
        test_data = {{'key1': 'value1', 'key2': 'value2'}}
        
        result = processor.process_data(test_data)
        
        self.assertEqual(result['status'], 'success')
        self.assertIn('processed_data', result)
        self.assertIn('processed_at', result)
'''
        
        # Create __init__.py for tests
        self._write_file(extension_path / 'tests' / '__init__.py', 
                        f'"""\nTest suite for {context["display_name"]} Extension\n"""')
        
        # Create the test file
        self._write_file(extension_path / 'tests' / f'test_{context["extension_name"]}.py', test_content)

    def _create_readme(self, extension_path, context):
        """Create README.md file"""
        content = f'''# {context['display_name']}

{context['description']}

## Overview

This extension provides [describe your extension's functionality here].

## Installation

### Development Setup

1. The extension is automatically discovered when placed in the `extensions/` directory
2. Start the development server:
   ```bash
   python manage.py runserver
   ```
3. Access the extension at: http://localhost:8000/pybirdai/extensions/{context['extension_name']}/

### Production Installation

For production deployment, package the extension:

```bash
python manage.py package_extension --name {context['extension_name']} --repo-name my-extension-repo --github-user myusername
```

## Usage

### Web Interface

Access the main dashboard at the extension URL. The dashboard provides:
- Extension information and status
- API testing interface
- [Add your specific features here]

### API Access

The extension provides REST API endpoints:

```bash
curl -X POST http://localhost:8000/pybirdai/extensions/{context['extension_name']}/api/ \\
  -H "Content-Type: application/json" \\
  -d '{{"message": "test data"}}'
```

## Development

### Project Structure

```
{context['extension_name']}/
├── __init__.py                    # Package initialization
├── views.py                       # HTTP endpoints
├── urls.py                        # URL routing
├── templates/{context['extension_name']}/       # HTML templates
├── static/{context['extension_name']}/          # CSS, JS files
├── process_steps/                 # Business logic
├── entry_point.py                 # Django app configuration
├── extension_configuration.py     # License & configuration
├── extension_models.py            # Database models
├── extension_manifest.yaml        # Extension metadata
├── tests/                         # Test suite
└── README.md                      # This file
```

### Running Tests

```bash
python manage.py test extensions.{context['extension_name']}
```

### Configuration

Add extension-specific settings to your Django settings:

```python
# Extension Settings
{context['extension_name'].upper()}_LICENSE_KEY = 'your-license-key'
{context['extension_name'].upper()}_DEBUG = True
```

## License

{context['license']}

## Author

{context['author']} ({context['author_email']})

## Version History

- v{context['version']} - Initial release
'''
        self._write_file(extension_path / 'README.md', content)

    def _write_file(self, file_path, content):
        """Write content to a file"""
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)