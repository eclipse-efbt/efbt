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

import ast
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

class SettingsExtractor:
    """
    Extracts extension-specific settings from the main Django settings file
    and creates a minimal settings configuration for the packaged extension.
    """
    
    def __init__(self):
        self.extension_settings = {}
        self.core_settings = {}
    
    def extract_extension_settings(
        self,
        settings_path: Path,
        output_path: Path,
        extension_name: str
    ) -> Path:
        """
        Extract settings relevant to the extension and create a settings file.
        
        Args:
            settings_path: Path to the main settings.py file
            output_path: Path where to create the extension settings
            extension_name: Name of the extension
            
        Returns:
            Path: Path to the created settings file
        """
        # Parse the main settings file
        self._parse_settings_file(settings_path)
        
        # Create extension-specific settings
        extension_settings = self._create_extension_settings(extension_name)
        
        # Write the settings file
        settings_file_path = output_path / 'settings_extension.py'
        self._write_settings_file(settings_file_path, extension_settings, extension_name)
        
        return settings_file_path
    
    def _parse_settings_file(self, settings_path: Path):
        """Parse the Django settings file and extract key configurations."""
        try:
            with open(settings_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Parse the AST to understand the settings structure
            tree = ast.parse(content)
            
            # Extract key settings
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            setting_name = target.id
                            setting_value = self._extract_value(node.value)
                            self.core_settings[setting_name] = setting_value
            
            # Also parse as text for some complex patterns
            self._parse_text_patterns(content)
            
        except Exception as e:
            logger.error(f"Failed to parse settings file: {e}")
            raise
    
    def _extract_value(self, node) -> Any:
        """Extract value from an AST node."""
        if isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.Str):  # Python < 3.8 compatibility
            return node.s
        elif isinstance(node, ast.Num):  # Python < 3.8 compatibility
            return node.n
        elif isinstance(node, ast.List):
            return [self._extract_value(item) for item in node.elts]
        elif isinstance(node, ast.Dict):
            return {
                self._extract_value(k): self._extract_value(v)
                for k, v in zip(node.keys, node.values)
                if k is not None
            }
        elif isinstance(node, ast.Name):
            return f"<variable:{node.id}>"
        elif isinstance(node, ast.Call):
            return f"<function_call>"
        else:
            return f"<complex_value>"
    
    def _parse_text_patterns(self, content: str):
        """Parse specific patterns that are difficult to extract via AST."""
        lines = content.split('\n')
        
        # Extract BASE_DIR pattern
        for line in lines:
            if 'BASE_DIR' in line and 'Path' in line:
                self.core_settings['BASE_DIR_PATTERN'] = line.strip()
                break
        
        # Extract database configuration
        in_database_block = False
        database_lines = []
        
        for line in lines:
            if line.strip().startswith('DATABASES'):
                in_database_block = True
                database_lines.append(line)
            elif in_database_block:
                database_lines.append(line)
                if line.strip() == '}' and line.count('}') >= line.count('{'):
                    break
        
        if database_lines:
            self.core_settings['DATABASES_TEXT'] = '\n'.join(database_lines)
    
    def _create_extension_settings(self, extension_name: str) -> Dict[str, Any]:
        """Create extension-specific settings configuration."""
        
        settings_config = {
            'header_comment': f'''# Extension Settings for {extension_name.replace('_', ' ').title()}
# This file contains minimal Django settings required to run this extension
# independently or integrate it into an existing BIRD installation.''',
            
            'imports': [
                'from pathlib import Path',
                'import os',
                'import logging'
            ],
            
            'base_settings': {
                'BASE_DIR': "Path(__file__).resolve().parent.parent",
                'DEBUG': True,
                'SECRET_KEY': "'django-insecure-extension-key-change-in-production'",
                'ALLOWED_HOSTS': "[]",
                'USE_I18N': True,
                'USE_TZ': True,
                'TIME_ZONE': "'UTC'",
                'LANGUAGE_CODE': "'en-us'",
            },
            
            'apps_settings': {
                'INSTALLED_APPS': [
                    "'django.contrib.admin'",
                    "'django.contrib.auth'",
                    "'django.contrib.contenttypes'",
                    "'django.contrib.sessions'",
                    "'django.contrib.messages'",
                    "'django.contrib.staticfiles'",
                    "'pybirdai'",
                    f"'{extension_name}'"
                ]
            },
            
            'middleware_settings': {
                'MIDDLEWARE': [
                    "'django.middleware.security.SecurityMiddleware'",
                    "'django.contrib.sessions.middleware.SessionMiddleware'",
                    "'django.middleware.common.CommonMiddleware'",
                    "'django.middleware.csrf.CsrfViewMiddleware'",
                    "'django.contrib.auth.middleware.AuthenticationMiddleware'",
                    "'django.contrib.messages.middleware.MessageMiddleware'",
                    "'django.middleware.clickjacking.XFrameOptionsMiddleware'"
                ]
            },
            
            'database_settings': {
                'DATABASES': {
                    "'default'": {
                        "'ENGINE'": "'django.db.backends.sqlite3'",
                        "'NAME'": "BASE_DIR / 'db.sqlite3'"
                    }
                }
            },
            
            'template_settings': {
                'TEMPLATES': [{
                    "'BACKEND'": "'django.template.backends.django.DjangoTemplates'",
                    "'DIRS'": "[BASE_DIR / 'templates']",
                    "'APP_DIRS'": True,
                    "'OPTIONS'": {
                        "'context_processors'": [
                            "'django.template.context_processors.debug'",
                            "'django.template.context_processors.request'",
                            "'django.contrib.auth.context_processors.auth'",
                            "'django.contrib.messages.context_processors.messages'"
                        ]
                    }
                }]
            },
            
            'static_settings': {
                'STATIC_URL': "'/static/'",
                'STATICFILES_DIRS': "[BASE_DIR / 'static']"
            },
            
            'extension_specific': self._extract_extension_specific_settings(extension_name)
        }
        
        return settings_config
    
    def _extract_extension_specific_settings(self, extension_name: str) -> Dict[str, Any]:
        """Extract any extension-specific settings from the main settings."""
        extension_settings = {}
        
        # Look for settings that might be extension-specific
        for key, value in self.core_settings.items():
            # Check for extension-related settings
            if extension_name.upper() in key.upper():
                extension_settings[key] = value
            
            # Check for common extension patterns
            elif any(pattern in key.upper() for pattern in ['EXTENSION', 'PLUGIN', 'ADDON']):
                extension_settings[key] = value
        
        # Add common extension settings
        extension_settings.update({
            'DEFAULT_AUTO_FIELD': "'django.db.models.BigAutoField'",
            'ROOT_URLCONF': f"'{extension_name}.urls'",
            'WSGI_APPLICATION': f"'{extension_name}.wsgi.application'"
        })
        
        return extension_settings
    
    def _write_settings_file(
        self,
        file_path: Path,
        settings_config: Dict[str, Any],
        extension_name: str
    ):
        """Write the settings configuration to a Python file."""
        
        with open(file_path, 'w', encoding='utf-8') as f:
            # Write header
            f.write("# coding=UTF-8\n")
            f.write("# Copyright (c) 2024 Bird Software Solutions Ltd\n")
            f.write("# This program and the accompanying materials\n")
            f.write("# are made available under the terms of the Eclipse Public License 2.0\n")
            f.write("# which accompanies this distribution, and is available at\n")
            f.write("# https://www.eclipse.org/legal/epl-2.0/\n")
            f.write("#\n")
            f.write("# SPDX-License-Identifier: EPL-2.0\n")
            f.write("#\n")
            f.write("# Contributors:\n")
            f.write("#    Extension Developer - initial API and implementation\n\n")
            
            f.write(settings_config['header_comment'] + '\n\n')
            
            # Write imports
            for import_line in settings_config['imports']:
                f.write(f"{import_line}\n")
            f.write('\n')
            
            # Write base directory
            f.write("# Build paths inside the project\n")
            f.write(f"BASE_DIR = {settings_config['base_settings']['BASE_DIR']}\n\n")
            
            # Write base settings
            f.write("# Quick-start development settings\n")
            for key, value in settings_config['base_settings'].items():
                if key != 'BASE_DIR':  # Already written
                    f.write(f"{key} = {value}\n")
            f.write('\n')
            
            # Write installed apps
            f.write("# Application definition\n")
            f.write("INSTALLED_APPS = [\n")
            for app in settings_config['apps_settings']['INSTALLED_APPS']:
                f.write(f"    {app},\n")
            f.write("]\n\n")
            
            # Write middleware
            f.write("MIDDLEWARE = [\n")
            for middleware in settings_config['middleware_settings']['MIDDLEWARE']:
                f.write(f"    {middleware},\n")
            f.write("]\n\n")
            
            # Write templates configuration
            f.write("TEMPLATES = [\n")
            template_config = settings_config['template_settings']['TEMPLATES'][0]
            f.write("    {\n")
            for key, value in template_config.items():
                if isinstance(value, list):
                    f.write(f"        {key}: [\n")
                    for item in value:
                        f.write(f"            {item},\n")
                    f.write("        ],\n")
                elif isinstance(value, dict):
                    f.write(f"        {key}: {{\n")
                    for sub_key, sub_value in value.items():
                        if isinstance(sub_value, list):
                            f.write(f"            {sub_key}: [\n")
                            for item in sub_value:
                                f.write(f"                {item},\n")
                            f.write("            ],\n")
                        else:
                            f.write(f"            {sub_key}: {sub_value},\n")
                    f.write("        },\n")
                else:
                    f.write(f"        {key}: {value},\n")
            f.write("    },\n")
            f.write("]\n\n")
            
            # Write database settings
            f.write("# Database\n")
            f.write("DATABASES = {\n")
            db_config = settings_config['database_settings']['DATABASES']
            for db_name, db_settings in db_config.items():
                f.write(f"    {db_name}: {{\n")
                for key, value in db_settings.items():
                    f.write(f"        {key}: {value},\n")
                f.write("    },\n")
            f.write("}\n\n")
            
            # Write static files settings
            f.write("# Static files (CSS, JavaScript, Images)\n")
            for key, value in settings_config['static_settings'].items():
                f.write(f"{key} = {value}\n")
            f.write('\n')
            
            # Write extension-specific settings
            if settings_config['extension_specific']:
                f.write("# Extension-specific settings\n")
                for key, value in settings_config['extension_specific'].items():
                    f.write(f"{key} = {value}\n")
                f.write('\n')
            
            # Write logging configuration
            self._write_logging_config(f, extension_name)
        
        logger.info(f"Created extension settings file: {file_path}")
    
    def _write_logging_config(self, f, extension_name: str):
        """Write logging configuration to the settings file."""
        f.write("# Logging configuration\n")
        f.write("LOGGING = {\n")
        f.write("    'version': 1,\n")
        f.write("    'disable_existing_loggers': False,\n")
        f.write("    'handlers': {\n")
        f.write("        'file': {\n")
        f.write("            'level': 'INFO',\n")
        f.write("            'class': 'logging.FileHandler',\n")
        f.write(f"            'filename': '{extension_name}.log',\n")
        f.write("        },\n")
        f.write("        'console': {\n")
        f.write("            'level': 'INFO',\n")
        f.write("            'class': 'logging.StreamHandler',\n")
        f.write("        },\n")
        f.write("    },\n")
        f.write("    'loggers': {\n")
        f.write(f"        '{extension_name}': {{\n")
        f.write("            'handlers': ['file', 'console'],\n")
        f.write("            'level': 'INFO',\n")
        f.write("        },\n")
        f.write("    },\n")
        f.write("}\n\n")
    
    def create_local_settings_template(self, output_path: Path, extension_name: str):
        """Create a local_settings.py template for customization."""
        local_settings_path = output_path / 'local_settings.py.template'
        
        with open(local_settings_path, 'w', encoding='utf-8') as f:
            f.write(f'''# coding=UTF-8
# Local Settings Template for {extension_name.replace('_', ' ').title()}
# Copy this file to local_settings.py and customize for your environment

# Override debug setting
# DEBUG = False

# Set your secret key for production
# SECRET_KEY = 'your-secret-key-here'

# Add allowed hosts
# ALLOWED_HOSTS = ['your-domain.com']

# Database configuration (example for PostgreSQL)
# DATABASES = {{
#     'default': {{
#         'ENGINE': 'django.db.backends.postgresql',
#         'NAME': '{extension_name}_db',
#         'USER': 'your_user',
#         'PASSWORD': 'your_password',
#         'HOST': 'localhost',
#         'PORT': '5432',
#     }}
# }}

# Static files in production
# STATIC_ROOT = '/var/www/{extension_name}/static/'

# Media files configuration
# MEDIA_URL = '/media/'
# MEDIA_ROOT = BASE_DIR / 'media'

# Email configuration
# EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
# EMAIL_HOST = 'smtp.your-email-provider.com'
# EMAIL_PORT = 587
# EMAIL_USE_TLS = True
# EMAIL_HOST_USER = 'your-email@example.com'
# EMAIL_HOST_PASSWORD = 'your-email-password'

# Extension-specific overrides
# Add any extension-specific settings here
''')
        
        logger.info(f"Created local settings template: {local_settings_path}")