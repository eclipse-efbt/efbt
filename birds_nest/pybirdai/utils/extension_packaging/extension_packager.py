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

import os
import shutil
from pathlib import Path
from typing import List, Dict, Optional

class ExtensionPackager:
    """
    Handles packaging of BIRD extensions following the standard structure.
    """
    
    REQUIRED_FILES = ['__init__.py', 'views.py', 'urls.py']
    REQUIRED_DIRS = ['templates', 'static']
    OPTIONAL_FILES = [
        'entry_point.py',
        'extension_configuration.py', 
        'extension_models.py',
        'extension_manifest.yaml'
    ]
    OPTIONAL_DIRS = ['process_steps', 'migrations']
    
    def __init__(self):
        self.validation_errors = []
    
    def validate_structure(self, extension_path: Path) -> bool:
        """
        Validate that the extension follows the standard BIRD extension structure.
        
        Args:
            extension_path: Path to the extension directory
            
        Returns:
            bool: True if valid, False otherwise
            
        Raises:
            ValidationError: If extension structure is invalid
        """
        self.validation_errors = []
        
        if not extension_path.exists():
            raise ValidationError(f"Extension directory does not exist: {extension_path}")
        
        if not extension_path.is_dir():
            raise ValidationError(f"Extension path is not a directory: {extension_path}")
        
        # Check required files
        for required_file in self.REQUIRED_FILES:
            file_path = extension_path / required_file
            if not file_path.exists():
                self.validation_errors.append(f"Missing required file: {required_file}")
            elif not file_path.is_file():
                self.validation_errors.append(f"Required item is not a file: {required_file}")
        
        # Check required directories
        for required_dir in self.REQUIRED_DIRS:
            dir_path = extension_path / required_dir
            if not dir_path.exists():
                self.validation_errors.append(f"Missing required directory: {required_dir}")
            elif not dir_path.is_dir():
                self.validation_errors.append(f"Required item is not a directory: {required_dir}")
        
        # Validate views.py has proper Django structure
        self._validate_views_file(extension_path / 'views.py')
        
        # Validate urls.py has proper Django structure
        self._validate_urls_file(extension_path / 'urls.py')
        
        if self.validation_errors:
            error_msg = "\n".join(self.validation_errors)
            raise ValidationError(f"Extension validation failed:\n{error_msg}")
        
        return True
    
    def _validate_views_file(self, views_path: Path):
        """Validate that views.py has proper Django imports and structure."""
        if not views_path.exists():
            return
        
        try:
            with open(views_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Check for basic Django imports
            if 'from django.shortcuts import' not in content and 'from django.http import' not in content:
                self.validation_errors.append("views.py should import Django shortcuts or http modules")
                
            # Check for at least one view function
            if 'def ' not in content:
                self.validation_errors.append("views.py should contain at least one view function")
                
        except Exception as e:
            self.validation_errors.append(f"Could not read views.py: {str(e)}")
    
    def _validate_urls_file(self, urls_path: Path):
        """Validate that urls.py has proper Django URL patterns."""
        if not urls_path.exists():
            return
        
        try:
            with open(urls_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Check for Django URL imports
            if 'from django.urls import' not in content:
                self.validation_errors.append("urls.py should import from django.urls")
                
            # Check for urlpatterns
            if 'urlpatterns' not in content:
                self.validation_errors.append("urls.py should define urlpatterns")
                
            # Check for app_name
            if 'app_name' not in content:
                self.validation_errors.append("urls.py should define app_name for namespacing")
                
        except Exception as e:
            self.validation_errors.append(f"Could not read urls.py: {str(e)}")
    
    def create_package(self, extension_path: Path, output_path: Path, extension_name: str):
        """
        Create a packaged version of the extension.
        
        Args:
            extension_path: Path to the source extension
            output_path: Path where the package should be created
            extension_name: Name of the extension
        """
        # Ensure output directory exists
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create extension directory in package
        ext_output_path = output_path / extension_name
        
        # Copy entire extension directory
        if ext_output_path.exists():
            shutil.rmtree(ext_output_path)
        
        shutil.copytree(extension_path, ext_output_path)
        
        # Clean up unnecessary files
        self._cleanup_package(ext_output_path)
        
        # Create package structure
        self._create_package_structure(output_path, extension_name)
    
    def _cleanup_package(self, package_path: Path):
        """Remove unnecessary files from the package."""
        cleanup_patterns = [
            '*.pyc',
            '__pycache__',
            '.DS_Store',
            '*.tmp',
            '*.log',
            'tmp'
        ]
        
        for pattern in cleanup_patterns:
            if pattern.startswith('*.'):
                # Remove files with specific extensions
                for file_path in package_path.rglob(pattern):
                    if file_path.is_file():
                        file_path.unlink()
            else:
                # Remove directories with specific names
                for dir_path in package_path.rglob(pattern):
                    if dir_path.is_dir():
                        shutil.rmtree(dir_path)
    
    def _create_package_structure(self, output_path: Path, extension_name: str):
        """Create additional package structure files."""
        
        # Create main package __init__.py
        init_file = output_path / '__init__.py'
        with open(init_file, 'w', encoding='utf-8') as f:
            f.write(f'''# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Extension Developer - initial API and implementation

"""
{extension_name.replace('_', ' ').title()} - BIRD Bench Extension
"""

__version__ = '1.0.0'
__author__ = 'Extension Developer'
''')
        
        # Create setup.py for easy installation
        setup_file = output_path / 'setup.py'
        with open(setup_file, 'w', encoding='utf-8') as f:
            f.write(f'''# coding=UTF-8
from setuptools import setup, find_packages

setup(
    name='bird-extension-{extension_name.replace("_", "-")}',
    version='1.0.0',
    description='BIRD Bench Extension: {extension_name.replace("_", " ").title()}',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Extension Developer',
    author_email='developer@example.com',
    url='https://github.com/user/bird-extension-{extension_name.replace("_", "-")}',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'django>=5.1.0',
        'pyecore>=0.15.0',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Financial and Insurance Industry',
        'License :: OSI Approved :: Eclipse Public License 2.0 (EPL-2.0)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Office/Business :: Financial',
        'Topic :: Scientific/Engineering',
    ],
    python_requires='>=3.8',
)
''')
    
    def get_extension_info(self, extension_path: Path) -> Dict[str, any]:
        """
        Extract information about the extension.
        
        Args:
            extension_path: Path to the extension
            
        Returns:
            Dict containing extension metadata
        """
        info = {
            'name': extension_path.name,
            'files': [],
            'directories': [],
            'has_models': False,
            'has_migrations': False,
            'has_entry_point': False,
            'has_configuration': False,
            'template_count': 0,
            'static_files': 0
        }
        
        # Count files and directories
        for item in extension_path.iterdir():
            if item.is_file():
                info['files'].append(item.name)
                if item.name == 'extension_models.py':
                    info['has_models'] = True
                elif item.name == 'entry_point.py':
                    info['has_entry_point'] = True
                elif item.name == 'extension_configuration.py':
                    info['has_configuration'] = True
            elif item.is_dir():
                info['directories'].append(item.name)
                if item.name == 'migrations':
                    info['has_migrations'] = True
                elif item.name == 'templates':
                    info['template_count'] = len(list(item.rglob('*.html')))
                elif item.name == 'static':
                    info['static_files'] = len([f for f in item.rglob('*') if f.is_file()])
        
        return info

class ValidationError(Exception):
    """Exception raised when extension validation fails."""
    pass