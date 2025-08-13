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

import yaml
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

class ManifestGenerator:
    """
    Generates extension manifest files containing metadata, dependencies,
    and configuration information for BIRD extensions.
    """
    
    def __init__(self):
        self.manifest_data = {}
    
    def generate(
        self,
        output_path: Path,
        extension_name: str,
        version: str = '1.0.0',
        author: str = 'Extension Developer',
        author_email: str = 'developer@example.com',
        license: str = 'EPL-2.0',
        description: str = None,
        dependencies: List[str] = None,
        **kwargs
    ):
        """
        Generate extension manifest files.
        
        Args:
            output_path: Directory where to create manifest files
            extension_name: Name of the extension
            version: Extension version
            author: Author name
            author_email: Author email
            license: License identifier
            description: Extension description
            dependencies: List of dependencies
            **kwargs: Additional metadata
        """
        # Create manifest data
        self.manifest_data = self._create_manifest_data(
            extension_name, version, author, author_email,
            license, description, dependencies, **kwargs
        )
        
        # Generate YAML manifest
        yaml_path = self._generate_yaml_manifest(output_path)
        
        # Generate JSON manifest
        json_path = self._generate_json_manifest(output_path)
        
        # Generate package.json for npm-style management
        package_json_path = self._generate_package_json(output_path, extension_name)
        
        logger.info(f"Generated manifests: {yaml_path}, {json_path}, {package_json_path}")
        
        return {
            'yaml': yaml_path,
            'json': json_path,
            'package_json': package_json_path
        }
    
    def _create_manifest_data(
        self,
        extension_name: str,
        version: str,
        author: str,
        author_email: str,
        license: str,
        description: Optional[str],
        dependencies: Optional[List[str]],
        **kwargs
    ) -> Dict[str, Any]:
        """Create the base manifest data structure."""
        
        manifest_data = {
            'name': extension_name,
            'display_name': extension_name.replace('_', ' ').title(),
            'version': version,
            'description': description or f'BIRD Bench extension: {extension_name.replace("_", " ").title()}',
            'author': {
                'name': author,
                'email': author_email
            },
            'license': license,
            'created_at': datetime.now().isoformat(),
            'api_version': '1.0',
            'bird_version_compatibility': '>=5.1.0',
            
            'extension_info': {
                'type': 'bird_extension',
                'category': kwargs.get('category', 'general'),
                'tags': kwargs.get('tags', ['bird', 'finrep', 'regulatory']),
                'homepage': kwargs.get('homepage', ''),
                'documentation': kwargs.get('documentation', ''),
                'support': kwargs.get('support', ''),
                'issues': kwargs.get('issues', ''),
            },
            
            'technical': {
                'django_version': '>=5.1.0',
                'python_version': '>=3.8',
                'dependencies': dependencies or [
                    'django>=5.1.0',
                    'pyecore>=0.15.0'
                ],
                'optional_dependencies': kwargs.get('optional_dependencies', []),
                'dev_dependencies': kwargs.get('dev_dependencies', [
                    'pytest>=8.3.0',
                    'ruff>=0.9.0'
                ])
            },
            
            'structure': {
                'has_models': kwargs.get('has_models', False),
                'has_migrations': kwargs.get('has_migrations', False),
                'has_static_files': kwargs.get('has_static_files', True),
                'has_templates': kwargs.get('has_templates', True),
                'has_management_commands': kwargs.get('has_management_commands', False),
                'has_api_endpoints': kwargs.get('has_api_endpoints', False),
                'has_background_tasks': kwargs.get('has_background_tasks', False)
            },
            
            'installation': {
                'auto_migrate': kwargs.get('auto_migrate', True),
                'collect_static': kwargs.get('collect_static', True),
                'requires_setup': kwargs.get('requires_setup', False),
                'post_install_steps': kwargs.get('post_install_steps', [])
            },
            
            'configuration': {
                'settings_required': kwargs.get('settings_required', []),
                'environment_variables': kwargs.get('environment_variables', []),
                'url_patterns': kwargs.get('url_patterns', [f'/extensions/{extension_name}/']),
                'permissions': kwargs.get('permissions', []),
                'menu_items': kwargs.get('menu_items', [])
            },
            
            'compatibility': {
                'tested_with': kwargs.get('tested_with', ['5.1.3']),
                'known_conflicts': kwargs.get('known_conflicts', []),
                'platform_support': kwargs.get('platform_support', ['linux', 'macos', 'windows'])
            }
        }
        
        return manifest_data
    
    def _generate_yaml_manifest(self, output_path: Path) -> Path:
        """Generate YAML manifest file."""
        yaml_path = output_path / 'extension_manifest.yaml'
        
        with open(yaml_path, 'w', encoding='utf-8') as f:
            # Write header comment
            f.write("# BIRD Extension Manifest\n")
            f.write("# This file contains metadata about the extension\n\n")
            
            # Write YAML data
            yaml.dump(
                self.manifest_data,
                f,
                default_flow_style=False,
                sort_keys=False,
                indent=2
            )
        
        return yaml_path
    
    def _generate_json_manifest(self, output_path: Path) -> Path:
        """Generate JSON manifest file."""
        json_path = output_path / 'extension_manifest.json'
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(
                self.manifest_data,
                f,
                indent=2,
                sort_keys=False,
                ensure_ascii=False
            )
        
        return json_path
    
    def _generate_package_json(self, output_path: Path, extension_name: str) -> Path:
        """Generate package.json for npm-style package management."""
        package_json_path = output_path / 'package.json'
        
        package_data = {
            'name': f"bird-extension-{extension_name.replace('_', '-')}",
            'version': self.manifest_data['version'],
            'description': self.manifest_data['description'],
            'keywords': [
                'bird',
                'finrep',
                'regulatory-reporting',
                'django',
                'extension'
            ] + self.manifest_data['extension_info']['tags'],
            
            'author': {
                'name': self.manifest_data['author']['name'],
                'email': self.manifest_data['author']['email']
            },
            
            'license': self.manifest_data['license'],
            'homepage': self.manifest_data['extension_info']['homepage'],
            'bugs': {
                'url': self.manifest_data['extension_info']['issues']
            },
            
            'engines': {
                'python': self.manifest_data['technical']['python_version'],
                'django': self.manifest_data['technical']['django_version']
            },
            
            'dependencies': {},
            'devDependencies': {},
            
            'scripts': {
                'install': 'python install.py',
                'test': 'python -m pytest',
                'lint': 'ruff check .',
                'format': 'ruff format .'
            },
            
            'repository': {
                'type': 'git',
                'url': f"git+https://github.com/user/bird-extension-{extension_name.replace('_', '-')}.git"
            },
            
            'bird': {
                'type': 'extension',
                'api_version': self.manifest_data['api_version'],
                'compatibility': self.manifest_data['bird_version_compatibility'],
                'category': self.manifest_data['extension_info']['category'],
                'entry_point': f'{extension_name}.urls',
                'settings_module': 'settings_extension'
            }
        }
        
        # Add Python dependencies as comments since package.json doesn't handle them
        package_data['_python_dependencies'] = {
            'runtime': self.manifest_data['technical']['dependencies'],
            'optional': self.manifest_data['technical']['optional_dependencies'],
            'development': self.manifest_data['technical']['dev_dependencies']
        }
        
        with open(package_json_path, 'w', encoding='utf-8') as f:
            json.dump(package_data, f, indent=2, sort_keys=False)
        
        return package_json_path
    
    def generate_compatibility_matrix(self, output_path: Path) -> Path:
        """Generate compatibility matrix file."""
        matrix_path = output_path / 'compatibility_matrix.md'
        
        with open(matrix_path, 'w', encoding='utf-8') as f:
            f.write(f"# Compatibility Matrix for {self.manifest_data['display_name']}\n\n")
            
            f.write("## BIRD Version Compatibility\n\n")
            f.write("| BIRD Version | Extension Version | Status | Notes |\n")
            f.write("|--------------|------------------|--------|---------|\n")
            
            tested_versions = self.manifest_data['compatibility']['tested_with']
            for version in tested_versions:
                f.write(f"| {version} | {self.manifest_data['version']} | ✅ Tested | Fully compatible |\n")
            
            f.write(f"\n## Requirements\n\n")
            f.write(f"- **Python**: {self.manifest_data['technical']['python_version']}\n")
            f.write(f"- **Django**: {self.manifest_data['technical']['django_version']}\n")
            f.write(f"- **BIRD**: {self.manifest_data['bird_version_compatibility']}\n\n")
            
            f.write("## Dependencies\n\n")
            f.write("### Runtime Dependencies\n")
            for dep in self.manifest_data['technical']['dependencies']:
                f.write(f"- `{dep}`\n")
            
            if self.manifest_data['technical']['optional_dependencies']:
                f.write("\n### Optional Dependencies\n")
                for dep in self.manifest_data['technical']['optional_dependencies']:
                    f.write(f"- `{dep}`\n")
            
            f.write("\n### Development Dependencies\n")
            for dep in self.manifest_data['technical']['dev_dependencies']:
                f.write(f"- `{dep}`\n")
            
            if self.manifest_data['compatibility']['known_conflicts']:
                f.write("\n## Known Conflicts\n\n")
                for conflict in self.manifest_data['compatibility']['known_conflicts']:
                    f.write(f"- {conflict}\n")
            
            f.write(f"\n## Platform Support\n\n")
            platforms = self.manifest_data['compatibility']['platform_support']
            for platform in platforms:
                f.write(f"- {platform.title()}\n")
        
        return matrix_path
    
    def generate_changelog(self, output_path: Path) -> Path:
        """Generate initial CHANGELOG.md file."""
        changelog_path = output_path / 'CHANGELOG.md'
        
        with open(changelog_path, 'w', encoding='utf-8') as f:
            f.write(f"# Changelog - {self.manifest_data['display_name']}\n\n")
            f.write("All notable changes to this extension will be documented in this file.\n\n")
            f.write("The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),\n")
            f.write("and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).\n\n")
            
            version = self.manifest_data['version']
            date = datetime.now().strftime('%Y-%m-%d')
            
            f.write(f"## [{version}] - {date}\n\n")
            f.write("### Added\n")
            f.write("- Initial release of the extension\n")
            f.write("- Core functionality implementation\n")
            f.write("- Integration with BIRD framework\n")
            f.write("- Documentation and setup instructions\n\n")
            
            f.write("### Changed\n")
            f.write("- N/A (initial release)\n\n")
            
            f.write("### Fixed\n")
            f.write("- N/A (initial release)\n\n")
            
            f.write("### Deprecated\n")
            f.write("- N/A (initial release)\n\n")
            
            f.write("### Removed\n")
            f.write("- N/A (initial release)\n\n")
            
            f.write("### Security\n")
            f.write("- N/A (initial release)\n")
        
        return changelog_path
    
    def validate_manifest(self) -> List[str]:
        """Validate the generated manifest for required fields and consistency."""
        errors = []
        
        required_fields = [
            'name', 'version', 'description', 'author', 'license'
        ]
        
        for field in required_fields:
            if field not in self.manifest_data or not self.manifest_data[field]:
                errors.append(f"Missing required field: {field}")
        
        # Validate version format
        version = self.manifest_data.get('version', '')
        if not self._is_valid_semver(version):
            errors.append(f"Invalid semantic version format: {version}")
        
        # Validate email format
        author_email = self.manifest_data.get('author', {}).get('email', '')
        if author_email and '@' not in author_email:
            errors.append(f"Invalid email format: {author_email}")
        
        return errors
    
    def _is_valid_semver(self, version: str) -> bool:
        """Check if version follows semantic versioning."""
        try:
            parts = version.split('.')
            return (len(parts) == 3 and 
                   all(part.isdigit() for part in parts))
        except:
            return False