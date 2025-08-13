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

import toml
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class PyProjectGenerator:
    """
    Generates modern pyproject.toml files for BIRD extensions following
    PEP 621 standards and Python packaging best practices.
    """

    def __init__(self):
        pass

    def generate(
        self,
        output_path: Path,
        extension_name: str,
        version: str = '1.0.0',
        description: str = None,
        author: str = 'Extension Developer',
        author_email: str = 'developer@example.com',
        license_name: str = 'EPL-2.0',
        homepage: str = None,
        repository: str = None,
        dependency_analysis: Dict = None,
        **kwargs
    ) -> Path:
        """
        Generate pyproject.toml file for the extension.

        Args:
            output_path: Directory where to create pyproject.toml
            extension_name: Name of the extension
            version: Extension version
            description: Extension description
            author: Author name
            author_email: Author email
            license_name: License identifier
            homepage: Homepage URL
            repository: Repository URL
            dependency_analysis: Results from DependencyAnalyzer
            **kwargs: Additional metadata

        Returns:
            Path to the generated pyproject.toml file
        """
        pyproject_path = output_path / 'pyproject.toml'

        # Build the pyproject configuration
        config = self._build_pyproject_config(
            extension_name, version, description, author, author_email,
            license_name, homepage, repository, dependency_analysis, **kwargs
        )

        # Write the configuration to file
        with open(pyproject_path, 'w', encoding='utf-8') as f:
            self._write_pyproject_toml(f, config)

        logger.info(f"Generated pyproject.toml for {extension_name}")
        return pyproject_path

    def _build_pyproject_config(
        self,
        extension_name: str,
        version: str,
        description: Optional[str],
        author: str,
        author_email: str,
        license_name: str,
        homepage: Optional[str],
        repository: Optional[str],
        dependency_analysis: Optional[Dict],
        **kwargs
    ) -> Dict[str, Any]:
        """Build the complete pyproject.toml configuration."""

        # Package name (following Python naming conventions)
        package_name = f"bird-extension-{extension_name.replace('_', '-')}"

        config = {
            # Build system configuration
            'build-system': {
                'requires': ['setuptools>=61.0', 'wheel'],
                'build-backend': 'setuptools.build_meta'
            },

            # Project metadata (PEP 621)
            'project': {
                'name': package_name,
                'version': version,
                'description': description or f'BIRD Bench extension: {extension_name.replace("_", " ").title()}',
                'readme': 'README.md',
                'license': {'text': license_name},
                'authors': [{'name': author, 'email': author_email}],
                'maintainers': [{'name': author, 'email': author_email}],
                'keywords': [
                    'bird',
                    'finrep',
                    'regulatory-reporting',
                    'django',
                    'extension',
                    'banking',
                    'compliance'
                ],
                'classifiers': [
                    'Development Status :: 4 - Beta',
                    'Environment :: Web Environment',
                    'Framework :: Django',
                    'Framework :: Django :: 5.1',
                    'Intended Audience :: Financial and Insurance Industry',
                    'License :: OSI Approved :: Eclipse Public License 2.0 (EPL-2.0)',
                    'Operating System :: OS Independent',
                    'Programming Language :: Python',
                    'Programming Language :: Python :: 3',
                    'Programming Language :: Python :: 3.8',
                    'Programming Language :: Python :: 3.9',
                    'Programming Language :: Python :: 3.10',
                    'Programming Language :: Python :: 3.11',
                    'Programming Language :: Python :: 3.12',
                    'Topic :: Office/Business :: Financial',
                    'Topic :: Scientific/Engineering',
                    'Topic :: Software Development :: Libraries :: Python Modules',
                ],
                'requires-python': '>=3.8',
            }
        }

        # Add URLs if provided
        urls = {}
        if homepage:
            urls['Homepage'] = homepage
        if repository:
            urls['Repository'] = repository
            urls['Bug Reports'] = f"{repository}/issues"
            urls['Documentation'] = f"{repository}#readme"

        if urls:
            config['project']['urls'] = urls

        # Add dependencies from analysis
        if dependency_analysis:
            # Runtime dependencies
            runtime_deps = dependency_analysis.get('runtime_dependencies', {})
            print(runtime_deps)
            if runtime_deps:
                config['project']['dependencies'] = [
                    f"{pkg}{version}" for pkg, version in runtime_deps.items()
                ]
            else:
                # Fallback to core dependencies
                config['project']['dependencies'] = ['django>=5.1.3', 'pyecore>=0.15.1']

            # Optional dependencies (development)
            dev_deps = dependency_analysis.get('development_dependencies', {})
            print(dev_deps)
            if dev_deps:
                config['project']['optional-dependencies'] = {
                    'dev': [f"{pkg}{version}" for pkg, version in dev_deps.items()],
                    'test': [
                        dep for dep in [f"{pkg}{version}" for pkg, version in dev_deps.items() if any(keyword in pkg for keyword in ['pytest', 'coverage', 'test'])]
                    ],
                    'lint': [
                        dep for dep in [f"{pkg}{version}" for pkg, version in dev_deps.items() if any(keyword in pkg for keyword in ['ruff', 'black', 'mypy', 'flake8'])]
                    ]
                }
        else:
            # Default dependencies
            config['project']['dependencies'] = ['django>=5.1.3', 'pyecore>=0.15.1']
            config['project']['optional-dependencies'] = {
                'dev': ['pytest>=8.3.4', 'pytest-django>=4.7.0', 'ruff>=0.9.7'],
                'test': ['pytest>=8.3.4', 'pytest-django>=4.7.0', 'pytest-cov>=4.1.0'],
                'lint': ['ruff>=0.9.7', 'black>=23.12.0', 'mypy>=1.8.0']
            }

        # Add tool configurations
        config.update(self._get_tool_configurations(extension_name))

        return config

    def _get_tool_configurations(self, extension_name: str) -> Dict[str, Any]:
        """Get tool configurations for development tools."""
        return {
            # Setuptools configuration
            'tool': {
                'setuptools': {
                    'packages': {'find': {'where': ['.'], 'include': [f'{extension_name}*']}},
                    'include-package-data': True,
                    'zip-safe': False,
                },

                # Setuptools dynamic configuration
                'setuptools.dynamic': {
                    'version': {'attr': f'{extension_name}.__version__'}
                },

                # Pytest configuration
                'pytest': {
                    'ini_options': {
                        'DJANGO_SETTINGS_MODULE': 'settings_extension',
                        'testpaths': ['tests'],
                        'python_files': ['test_*.py', '*_test.py'],
                        'python_classes': ['Test*'],
                        'python_functions': ['test_*'],
                        'addopts': [
                            '--verbose',
                            '--tb=short',
                            '--strict-markers',
                            '--disable-warnings'
                        ],
                        'markers': [
                            'slow: marks tests as slow',
                            'integration: marks tests as integration tests',
                            'unit: marks tests as unit tests'
                        ],
                        'filterwarnings': [
                            'ignore::DeprecationWarning',
                            'ignore::PendingDeprecationWarning'
                        ]
                    }
                },

                # Ruff configuration (linting and formatting)
                'ruff': {
                    'line-length': 88,
                    'target-version': 'py38',
                    'select': [
                        'E',   # pycodestyle errors
                        'W',   # pycodestyle warnings
                        'F',   # pyflakes
                        'I',   # isort
                        'B',   # flake8-bugbear
                        'C4',  # flake8-comprehensions
                        'UP',  # pyupgrade
                        'DJ',  # flake8-django
                    ],
                    'ignore': [
                        'E501',  # line too long (handled by formatter)
                        'B008',  # do not perform function calls in argument defaults
                        'B904',  # use raise from to specify exception cause
                    ],
                    'exclude': [
                        '.git',
                        '__pycache__',
                        '.venv',
                        'venv',
                        'env',
                        'migrations',
                        'static',
                        'media',
                        '*.egg-info',
                    ],
                    'per-file-ignores': {
                        '__init__.py': ['F401'],  # unused imports
                        'settings*.py': ['F403', 'F405'],  # star imports
                        'tests/*.py': ['D'],  # no docstring requirements for tests
                    },
                    'format': {
                        'quote-style': 'single',
                        'indent-style': 'space',
                        'skip-source-first-line': False,
                        'line-ending': 'auto',
                    }
                },

                # MyPy configuration (type checking)
                'mypy': {
                    'python_version': '3.8',
                    'check_untyped_defs': True,
                    'ignore_missing_imports': True,
                    'warn_unused_ignores': True,
                    'warn_redundant_casts': True,
                    'warn_return_any': True,
                    'strict_optional': True,
                    'disallow_untyped_decorators': False,  # Django uses many untyped decorators
                    'exclude': [
                        'migrations/',
                        'tests/',
                        'venv/',
                        '.venv/',
                    ],
                    'plugins': ['mypy_django_plugin.main'],
                },

                # Django MyPy plugin configuration
                'django-stubs': {
                    'django_settings_module': 'settings_extension'
                },

                # Coverage configuration
                'coverage': {
                    'run': {
                        'source': [f'{extension_name}'],
                        'omit': [
                            '*/migrations/*',
                            '*/tests/*',
                            '*/venv/*',
                            '*/.venv/*',
                            'manage.py',
                            'settings*.py',
                            'wsgi.py',
                            'asgi.py',
                        ]
                    },
                    'report': {
                        'exclude_lines': [
                            'pragma: no cover',
                            'def __repr__',
                            'if self.debug:',
                            'if settings.DEBUG',
                            'raise AssertionError',
                            'raise NotImplementedError',
                            'if 0:',
                            'if __name__ == .__main__.:',
                            'class .*\\bProtocol\\):',
                            '@(abc\\.)?abstractmethod',
                        ],
                        'show_missing': True,
                        'skip_covered': False,
                    },
                    'html': {
                        'directory': 'htmlcov'
                    }
                },

                # Black formatter configuration
                'black': {
                    'line-length': 88,
                    'target-version': ['py38', 'py39', 'py310', 'py311', 'py312'],
                    'include': '\\.pyi?$',
                    'exclude': '''
                    /(
                        \\.eggs
                        | \\.git
                        | \\.hg
                        | \\.mypy_cache
                        | \\.tox
                        | \\.venv
                        | _build
                        | buck-out
                        | build
                        | dist
                        | migrations
                    )/
                    '''
                },

                # Bandit security linting configuration
                'bandit': {
                    'exclude_dirs': ['tests', 'migrations'],
                    'skips': ['B101'],  # Skip assert_used test
                }
            }
        }

    def _write_pyproject_toml(self, file_handle, config: Dict[str, Any]):
        """Write the pyproject.toml configuration to file with proper formatting."""
        # Write header comment
        file_handle.write("# pyproject.toml - Modern Python packaging configuration\n")
        file_handle.write("# Generated automatically by BIRD Extension Packaging Toolkit\n")
        file_handle.write(f"# Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Write build system first
        file_handle.write("# Build system configuration\n")
        file_handle.write("[build-system]\n")
        build_system = config.pop('build-system', {})
        for key, value in build_system.items():
            if isinstance(value, list):
                formatted_list = ', '.join(f'"{item}"' for item in value)
                file_handle.write(f'{key} = [{formatted_list}]\n')
            else:
                file_handle.write(f'{key} = "{value}"\n')
        file_handle.write("\n")

        # Write project metadata
        file_handle.write("# Project metadata (PEP 621)\n")
        file_handle.write("[project]\n")
        project = config.pop('project', {})

        # Write basic project fields
        basic_fields = ['name', 'version', 'description', 'readme']
        for field in basic_fields:
            if field in project:
                file_handle.write(f'{field} = "{project[field]}"\n')

        # Write license
        if 'license' in project:
            license_info = project['license']
            if isinstance(license_info, dict):
                file_handle.write(f'license = {{text = "{license_info["text"]}"}}\n')

        # Write requires-python
        if 'requires-python' in project:
            file_handle.write(f'requires-python = "{project["requires-python"]}"\n')

        # Write authors
        if 'authors' in project:
            file_handle.write('authors = [\n')
            for author in project['authors']:
                file_handle.write(f'    {{name = "{author["name"]}", email = "{author["email"]}"}},\n')
            file_handle.write(']\n')

        # Write maintainers
        if 'maintainers' in project:
            file_handle.write('maintainers = [\n')
            for maintainer in project['maintainers']:
                file_handle.write(f'    {{name = "{maintainer["name"]}", email = "{maintainer["email"]}"}},\n')
            file_handle.write(']\n')

        # Write keywords
        if 'keywords' in project:
            file_handle.write('keywords = [\n')
            for keyword in project['keywords']:
                file_handle.write(f'    "{keyword}",\n')
            file_handle.write(']\n')

        # Write classifiers
        if 'classifiers' in project:
            file_handle.write('classifiers = [\n')
            for classifier in project['classifiers']:
                file_handle.write(f'    "{classifier}",\n')
            file_handle.write(']\n')

        # Write dependencies
        if 'dependencies' in project:
            file_handle.write('dependencies = [\n')
            for dep in project['dependencies']:
                file_handle.write(f'    "{dep}",\n')
            file_handle.write(']\n')

        # Write optional dependencies
        if 'optional-dependencies' in project:
            file_handle.write('\n[project.optional-dependencies]\n')
            for group, deps in project['optional-dependencies'].items():
                if deps:  # Only write non-empty groups
                    file_handle.write(f'{group} = [\n')
                    for dep in deps:
                        file_handle.write(f'    "{dep}",\n')
                    file_handle.write(']\n')

        # Write URLs
        if 'urls' in project:
            file_handle.write('\n[project.urls]\n')
            for name, url in project['urls'].items():
                file_handle.write(f'"{name}" = "{url}"\n')

        file_handle.write("\n")

        # Write tool configurations
        tool_config = config.get('tool', {})
        if tool_config:
            # Use toml library for complex nested structures
            tool_toml = toml.dumps({'tool': tool_config})
            file_handle.write("# Tool configurations\n")
            file_handle.write(tool_toml)

    def generate_setupcfg_fallback(self, output_path: Path, extension_name: str) -> Path:
        """Generate setup.cfg as fallback for older Python versions."""
        setupcfg_path = output_path / 'setup.cfg'

        with open(setupcfg_path, 'w', encoding='utf-8') as f:
            f.write(f"""[metadata]
name = bird-extension-{extension_name.replace('_', '-')}
version = 1.0.0
description = BIRD Bench extension: {extension_name.replace('_', ' ').title()}
long_description = file: README.md
long_description_content_type = text/markdown
license = EPL-2.0
author = Extension Developer
author_email = developer@example.com
classifiers =
    Development Status :: 4 - Beta
    Environment :: Web Environment
    Framework :: Django
    Framework :: Django :: 5.1
    Intended Audience :: Financial and Insurance Industry
    License :: OSI Approved :: Eclipse Public License 2.0 (EPL-2.0)
    Operating System :: OS Independent
    Programming Language :: Python
    Programming Language :: Python :: 3
    Programming Language :: Python :: 3.8
    Programming Language :: Python :: 3.9
    Programming Language :: Python :: 3.10
    Programming Language :: Python :: 3.11
    Programming Language :: Python :: 3.12

[options]
packages = find:
python_requires = >=3.8
install_requires =
    django>=5.1.3
    pyecore>=0.15.1
include_package_data = True
zip_safe = False

[options.packages.find]
where = .
include = {extension_name}*

[options.extras_require]
dev =
    pytest>=8.3.4
    pytest-django>=4.7.0
    ruff>=0.9.7
test =
    pytest>=8.3.4
    pytest-django>=4.7.0
    pytest-cov>=4.1.0

[tool:pytest]
DJANGO_SETTINGS_MODULE = settings_extension
testpaths = tests
python_files = test_*.py *_test.py
addopts = --verbose --tb=short

[flake8]
max-line-length = 88
exclude = .git,__pycache__,.venv,venv,env,migrations,static,media
""")

        return setupcfg_path
