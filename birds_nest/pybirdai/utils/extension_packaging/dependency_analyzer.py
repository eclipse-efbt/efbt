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
import sys
import re
from pathlib import Path
from typing import Dict, Set, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class DependencyAnalyzer:
    """
    Analyzes Python code to automatically detect dependencies and generate
    requirements files for BIRD extensions.
    """
    
    # Standard library modules (Python 3.8+)
    STDLIB_MODULES = {
        'os', 'sys', 'json', 'math', 'datetime', 'time', 'random', 'uuid',
        'collections', 'itertools', 'functools', 'operator', 're', 'string',
        'pathlib', 'urllib', 'http', 'email', 'html', 'xml', 'sqlite3',
        'csv', 'configparser', 'logging', 'argparse', 'subprocess', 'shutil',
        'tempfile', 'glob', 'fnmatch', 'io', 'typing', 'dataclasses',
        'enum', 'abc', 'contextlib', 'warnings', 'inspect', 'unittest',
        'asyncio', 'concurrent', 'multiprocessing', 'threading', 'queue',
        'socket', 'ssl', 'hashlib', 'hmac', 'secrets', 'base64', 'binascii',
        'struct', 'codecs', 'locale', 'gettext', 'platform', 'copy', 'pickle',
        'weakref', 'gc', 'ctypes', 'array', 'memoryview', 'zlib', 'gzip',
        'bz2', 'lzma', 'zipfile', 'tarfile', 'importlib', 'pkgutil',
        'modulefinder', 'runpy', 'trace', 'traceback', 'linecache',
        'tokenize', 'keyword', 'parser', 'symbol', 'token', 'dis',
        'pickletools', 'formatter', 'calendar', 'decimal', 'fractions',
        'statistics', 'heapq', 'bisect', 'pprint', 'reprlib', 'textwrap'
    }
    
    # Core BIRD framework dependencies
    BIRD_CORE_DEPS = {
        'django': '>=5.1.3',
        'pyecore': '>=0.15.1'
    }
    
    # Common third-party packages with recommended versions
    COMMON_PACKAGES = {
        'pandas': '>=2.0.0,<3.0.0',
        'numpy': '>=1.24.0,<2.0.0',
        'scipy': '>=1.10.0,<2.0.0',
        'requests': '>=2.31.0,<3.0.0',
        'matplotlib': '>=3.7.0,<4.0.0',
        'seaborn': '>=0.12.0,<1.0.0',
        'plotly': '>=5.15.0,<6.0.0',
        'openpyxl': '>=3.1.0,<4.0.0',
        'xlsxwriter': '>=3.1.0,<4.0.0',
        'pillow': '>=10.0.0,<11.0.0',
        'celery': '>=5.3.0,<6.0.0',
        'redis': '>=4.6.0,<5.0.0',
        'psycopg2-binary': '>=2.9.7,<3.0.0',
        'mysqlclient': '>=2.2.0,<3.0.0',
        'sqlalchemy': '>=2.0.0,<3.0.0',
        'click': '>=8.1.0,<9.0.0',
        'jinja2': '>=3.1.0,<4.0.0',
        'pyyaml': '>=6.0,<7.0',
        'toml': '>=0.10.2,<1.0.0',
        'beautifulsoup4': '>=4.12.0,<5.0.0',
        'lxml': '>=4.9.0,<5.0.0',
        'python-dateutil': '>=2.8.0,<3.0.0',
        'pytz': '>=2023.3',
        'cryptography': '>=41.0.0,<42.0.0',
        'pyjwt': '>=2.8.0,<3.0.0',
        'reportlab': '>=4.0.0,<5.0.0',
        'weasyprint': '>=60.0,<61.0',
        'xhtml2pdf': '>=0.2.11,<1.0.0'
    }
    
    # Development dependencies
    DEV_PACKAGES = {
        'pytest': '>=8.3.4',
        'pytest-django': '>=4.7.0',
        'pytest-xdist': '>=3.6.1',
        'pytest-cov': '>=4.1.0',
        'ruff': '>=0.9.7',
        'black': '>=23.12.0',
        'mypy': '>=1.8.0',
        'django-stubs': '>=4.2.0',
        'coverage': '>=7.4.0',
        'pre-commit': '>=3.6.0',
        'tox': '>=4.11.0',
        'sphinx': '>=7.2.0',
        'sphinx-rtd-theme': '>=2.0.0'
    }
    
    def __init__(self):
        self.runtime_deps = set()
        self.dev_deps = set()
        self.local_imports = set()
        self.conditional_imports = set()
        self.import_errors = []
    
    def analyze_extension(self, extension_path: Path) -> Dict[str, any]:
        """
        Analyze an extension directory to detect all dependencies.
        
        Args:
            extension_path: Path to the extension directory
            
        Returns:
            Dict containing dependency analysis results
        """
        logger.info(f"Analyzing dependencies for extension: {extension_path.name}")
        
        # Reset analysis state
        self.runtime_deps = set()
        self.dev_deps = set()
        self.local_imports = set()
        self.conditional_imports = set()
        self.import_errors = []
        
        # Find all Python files
        python_files = self._find_python_files(extension_path)
        logger.info(f"Found {len(python_files)} Python files to analyze")
        
        # Analyze each file
        for file_path in python_files:
            try:
                self._analyze_file(file_path, extension_path)
            except Exception as e:
                logger.warning(f"Failed to analyze {file_path}: {e}")
                self.import_errors.append(f"{file_path}: {str(e)}")
        
        # Categorize dependencies
        runtime_requirements = self._categorize_dependencies()
        dev_requirements = self._get_dev_requirements()
        
        return {
            'runtime_dependencies': runtime_requirements,
            'development_dependencies': dev_requirements,
            'local_imports': list(self.local_imports),
            'conditional_imports': list(self.conditional_imports),
            'analysis_errors': self.import_errors,
            'total_files_analyzed': len(python_files),
            'core_dependencies': dict(self.BIRD_CORE_DEPS)
        }
    
    def _find_python_files(self, extension_path: Path) -> List[Path]:
        """Find all Python files in the extension directory."""
        python_files = []
        
        # Include all .py files recursively
        for file_path in extension_path.rglob('*.py'):
            # Skip __pycache__ directories and .pyc files
            if '__pycache__' not in str(file_path):
                python_files.append(file_path)
        
        return sorted(python_files)
    
    def _analyze_file(self, file_path: Path, extension_root: Path):
        """Analyze a single Python file for imports."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Parse AST
            tree = ast.parse(content, filename=str(file_path))
            
            # Visit all import nodes
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        self._process_import(alias.name, file_path, extension_root)
                
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        self._process_import(node.module, file_path, extension_root)
                
                elif isinstance(node, ast.Try):
                    # Handle try/except imports (conditional imports)
                    self._analyze_try_imports(node, file_path, extension_root)
        
        except SyntaxError as e:
            logger.warning(f"Syntax error in {file_path}: {e}")
            self.import_errors.append(f"{file_path}: Syntax error - {str(e)}")
        except Exception as e:
            logger.error(f"Error analyzing {file_path}: {e}")
            self.import_errors.append(f"{file_path}: Analysis error - {str(e)}")
    
    def _analyze_try_imports(self, try_node: ast.Try, file_path: Path, extension_root: Path):
        """Analyze imports inside try/except blocks."""
        for stmt in try_node.body:
            if isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    self.conditional_imports.add(alias.name)
                    self._process_import(alias.name, file_path, extension_root)
            elif isinstance(stmt, ast.ImportFrom):
                if stmt.module:
                    self.conditional_imports.add(stmt.module)
                    self._process_import(stmt.module, file_path, extension_root)
    
    def _process_import(self, import_name: str, file_path: Path, extension_root: Path):
        """Process a single import statement."""
        # Get the root module name
        root_module = import_name.split('.')[0]
        
        # Skip if it's a standard library module
        if root_module in self.STDLIB_MODULES:
            return
        
        # Check if it's a local import (relative to extension)
        if self._is_local_import(import_name, file_path, extension_root):
            self.local_imports.add(import_name)
            return
        
        # Check if it's a Django internal import
        if root_module == 'django':
            return  # Django is already in core deps
        
        # Check if it's a BIRD internal import
        if root_module in ['pybirdai', 'extensions']:
            return
        
        # Add to runtime dependencies
        self.runtime_deps.add(root_module)
    
    def _is_local_import(self, import_name: str, file_path: Path, extension_root: Path) -> bool:
        """Check if an import is local to the extension."""
        # Check for relative imports (starting with .)
        if import_name.startswith('.'):
            return True
        
        # Check if the import corresponds to a file in the extension
        parts = import_name.split('.')
        current_path = extension_root
        
        for part in parts:
            potential_file = current_path / f"{part}.py"
            potential_dir = current_path / part
            
            if potential_file.exists() or (potential_dir.exists() and potential_dir.is_dir()):
                current_path = potential_dir
                continue
            else:
                return False
        
        return True
    
    def _categorize_dependencies(self) -> Dict[str, str]:
        """Categorize runtime dependencies with version specifications."""
        requirements = {}
        
        # Add core BIRD dependencies
        requirements.update(self.BIRD_CORE_DEPS)
        
        # Add detected third-party dependencies
        for dep in self.runtime_deps:
            if dep in self.COMMON_PACKAGES:
                requirements[dep] = self.COMMON_PACKAGES[dep]
            else:
                # For unknown packages, use a conservative version spec
                requirements[dep] = '>=1.0.0'
        
        return requirements
    
    def _get_dev_requirements(self) -> Dict[str, str]:
        """Get development dependencies."""
        dev_deps = {}
        
        # Always include basic dev tools
        basic_dev_tools = ['pytest', 'pytest-django', 'ruff']
        
        for tool in basic_dev_tools:
            if tool in self.DEV_PACKAGES:
                dev_deps[tool] = self.DEV_PACKAGES[tool]
        
        # Add coverage if tests are detected
        if any('test' in str(f) for f in self.local_imports):
            dev_deps.update({
                'pytest-cov': self.DEV_PACKAGES['pytest-cov'],
                'coverage': self.DEV_PACKAGES['coverage']
            })
        
        # Add type checking if type hints are used
        if self._has_type_hints():
            dev_deps.update({
                'mypy': self.DEV_PACKAGES['mypy'],
                'django-stubs': self.DEV_PACKAGES['django-stubs']
            })
        
        return dev_deps
    
    def _has_type_hints(self) -> bool:
        """Check if the extension uses type hints."""
        # This is a simplified check - in reality, we'd parse the AST more thoroughly
        return 'typing' in self.runtime_deps
    
    def generate_requirements_txt(self, analysis_result: Dict) -> str:
        """Generate requirements.txt content."""
        lines = [
            "# BIRD Extension Requirements",
            "# Generated automatically by Extension Packaging Toolkit",
            "",
            "# Core BIRD Framework Dependencies",
        ]
        
        # Core dependencies
        core_deps = analysis_result['core_dependencies']
        for package, version in sorted(core_deps.items()):
            lines.append(f"{package}{version}")
        
        lines.append("")
        lines.append("# Extension-Specific Dependencies")
        
        # Runtime dependencies
        runtime_deps = analysis_result['runtime_dependencies']
        extension_deps = {k: v for k, v in runtime_deps.items() 
                         if k not in core_deps}
        
        if extension_deps:
            for package, version in sorted(extension_deps.items()):
                lines.append(f"{package}{version}")
        else:
            lines.append("# No additional runtime dependencies detected")
        
        # Development dependencies (commented out by default)
        dev_deps = analysis_result['development_dependencies']
        if dev_deps:
            lines.extend([
                "",
                "# Development Dependencies (uncomment as needed)",
                "# Install with: pip install -r requirements.txt -r requirements-dev.txt",
            ])
            for package, version in sorted(dev_deps.items()):
                lines.append(f"# {package}{version}")
        
        return '\n'.join(lines) + '\n'
    
    def generate_requirements_dev_txt(self, analysis_result: Dict) -> str:
        """Generate requirements-dev.txt content."""
        lines = [
            "# Development Dependencies",
            "# Install with: pip install -r requirements-dev.txt",
            "",
        ]
        
        dev_deps = analysis_result['development_dependencies']
        for package, version in sorted(dev_deps.items()):
            lines.append(f"{package}{version}")
        
        if not dev_deps:
            lines.append("# No development dependencies specified")
        
        return '\n'.join(lines) + '\n'
    
    def get_dependency_summary(self, analysis_result: Dict) -> str:
        """Generate a human-readable dependency summary."""
        runtime_count = len(analysis_result['runtime_dependencies'])
        dev_count = len(analysis_result['development_dependencies'])
        files_analyzed = analysis_result['total_files_analyzed']
        errors = len(analysis_result['analysis_errors'])
        
        summary = f"""Dependency Analysis Summary:
- Files analyzed: {files_analyzed}
- Runtime dependencies: {runtime_count}
- Development dependencies: {dev_count}
- Local imports detected: {len(analysis_result['local_imports'])}
- Conditional imports: {len(analysis_result['conditional_imports'])}
- Analysis errors: {errors}

Core BIRD dependencies:
"""
        
        for package, version in analysis_result['core_dependencies'].items():
            summary += f"  - {package}{version}\n"
        
        if analysis_result['runtime_dependencies']:
            summary += "\nExtension-specific dependencies:\n"
            extension_deps = {k: v for k, v in analysis_result['runtime_dependencies'].items() 
                             if k not in analysis_result['core_dependencies']}
            for package, version in sorted(extension_deps.items()):
                summary += f"  - {package}{version}\n"
        
        if analysis_result['analysis_errors']:
            summary += f"\nWarnings/Errors:\n"
            for error in analysis_result['analysis_errors'][:5]:  # Show max 5 errors
                summary += f"  - {error}\n"
            if len(analysis_result['analysis_errors']) > 5:
                summary += f"  ... and {len(analysis_result['analysis_errors']) - 5} more\n"
        
        return summary