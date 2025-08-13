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
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging
from .dependency_analyzer import DependencyAnalyzer

logger = logging.getLogger(__name__)

class InstallerGenerator:
    """
    Generates installation scripts and configuration files for BIRD extensions.
    Creates both automated and manual installation procedures.
    """
    
    def __init__(self):
        self.extension_name = ''
        self.has_models = False
        self.has_static = False
        self.requires_setup = False
    
    def generate(self, output_path: Path, extension_name: str, extension_path: Path = None, **kwargs):
        """
        Generate installation files for the extension.
        
        Args:
            output_path: Directory where to create installation files
            extension_name: Name of the extension
            extension_path: Path to the source extension (for dependency analysis)
            **kwargs: Additional options (has_models, has_static, etc.)
        """
        self.extension_name = extension_name
        self.has_models = kwargs.get('has_models', False)
        self.has_static = kwargs.get('has_static', True)
        self.requires_setup = kwargs.get('requires_setup', False)
        
        # Analyze dependencies if extension path provided
        self.dependency_analysis = None
        if extension_path:
            analyzer = DependencyAnalyzer()
            self.dependency_analysis = analyzer.analyze_extension(extension_path)
            logger.info(f"Analyzed dependencies: {len(self.dependency_analysis['runtime_dependencies'])} runtime, {len(self.dependency_analysis['development_dependencies'])} dev")
        
        # Generate main installation script
        install_py = self._generate_install_script(output_path)
        
        # Generate requirements files with dependency analysis
        requirements_txt = self._generate_requirements_file(output_path)
        requirements_dev_txt = self._generate_requirements_dev_file(output_path)
        
        # Generate installation configuration
        install_config = self._generate_install_config(output_path)
        
        # Generate shell installation script
        install_sh = self._generate_shell_install_script(output_path)
        
        # Generate Windows batch script
        install_bat = self._generate_batch_install_script(output_path)
        
        # Generate uninstall script
        uninstall_py = self._generate_uninstall_script(output_path)
        
        # Generate migration script
        migrate_py = self._generate_migration_script(output_path)
        
        # Generate dependency summary
        dependency_summary = self._generate_dependency_summary(output_path)
        
        logger.info(f"Generated installation files for {extension_name}")
        
        return {
            'install_py': install_py,
            'requirements_txt': requirements_txt,
            'requirements_dev_txt': requirements_dev_txt,
            'install_config': install_config,
            'install_sh': install_sh,
            'install_bat': install_bat,
            'uninstall_py': uninstall_py,
            'migrate_py': migrate_py,
            'dependency_summary': dependency_summary
        }
    
    def _generate_install_script(self, output_path: Path) -> Path:
        """Generate the main Python installation script."""
        install_path = output_path / 'install.py'
        
        with open(install_path, 'w', encoding='utf-8') as f:
            f.write('''#!/usr/bin/env python3
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
#    Extension Developer - initial API and implementation

"""
Automated installation script for the BIRD extension.
This script handles the complete installation process including:
- Dependencies installation
- Django settings integration
- Database migrations
- Static files collection
- URL pattern registration
"""

import os
import sys
import subprocess
import shutil
import json
from pathlib import Path
import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExtensionInstaller:
    """Handles automated installation of BIRD extensions."""
    
    def __init__(self, bird_project_path: str = None):
        self.bird_project_path = Path(bird_project_path) if bird_project_path else None
        self.extension_name = "''' + self.extension_name + '''"
        self.current_dir = Path(__file__).parent
        
    def install(self):
        """Run the complete installation process."""
        try:
            logger.info(f"Starting installation of {self.extension_name} extension...")
            
            # Step 1: Validate environment
            self._validate_environment()
            
            # Step 2: Install dependencies
            self._install_dependencies()
            
            # Step 3: Copy extension to BIRD project
            if self.bird_project_path:
                self._copy_extension()
            
            # Step 4: Update Django settings
            if self.bird_project_path:
                self._update_settings()
            
            # Step 5: Run migrations''')
            
            if self.has_models:
                f.write('''
            if self.bird_project_path:
                self._run_migrations()''')
            
            f.write('''
            
            # Step 6: Collect static files''')
            
            if self.has_static:
                f.write('''
            if self.bird_project_path:
                self._collect_static()''')
            
            f.write('''
            
            # Step 7: Run setup tasks''')
            
            if self.requires_setup:
                f.write('''
            self._run_setup_tasks()''')
            
            f.write(f'''
            
            logger.info(f"✅ {self.extension_name} extension installed successfully!")
            self._print_next_steps()
            
        except Exception as e:
            logger.error(f"❌ Installation failed: {{str(e)}}")
            sys.exit(1)
    
    def _validate_environment(self):
        """Validate that the environment is suitable for installation."""
        logger.info("Validating environment...")
        
        # Check Python version
        if sys.version_info < (3, 8):
            raise RuntimeError("Python 3.8 or higher is required")
        
        # Check if Django is available
        try:
            import django
            django_version = django.get_version()
            logger.info(f"Found Django {{django_version}}")
            
            if not django_version.startswith(('5.1', '5.2')):
                logger.warning(f"Django {{django_version}} may not be fully compatible")
        except ImportError:
            raise RuntimeError("Django is not installed")
        
        # Check for BIRD project if path provided
        if self.bird_project_path:
            if not self.bird_project_path.exists():
                raise RuntimeError(f"BIRD project not found at {{self.bird_project_path}}")
            
            manage_py = self.bird_project_path / 'manage.py'
            if not manage_py.exists():
                raise RuntimeError(f"Not a valid Django project: {{self.bird_project_path}}")
    
    def _install_dependencies(self):
        """Install required dependencies."""
        logger.info("Installing dependencies...")
        
        requirements_file = self.current_dir / 'requirements.txt'
        if requirements_file.exists():
            cmd = [sys.executable, '-m', 'pip', 'install', '-r', str(requirements_file)]
            subprocess.run(cmd, check=True)
        else:
            logger.warning("No requirements.txt found, skipping dependency installation")
    
    def _copy_extension(self):
        """Copy extension files to the BIRD project."""
        logger.info("Copying extension files...")
        
        extensions_dir = self.bird_project_path / 'extensions'
        extensions_dir.mkdir(exist_ok=True)
        
        target_dir = extensions_dir / self.extension_name
        if target_dir.exists():
            response = input(f"Extension {{self.extension_name}} already exists. Overwrite? [y/N]: ")
            if response.lower() != 'y':
                logger.info("Installation cancelled by user")
                sys.exit(0)
            shutil.rmtree(target_dir)
        
        # Copy extension directory
        extension_source = self.current_dir / self.extension_name
        if extension_source.exists():
            shutil.copytree(extension_source, target_dir)
        else:
            logger.error(f"Extension source directory not found: {{extension_source}}")
            raise RuntimeError("Extension files not found")
    
    def _update_settings(self):
        """Update Django settings to include the extension."""
        logger.info("Updating Django settings...")
        
        settings_file = self.bird_project_path / 'birds_nest' / 'settings.py'
        if not settings_file.exists():
            logger.warning("Django settings.py not found, manual configuration required")
            return
        
        # Read current settings
        with open(settings_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check if extension is already added
        extension_app = f"'extensions.{{self.extension_name}}'"
        if extension_app in content:
            logger.info("Extension already configured in settings")
            return
        
        # Extension should be auto-discovered by the existing discover_extensions function
        logger.info("Extension will be auto-discovered on next restart")
    
    def _run_migrations(self):
        """Run Django migrations for the extension."""
        logger.info("Running database migrations...")
        
        os.chdir(self.bird_project_path)
        
        # Make migrations for the extension
        cmd = [sys.executable, 'manage.py', 'makemigrations', self.extension_name]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.warning(f"makemigrations failed: {{result.stderr}}")
        
        # Run migrations
        cmd = [sys.executable, 'manage.py', 'migrate']
        subprocess.run(cmd, check=True)
    
    def _collect_static(self):
        """Collect static files."""
        logger.info("Collecting static files...")
        
        os.chdir(self.bird_project_path)
        cmd = [sys.executable, 'manage.py', 'collectstatic', '--noinput']
        subprocess.run(cmd, check=True)
    
    def _run_setup_tasks(self):
        """Run any additional setup tasks."""
        logger.info("Running setup tasks...")
        
        # Check for custom setup script
        setup_script = self.current_dir / 'setup_tasks.py'
        if setup_script.exists():
            logger.info("Running custom setup script...")
            subprocess.run([sys.executable, str(setup_script)], check=True)
    
    def _print_next_steps(self):
        """Print next steps for the user."""
        print("\\n" + "="*60)
        print(f"🎉 {self.extension_name.replace('_', ' ').title()} Extension Installed!")
        print("="*60)
        print()
        print("Next steps:")
        print(f"1. Restart your Django development server")
        print(f"2. Visit /extensions/{self.extension_name}/ to access the extension")
        print(f"3. Check the documentation for configuration options")
        print()
        print("Need help?")
        print(f"- Documentation: README.md")
        print(f"- Issues: Check the repository issues page")
        print()

def main():
    """Main installation function."""
    parser = argparse.ArgumentParser(description="Install BIRD extension")
    parser.add_argument(
        '--bird-path',
        help='Path to BIRD project directory'
    )
    parser.add_argument(
        '--standalone',
        action='store_true',
        help='Install as standalone (do not integrate with existing BIRD project)'
    )
    
    args = parser.parse_args()
    
    if not args.standalone and not args.bird_path:
        # Try to auto-detect BIRD project
        current = Path.cwd()
        while current != current.parent:
            if (current / 'manage.py').exists() and (current / 'pybirdai').exists():
                args.bird_path = str(current)
                print(f"Auto-detected BIRD project at: {{args.bird_path}}")
                break
            current = current.parent
        
        if not args.bird_path:
            print("BIRD project not found. Use --bird-path to specify location or --standalone for standalone installation.")
            sys.exit(1)
    
    installer = ExtensionInstaller(args.bird_path if not args.standalone else None)
    installer.install()

if __name__ == '__main__':
    main()
''')
        
        # Make the script executable
        install_path.chmod(0o755)
        return install_path
    
    def _generate_requirements_file(self, output_path: Path) -> Path:
        """Generate requirements.txt file with smart dependency detection."""
        requirements_path = output_path / 'requirements.txt'
        
        if self.dependency_analysis:
            # Use analyzed dependencies
            analyzer = DependencyAnalyzer()
            content = analyzer.generate_requirements_txt(self.dependency_analysis)
        else:
            # Fallback to static template
            content = '''# BIRD Extension Requirements
# Core dependencies required for this extension

# Django framework
django>=5.1.3

# BIRD core dependencies
pyecore>=0.15.1

# Additional dependencies
# Add any specific packages your extension needs below

# Development dependencies (optional)
# Uncomment the following for development/testing
# pytest>=8.3.4
# pytest-django>=4.7.0
# pytest-xdist>=3.6.1
# ruff>=0.9.7

# Optional dependencies
# unidecode>=1.3.8  # For text processing
# requests>=2.31.0  # For HTTP requests
# pandas>=2.0.0     # For data analysis
# numpy>=1.24.0     # For numerical computing
'''
        
        with open(requirements_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return requirements_path
    
    def _generate_requirements_dev_file(self, output_path: Path) -> Path:
        """Generate requirements-dev.txt file for development dependencies."""
        requirements_dev_path = output_path / 'requirements-dev.txt'
        
        if self.dependency_analysis:
            # Use analyzed development dependencies
            analyzer = DependencyAnalyzer()
            content = analyzer.generate_requirements_dev_txt(self.dependency_analysis)
        else:
            # Fallback to static template
            content = '''# Development Dependencies
# Install with: pip install -r requirements-dev.txt

# Testing framework
pytest>=8.3.4
pytest-django>=4.7.0
pytest-xdist>=3.6.1
pytest-cov>=4.1.0

# Code formatting and linting
ruff>=0.9.7
black>=23.12.0

# Type checking
mypy>=1.8.0
django-stubs>=4.2.0

# Coverage reporting
coverage>=7.4.0

# Pre-commit hooks
pre-commit>=3.6.0
'''
        
        with open(requirements_dev_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return requirements_dev_path
    
    def _generate_dependency_summary(self, output_path: Path) -> Path:
        """Generate dependency analysis summary."""
        summary_path = output_path / 'DEPENDENCIES.md'
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(f"# Dependency Analysis - {self.extension_name.replace('_', ' ').title()}\n\n")
            
            if self.dependency_analysis:
                analyzer = DependencyAnalyzer()
                summary_content = analyzer.get_dependency_summary(self.dependency_analysis)
                f.write(summary_content)
                
                # Add installation instructions
                f.write("\n## Installation Instructions\n\n")
                f.write("### Runtime Dependencies\n")
                f.write("```bash\n")
                f.write("pip install -r requirements.txt\n")
                f.write("```\n\n")
                f.write("### Development Dependencies\n")
                f.write("```bash\n")
                f.write("pip install -r requirements.txt -r requirements-dev.txt\n")
                f.write("```\n\n")
                f.write("### Using uv (recommended)\n")
                f.write("```bash\n")
                f.write("uv add -r requirements.txt\n")
                f.write("uv add -r requirements-dev.txt --group dev\n")
                f.write("```\n\n")
                
                # Add troubleshooting section if there were errors
                if self.dependency_analysis['analysis_errors']:
                    f.write("## Analysis Warnings\n\n")
                    f.write("The following files had analysis issues:\n\n")
                    for error in self.dependency_analysis['analysis_errors']:
                        f.write(f"- {error}\n")
                    f.write("\nThese may indicate missing dependencies that need to be added manually.\n\n")
            else:
                f.write("Dependency analysis was not performed during packaging.\n")
                f.write("Please review the requirements.txt file and add any missing dependencies manually.\n")
        
        return summary_path
    
    def _generate_install_config(self, output_path: Path) -> Path:
        """Generate installation configuration file."""
        config_path = output_path / 'install_config.json'
        
        config = {
            'extension_name': self.extension_name,
            'version': '1.0.0',
            'requires_migration': self.has_models,
            'requires_static_collection': self.has_static,
            'requires_setup': self.requires_setup,
            'compatible_bird_versions': ['5.1.x'],
            'compatible_django_versions': ['5.1.x', '5.2.x'],
            'python_version_min': '3.8',
            'installation_steps': [
                'validate_environment',
                'install_dependencies',
                'copy_extension_files',
                'update_settings',
            ],
            'post_install_commands': [],
            'configuration_files': [
                'settings_extension.py',
                'local_settings.py.template'
            ]
        }
        
        if self.has_models:
            config['installation_steps'].append('run_migrations')
        
        if self.has_static:
            config['installation_steps'].append('collect_static')
        
        if self.requires_setup:
            config['installation_steps'].append('run_setup_tasks')
            config['post_install_commands'].append('python setup_tasks.py')
        
        with open(config_path, 'w', encoding='utf-8') as f:
            import json
            json.dump(config, f, indent=2)
        
        return config_path
    
    def _generate_shell_install_script(self, output_path: Path) -> Path:
        """Generate shell installation script for Unix/Linux."""
        install_sh_path = output_path / 'install.sh'
        
        with open(install_sh_path, 'w', encoding='utf-8') as f:
            f.write(f'''#!/bin/bash
# BIRD Extension Installation Script
# Installs {self.extension_name.replace('_', ' ').title()} extension

set -e

echo "🚀 Installing {self.extension_name.replace('_', ' ').title()} Extension..."

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed"
    exit 1
fi

# Check Python version
python3 -c "import sys; assert sys.version_info >= (3, 8), 'Python 3.8+ required'"

# Run the Python installer
python3 install.py "$@"

echo "✅ Installation complete!"
''')
        
        # Make executable
        install_sh_path.chmod(0o755)
        return install_sh_path
    
    def _generate_batch_install_script(self, output_path: Path) -> Path:
        """Generate Windows batch installation script."""
        install_bat_path = output_path / 'install.bat'
        
        with open(install_bat_path, 'w', encoding='utf-8') as f:
            f.write(f'''@echo off
REM BIRD Extension Installation Script
REM Installs {self.extension_name.replace('_', ' ').title()} extension

echo 🚀 Installing {self.extension_name.replace('_', ' ').title()} Extension...

REM Check if Python is available
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Python is required but not installed
    exit /b 1
)

REM Run the Python installer
python install.py %*

echo ✅ Installation complete!
pause
''')
        
        return install_bat_path
    
    def _generate_uninstall_script(self, output_path: Path) -> Path:
        """Generate uninstallation script."""
        uninstall_path = output_path / 'uninstall.py'
        
        with open(uninstall_path, 'w', encoding='utf-8') as f:
            f.write(f'''#!/usr/bin/env python3
# coding=UTF-8
# Uninstall script for {self.extension_name} extension

import os
import sys
import shutil
from pathlib import Path
import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExtensionUninstaller:
    """Handles removal of BIRD extensions."""
    
    def __init__(self, bird_project_path: str = None):
        self.bird_project_path = Path(bird_project_path) if bird_project_path else None
        self.extension_name = "{self.extension_name}"
    
    def uninstall(self):
        """Remove the extension."""
        try:
            logger.info(f"Uninstalling {{self.extension_name}} extension...")
            
            if self.bird_project_path:
                self._remove_extension_files()
                self._cleanup_database()
                self._cleanup_static_files()
            
            logger.info("✅ Extension uninstalled successfully!")
            print("\\nNote: You may need to restart your Django server.")
            
        except Exception as e:
            logger.error(f"❌ Uninstallation failed: {{str(e)}}")
            sys.exit(1)
    
    def _remove_extension_files(self):
        """Remove extension files from the project."""
        logger.info("Removing extension files...")
        
        extension_dir = self.bird_project_path / 'extensions' / self.extension_name
        if extension_dir.exists():
            shutil.rmtree(extension_dir)
            logger.info(f"Removed {{extension_dir}}")
        else:
            logger.warning("Extension files not found")
    
    def _cleanup_database(self):
        """Clean up database tables (requires manual confirmation)."""''')
            
            if self.has_models:
                f.write('''
        logger.info("Database cleanup required...")
        print("\\nWARNING: This extension created database tables.")
        print("You may want to run the following commands manually:")
        print(f"  python manage.py migrate {{self.extension_name}} zero")
        print(f"  python manage.py showmigrations {{self.extension_name}}")
        ''')
            
            f.write('''
    
    def _cleanup_static_files(self):
        """Clean up static files."""''')
        
            if self.has_static:
                f.write('''
        logger.info("Cleaning up static files...")
        static_dir = self.bird_project_path / 'static' / self.extension_name
        if static_dir.exists():
            shutil.rmtree(static_dir)
            logger.info(f"Removed static files: {{static_dir}}")
        ''')
            
            f.write('''

def main():
    """Main uninstallation function."""
    parser = argparse.ArgumentParser(description="Uninstall BIRD extension")
    parser.add_argument('--bird-path', help='Path to BIRD project directory')
    
    args = parser.parse_args()
    
    if not args.bird_path:
        # Try to auto-detect
        current = Path.cwd()
        while current != current.parent:
            if (current / 'manage.py').exists():
                args.bird_path = str(current)
                break
            current = current.parent
        
        if not args.bird_path:
            print("BIRD project not found. Use --bird-path to specify location.")
            sys.exit(1)
    
    response = input(f"Are you sure you want to uninstall {self.extension_name}? [y/N]: ")
    if response.lower() != 'y':
        print("Uninstallation cancelled")
        sys.exit(0)
    
    uninstaller = ExtensionUninstaller(args.bird_path)
    uninstaller.uninstall()

if __name__ == '__main__':
    main()
''')
        
        # Make executable
        uninstall_path.chmod(0o755)
        return uninstall_path
    
    def _generate_migration_script(self, output_path: Path) -> Path:
        """Generate migration helper script."""
        migrate_path = output_path / 'migrate.py'
        
        with open(migrate_path, 'w', encoding='utf-8') as f:
            f.write(f'''#!/usr/bin/env python3
# coding=UTF-8
# Migration helper script for {self.extension_name} extension

import os
import sys
import subprocess
from pathlib import Path

def run_migrations(bird_path):
    """Run migrations for the extension."""
    bird_path = Path(bird_path)
    
    if not bird_path.exists():
        print(f"BIRD project not found at {{bird_path}}")
        sys.exit(1)
    
    os.chdir(bird_path)
    
    print("Making migrations...")
    subprocess.run([
        sys.executable, 'manage.py', 'makemigrations', '{self.extension_name}'
    ], check=True)
    
    print("Running migrations...")
    subprocess.run([
        sys.executable, 'manage.py', 'migrate'
    ], check=True)
    
    print("✅ Migrations completed successfully!")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python migrate.py <bird_project_path>")
        sys.exit(1)
    
    run_migrations(sys.argv[1])
''')
        
        # Make executable
        migrate_path.chmod(0o755)
        return migrate_path