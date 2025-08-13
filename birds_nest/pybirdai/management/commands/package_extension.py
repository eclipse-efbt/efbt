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
import sys
import tempfile
import shutil
from pybirdai.utils.extension_packaging import GitManager, InstallerGenerator, ReadmeGenerator
import re
import ast

class Command(BaseCommand):
    help = 'Package a BIRD extension and push it to a Git repository'

    def add_arguments(self, parser):
        parser.add_argument(
            '--name',
            type=str,
            required=True,
            help='Name of the extension to package'
        )
        parser.add_argument(
            '--github-user',
            type=str,
            help='GitHub username or organization'
        )
        parser.add_argument(
            '--gitlab-user',
            type=str,
            help='GitLab username or organization'
        )
        parser.add_argument(
            '--repo-name',
            type=str,
            required=True,
            help='Name of the repository to create'
        )
        parser.add_argument(
            '--token',
            type=str,
            help='GitHub/GitLab personal access token (can also use GITHUB_TOKEN or GITLAB_TOKEN env var)'
        )
        parser.add_argument(
            '--private',
            action='store_true',
            default=False,
            help='Create a private repository'
        )
        parser.add_argument(
            '--no-push',
            action='store_true',
            default=False,
            help='Create repository but do not push code'
        )
        parser.add_argument(
            '--output-dir',
            type=str,
            help='Output directory for packaged extension (default: temp directory)'
        )
        parser.add_argument(
            '--author',
            type=str,
            default='BIRD Extension Developer',
            help='Author name for the extension'
        )
        parser.add_argument(
            '--author-email',
            type=str,
            default='developer@example.com',
            help='Author email for the extension'
        )
        parser.add_argument(
            '--license',
            type=str,
            default='EPL-2.0',
            help='License for the extension'
        )
        parser.add_argument(
            '--ext-version',
            type=str,
            default='1.0.0',
            help='Version of the extension'
        )
        parser.add_argument(
            '--description',
            type=str,
            help='Description of the extension'
        )

    def handle(self, *args, **options):
        extension_name = options['name']
        repo_name = options['repo_name']

        # Determine platform
        github_user = options.get('github_user')
        gitlab_user = options.get('gitlab_user')

        if not github_user and not gitlab_user:
            raise CommandError('Please specify either --github-user or --gitlab-user')

        if github_user and gitlab_user:
            raise CommandError('Please specify only one of --github-user or --gitlab-user')

        platform = 'github' if github_user else 'gitlab'
        username = github_user if github_user else gitlab_user

        # Get token
        token = options.get('token')
        if not token:
            env_var = 'GITHUB_TOKEN' if platform == 'github' else 'GITLAB_TOKEN'
            token = os.environ.get(env_var)
            if not token and not options['no_push']:
                raise CommandError(f'Please provide --token or set {env_var} environment variable')

        # Paths
        base_dir = Path(settings.BASE_DIR)
        extensions_dir = base_dir / 'extensions'
        extension_path = extensions_dir / extension_name

        if not extension_path.exists():
            raise CommandError(f'Extension "{extension_name}" not found in {extensions_dir}')

        # Create output directory
        if options['output_dir']:
            output_dir = Path(options['output_dir'])
            output_dir.mkdir(parents=True, exist_ok=True)
        else:
            output_dir = Path(tempfile.mkdtemp(prefix=f'bird-ext-{extension_name}-'))

        package_dir = output_dir / repo_name

        try:
            self.stdout.write(f'Packaging extension "{extension_name}"...')

            # Step 1: Copy extension directory directly
            extension_target = package_dir / extension_name
            shutil.copytree(extension_path, extension_target)
            self.stdout.write(f'✓ Extension folder copied to {extension_target}')

            # Step 2: Generate requirements.txt from basic dependency analysis
            self._generate_requirements_txt(extension_path, package_dir)

            # Step 3: Generate installer with dependency analysis
            installer_gen = InstallerGenerator()
            installer_gen.generate(
                package_dir,
                extension_name,
                extension_path=extension_path,
                has_models=len([f for f in extension_path.rglob('models.py') if f.exists()]) > 0,
                has_static=len(list((extension_path / 'static').rglob('*'))) > 0 if (extension_path / 'static').exists() else False
            )

            # Step 4: Generate README with dependency information
            readme_gen = ReadmeGenerator()
            readme_gen.generate(
                package_dir,
                extension_name,
                repo_name,
                username,
                platform,
                options['ext_version'],
                options.get('description', f'BIRD Bench extension: {extension_name}'),
                dependency_analysis=None  # We're not doing full dependency analysis anymore
            )

            self.stdout.write(self.style.SUCCESS(f'Extension packaged to {package_dir}'))

            # Step 3: Initialize Git and push
            if not options['no_push']:
                git_manager = GitManager(platform, token)

                # Initialize local repository
                git_manager.init_repository(package_dir)

                # Create .gitignore
                git_manager.create_gitignore(package_dir)

                # Add and commit files
                git_manager.add_and_commit(
                    package_dir,
                    f'Initial commit of {extension_name} extension'
                )

                # Create remote repository
                repo_url = git_manager.create_remote_repository(
                    username,
                    repo_name,
                    description=options.get('description', f'BIRD Bench extension: {extension_name}'),
                    private=options['private']
                )

                # Push to remote
                git_manager.push_to_remote(package_dir, repo_url)

                self.stdout.write(
                    self.style.SUCCESS(
                        f'Extension successfully pushed to {repo_url}\n'
                        f'Clone with: git clone {repo_url}'
                    )
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS(
                        f'Extension packaged successfully at {package_dir}\n'
                        f'To push manually:\n'
                        f'  cd {package_dir}\n'
                        f'  git init\n'
                        f'  git add .\n'
                        f'  git commit -m "Initial commit"\n'
                        f'  git remote add origin <repository-url>\n'
                        f'  git push -u origin main'
                    )
                )

            # Clean up temp directory if not using custom output
            if not options['output_dir']:
                self.stdout.write(f'Package saved to temporary directory: {output_dir}')
                self.stdout.write('To keep the files, copy them before they are cleaned up.')

        except Exception as e:
            raise CommandError(f'Failed to package extension: {str(e)}')
    
    def _generate_requirements_txt(self, extension_path, package_dir):
        """Generate a simple requirements.txt file from extension dependencies"""
        self.stdout.write('Generating requirements.txt...')
        
        # Basic dependencies for BIRD extensions
        base_dependencies = [
            'django>=4.0',
            'pyecore',
        ]
        
        detected_dependencies = set(base_dependencies)
        
        # Scan Python files for import statements
        for python_file in extension_path.rglob('*.py'):
            try:
                with open(python_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Parse the file to find imports
                try:
                    tree = ast.parse(content)
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Import):
                            for name in node.names:
                                dep = self._map_import_to_dependency(name.name)
                                if dep:
                                    detected_dependencies.add(dep)
                        elif isinstance(node, ast.ImportFrom):
                            if node.module:
                                dep = self._map_import_to_dependency(node.module)
                                if dep:
                                    detected_dependencies.add(dep)
                except SyntaxError:
                    # Skip files with syntax errors
                    continue
                    
            except Exception:
                # Skip files that can't be read
                continue
        
        # Write requirements.txt
        requirements_file = package_dir / 'requirements.txt'
        with open(requirements_file, 'w', encoding='utf-8') as f:
            for dep in sorted(detected_dependencies):
                f.write(f'{dep}\n')
        
        self.stdout.write(f'✓ Requirements.txt generated with {len(detected_dependencies)} dependencies')
    
    def _map_import_to_dependency(self, import_name):
        """Map import names to pip package names"""
        # Common mappings from import names to pip package names
        mapping = {
            'numpy': 'numpy',
            'pandas': 'pandas',
            'scipy': 'scipy',
            'requests': 'requests',
            'yaml': 'PyYAML',
            'PIL': 'Pillow',
            'cv2': 'opencv-python',
            'sklearn': 'scikit-learn',
            'bs4': 'beautifulsoup4',
            'dateutil': 'python-dateutil',
        }
        
        # Get the top-level module name
        top_level = import_name.split('.')[0]
        
        # Skip standard library modules
        stdlib_modules = {
            'os', 'sys', 'json', 'datetime', 'logging', 'pathlib', 're', 'tempfile',
            'shutil', 'subprocess', 'urllib', 'http', 'collections', 'itertools',
            'functools', 'operator', 'math', 'random', 'hashlib', 'base64', 'uuid',
            'typing', 'dataclasses', 'enum', 'abc', 'contextlib', 'warnings'
        }
        
        # Skip Django and local modules
        if (top_level in stdlib_modules or 
            top_level.startswith('django') or 
            top_level.startswith('extensions') or
            top_level.startswith('pybirdai')):
            return None
        
        return mapping.get(top_level, None)
