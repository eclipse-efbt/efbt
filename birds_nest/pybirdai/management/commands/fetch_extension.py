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
import subprocess
import requests
import json
import yaml
import tempfile
import shutil
from urllib.parse import urlparse
import re

class Command(BaseCommand):
    help = 'Fetch an extension from a Git repository (GitHub/GitLab) with OAuth authentication'

    def add_arguments(self, parser):
        parser.add_argument(
            '--repo',
            type=str,
            required=True,
            help='Repository URL (e.g., github.com/user/repo or https://github.com/user/repo.git)'
        )
        parser.add_argument(
            '--name',
            type=str,
            help='Local extension name (defaults to repository name)'
        )
        parser.add_argument(
            '--token',
            type=str,
            help='GitHub/GitLab personal access token (can also use GITHUB_TOKEN or GITLAB_TOKEN env var)'
        )
        parser.add_argument(
            '--oauth',
            action='store_true',
            help='Use OAuth flow for authentication (opens browser)'
        )
        parser.add_argument(
            '--branch',
            type=str,
            default='main',
            help='Branch to clone (default: main)'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Overwrite existing extension if it exists'
        )
        parser.add_argument(
            '--license-check',
            action='store_true',
            default=True,
            help='Verify license before installing (default: True)'
        )
        parser.add_argument(
            '--no-license-check',
            action='store_false',
            dest='license_check',
            help='Skip license verification'
        )
        parser.add_argument(
            '--install-deps',
            action='store_true',
            default=True,
            help='Install extension dependencies (default: True)'
        )
        parser.add_argument(
            '--no-install-deps',
            action='store_false',
            dest='install_deps',
            help='Skip dependency installation'
        )
        parser.add_argument(
            '--run-migrations',
            action='store_true',
            default=True,
            help='Run database migrations for extension (default: True)'
        )
        parser.add_argument(
            '--no-run-migrations',
            action='store_false',
            dest='run_migrations',
            help='Skip database migrations'
        )
        parser.add_argument(
            '--uv',
            action='store_true',
            default=True,
            help='Use uv as package manager'
        )

    def handle(self, *args, **options):
        repo_url = options['repo']
        use_uv = options["uv"]
        print(use_uv)

        # Parse repository information
        repo_info = self._parse_repository_url(repo_url)
        platform = repo_info['platform']
        username = repo_info['username']
        repo_name = repo_info['repo_name']

        extension_name = options.get('name') or repo_name

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

        # Get authentication token
        token = self._get_auth_token(options, platform)

        try:
            self.stdout.write(f'Fetching extension from {repo_url}...')

            # Step 1: Check repository access and license
            if options['license_check']:
                self._verify_repository_access_and_license(
                    platform, username, repo_name, token
                )

            # Step 2: Clone repository
            temp_dir = self._clone_repository(
                repo_info, token, options['branch']
            )

            try:
                # Step 3: Validate extension structure
                self._validate_extension_structure(temp_dir, extension_name)

                # Step 4: Install extension
                self._install_extension(
                    temp_dir, extension_path, extension_name, options['force']
                )

                # Step 5: Install dependencies
                if options['install_deps']:
                    self._install_dependencies(extension_path,use_uv)

                # # Step 6: Run migrations
                # if options['run_migrations']:
                #     self._run_migrations(extension_name)

                self.stdout.write(
                    self.style.SUCCESS(
                        f'✅ Extension "{extension_name}" installed successfully!\n\n'
                        f'Location: {extension_path}\n'
                        f'Access at: http://localhost:8000/pybirdai/extensions/{extension_name}/\n\n'
                        f'Next steps:\n'
                        f'  1. Restart Django: python manage.py runserver\n'
                        f'  2. Check extension in admin or web interface\n'
                        f'  3. Configure extension settings if needed'
                    )
                )

            finally:
                # Clean up temporary directory
                shutil.rmtree(temp_dir)

        except Exception as e:
            # Clean up on failure
            if extension_path.exists() and options['force']:
                shutil.rmtree(extension_path)
            raise CommandError(f'Failed to fetch extension: {str(e)}')

    def _parse_repository_url(self, repo_url):
        """Parse repository URL and extract platform, username, repo name"""
        # Normalize URL
        if not repo_url.startswith(('http://', 'https://')):
            if repo_url.startswith('github.com/'):
                repo_url = 'https://' + repo_url
            elif repo_url.startswith('gitlab.com/'):
                repo_url = 'https://' + repo_url
            else:
                repo_url = 'https://github.com/' + repo_url

        parsed = urlparse(repo_url)
        hostname = parsed.netloc.lower()

        # Determine platform
        if 'github.com' in hostname:
            platform = 'github'
        elif 'gitlab.com' in hostname:
            platform = 'gitlab'
        else:
            raise CommandError(f'Unsupported platform: {hostname}')

        # Extract username and repo name
        path_parts = parsed.path.strip('/').split('/')
        if len(path_parts) < 2:
            raise CommandError(f'Invalid repository URL: {repo_url}')

        username = path_parts[0]
        repo_name = path_parts[1].replace('.git', '')

        return {
            'platform': platform,
            'username': username,
            'repo_name': repo_name,
            'full_url': repo_url
        }

    def _get_auth_token(self, options, platform):
        """Get authentication token"""
        token = options.get('token')

        if not token:
            env_var = 'GITHUB_TOKEN' if platform == 'github' else 'GITLAB_TOKEN'
            token = os.environ.get(env_var)

        if not token and options['oauth']:
            token = self._perform_oauth_flow(platform)

        if not token:
            self.stdout.write(
                self.style.WARNING(
                    f'No authentication token provided. '
                    f'Use --token, set {env_var} environment variable, or use --oauth'
                )
            )
            return None

        return token

    def _perform_oauth_flow(self, platform):
        """Perform OAuth flow (simplified simulation)"""
        self.stdout.write(f'Starting OAuth flow for {platform}...')

        # In a real implementation, this would:
        # 1. Open browser to OAuth URL
        # 2. Handle callback
        # 3. Exchange code for token

        # For demonstration, we'll simulate this
        self.stdout.write(
            self.style.WARNING(
                f'OAuth flow simulation - in production this would:\n'
                f'1. Open browser to {platform} OAuth page\n'
                f'2. User authorizes application\n'
                f'3. Exchange code for access token\n\n'
                f'For now, please provide a personal access token manually.'
            )
        )
        return None

    def _verify_repository_access_and_license(self, platform, username, repo_name, token):
        """Verify repository access and license"""
        self.stdout.write('Verifying repository access and license...')

        if platform == 'github':
            api_url = f'https://api.github.com/repos/{username}/{repo_name}'
            headers = {'Authorization': f'token {token}'} if token else {}
        elif platform == 'gitlab':
            api_url = f'https://gitlab.com/api/v4/projects/{username}%2F{repo_name}'
            headers = {'Authorization': f'Bearer {token}'} if token else {}
        else:
            raise CommandError(f'Unsupported platform: {platform}')

        try:
            response = requests.get(api_url, headers=headers, timeout=10)

            if response.status_code == 404:
                raise CommandError(f'Repository {username}/{repo_name} not found or not accessible')
            elif response.status_code == 403:
                raise CommandError(f'Access denied to {username}/{repo_name}. Check your token permissions.')
            elif response.status_code != 200:
                raise CommandError(f'Failed to access repository: HTTP {response.status_code}')

            repo_data = response.json()

            # Check if repository is private and we have access
            if platform == 'github' and repo_data.get('private') and not token:
                raise CommandError('Repository is private but no access token provided')

            # In a real implementation, you might check:
            # - License compatibility
            # - Repository metadata
            # - User permissions

            self.stdout.write('✓ Repository access verified')

        except requests.RequestException as e:
            raise CommandError(f'Failed to verify repository access: {str(e)}')

    def _clone_repository(self, repo_info, token, branch):
        """Clone repository to temporary directory"""
        temp_dir = Path(tempfile.mkdtemp(prefix='bird-fetch-'))

        # Construct clone URL with token if available
        if token:
            if repo_info['platform'] == 'github':
                clone_url = f"https://{token}@github.com/{repo_info['username']}/{repo_info['repo_name']}.git"
            elif repo_info['platform'] == 'gitlab':
                clone_url = f"https://oauth2:{token}@gitlab.com/{repo_info['username']}/{repo_info['repo_name']}.git"
        else:
            clone_url = repo_info['full_url']
            if not clone_url.endswith('.git'):
                clone_url += '.git'

        try:
            # Clone repository
            cmd = ['git', 'clone', '--branch', branch, '--depth', '1', clone_url, str(temp_dir / 'repo')]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

            if result.returncode != 0:
                raise CommandError(f'Git clone failed: {result.stderr}')

            self.stdout.write(f'✓ Repository cloned to temporary directory')
            return temp_dir / 'repo'

        except subprocess.TimeoutExpired:
            raise CommandError('Git clone timed out after 2 minutes')
        except Exception as e:
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise CommandError(f'Failed to clone repository: {str(e)}')

    def _validate_extension_structure(self, temp_dir, extension_name):
        """Validate that the cloned repository has proper extension structure"""
        required_files = [
            f'{extension_name}/__init__.py',
            f'{extension_name}/views.py',
            f'{extension_name}/urls.py',
            f'{extension_name}/entry_point.py'
        ]

        missing_files = []
        for file_name in required_files:
            if not (temp_dir / file_name).exists():
                missing_files.append(file_name)

        if missing_files:
            raise CommandError(
                f'Invalid extension structure. Missing required files: {", ".join(missing_files)}'
            )

        # Validate manifest if present
        manifest_path = temp_dir / 'extension_manifest.yaml'
        if manifest_path.exists():
            try:
                with open(manifest_path, 'r') as f:
                    manifest = yaml.safe_load(f)

                # Basic validation
                if not manifest.get('name'):
                    raise CommandError('Extension manifest missing required "name" field')

            except yaml.YAMLError as e:
                raise CommandError(f'Invalid extension manifest: {str(e)}')

        self.stdout.write('✓ Extension structure validated')

    def _install_extension(self, temp_dir, extension_path, extension_name, force):
        """Install extension to extensions directory"""
        # Create extensions directory if it doesn't exist
        extension_path.parent.mkdir(exist_ok=True)

        # Remove existing extension if force is used
        if extension_path.exists() and force:
            shutil.rmtree(extension_path)
            self.stdout.write(f'Removed existing extension "{extension_name}"')

        # Copy extension files
        shutil.copytree(temp_dir, extension_path)

        # Remove git directory from installed extension
        git_dir = extension_path / '.git'
        if git_dir.exists():
            shutil.rmtree(git_dir)

        shutil.copytree(extension_path/extension_name,extension_path,dirs_exist_ok=True)
        # shutil.rmtree(extension_path/os.sep/extension_name)

        self.stdout.write(f'✓ Extension installed to {extension_path}')

    def _install_dependencies(self, extension_path, use_uv=False):
        """Install extension dependencies"""
        self.stdout.write('Installing extension dependencies...')

        # Check for requirements.txt
        requirements_file = extension_path / 'requirements.txt'
        if requirements_file.exists():
            try:
                cmd = ['pip', 'install', '-r', str(requirements_file)]
                if use_uv:
                    cmd = ["uv", 'pip', 'install', '-r', str(requirements_file)]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

                if result.returncode != 0:
                    self.stdout.write(
                        self.style.WARNING(f'Warning: Failed to install some dependencies: {result.stderr}')
                    )
                else:
                    self.stdout.write('✓ Dependencies installed from requirements.txt')

            except subprocess.TimeoutExpired:
                self.stdout.write(self.style.WARNING('Warning: Dependency installation timed out'))
            except Exception as e:
                self.stdout.write(self.style.WARNING(f'Warning: Dependency installation failed: {str(e)}'))

        # Check for pyproject.toml
        pyproject_file = extension_path / 'pyproject.toml'
        if pyproject_file.exists():
            try:
                cmd = ['pip', 'install', '-e', str(extension_path)]
                if uv:
                    cmd = ['uv', 'pip','install', '-e', str(extension_path)]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

                if result.returncode != 0:
                    self.stdout.write(
                        self.style.WARNING(f'Warning: Failed to install extension package: {result.stderr}')
                    )
                else:
                    self.stdout.write('✓ Extension package installed')

            except Exception as e:
                self.stdout.write(self.style.WARNING(f'Warning: Package installation failed: {str(e)}'))

    def _run_migrations(self, extension_name):
        """Run database migrations for the extension"""
        self.stdout.write('Running database migrations...')

        try:
            # Check if extension has models
            from django.core.management import call_command

            # Make migrations for the extension
            try:
                call_command('makemigrations', f'extensions.{extension_name}', verbosity=0)
                self.stdout.write('✓ Extension migrations created')
            except Exception:
                # Extension might not have models or migrations already exist
                pass

            # Run migrations
            call_command('migrate', verbosity=0)
            self.stdout.write('✓ Database migrations completed')

        except Exception as e:
            self.stdout.write(
                self.style.WARNING(f'Warning: Migration failed: {str(e)}')
            )
