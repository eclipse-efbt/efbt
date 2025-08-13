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

import subprocess
import json
import requests
from pathlib import Path
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

class GitManager:
    """
    Handles Git operations for extension packaging, including creating repositories
    on GitHub and GitLab and pushing code.
    """
    
    def __init__(self, platform: str = 'github', token: Optional[str] = None):
        """
        Initialize GitManager.
        
        Args:
            platform: 'github' or 'gitlab'
            token: Personal access token for the platform
        """
        self.platform = platform.lower()
        self.token = token
        
        if self.platform == 'github':
            self.api_base = 'https://api.github.com'
        elif self.platform == 'gitlab':
            self.api_base = 'https://gitlab.com/api/v4'
        else:
            raise ValueError(f"Unsupported platform: {platform}")
    
    def init_repository(self, repo_path: Path):
        """Initialize a Git repository in the given path."""
        try:
            # Initialize repository
            self._run_git_command(['init'], cwd=repo_path)
            
            # Set default branch to main
            self._run_git_command(['branch', '-M', 'main'], cwd=repo_path)
            
            logger.info(f"Initialized Git repository in {repo_path}")
            
        except subprocess.CalledProcessError as e:
            raise GitError(f"Failed to initialize Git repository: {e}")
    
    def create_gitignore(self, repo_path: Path):
        """Create a .gitignore file appropriate for Python/Django projects."""
        gitignore_content = '''# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# C extensions
*.so

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
pip-wheel-metadata/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# PyInstaller
#  Usually these files are written by a python script from a template
#  before PyInstaller builds the exe, so as to inject date/other infos into it.
*.manifest
*.spec

# Installer logs
pip-log.txt
pip-delete-this-directory.txt

# Unit test / coverage reports
htmlcov/
.tox/
.nox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
*.py,cover
.hypothesis/
.pytest_cache/

# Translations
*.mo
*.pot

# Django stuff:
*.log
local_settings.py
db.sqlite3
db.sqlite3-journal

# Flask stuff:
instance/
.webassets-cache

# Scrapy stuff:
.scrapy

# Sphinx documentation
docs/_build/

# PyBuilder
target/

# Jupyter Notebook
.ipynb_checkpoints

# IPython
profile_default/
ipython_config.py

# pyenv
.python-version

# pipenv
#   According to pypa/pipenv#598, it is recommended to include Pipfile.lock in version control.
#   However, in case of collaboration, if having platform-specific dependencies or dependencies
#   having no cross-platform support, pipenv may install dependencies that don't work, or not
#   install all needed dependencies.
#Pipfile.lock

# PEP 582; used by e.g. github.com/David-OConnor/pyflow
__pypackages__/

# Celery stuff
celerybeat-schedule
celerybeat.pid

# SageMath parsed files
*.sage.py

# Environments
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# Spyder project settings
.spyderproject
.spyproject

# Rope project settings
.ropeproject

# mkdocs documentation
/site

# mypy
.mypy_cache/
.dmypy.json
dmypy.json

# Pyre type checker
.pyre/

# PyCharm
.idea/

# VS Code
.vscode/

# macOS
.DS_Store

# Windows
Thumbs.db
ehthumbs.db
Desktop.ini

# Extension specific
tmp/
*.tmp
resources/*/tmp/
results/*/tmp/
'''
        
        gitignore_path = repo_path / '.gitignore'
        with open(gitignore_path, 'w', encoding='utf-8') as f:
            f.write(gitignore_content)
        
        logger.info(f"Created .gitignore at {gitignore_path}")
    
    def add_and_commit(self, repo_path: Path, commit_message: str):
        """Add all files and create initial commit."""
        try:
            # Add all files
            self._run_git_command(['add', '.'], cwd=repo_path)
            
            # Commit
            self._run_git_command(['commit', '-m', commit_message], cwd=repo_path)
            
            logger.info(f"Created commit: {commit_message}")
            
        except subprocess.CalledProcessError as e:
            raise GitError(f"Failed to commit files: {e}")
    
    def create_remote_repository(
        self,
        username: str,
        repo_name: str,
        description: str = '',
        private: bool = False
    ) -> str:
        """
        Create a remote repository on GitHub or GitLab.
        
        Args:
            username: Username or organization name
            repo_name: Name of the repository
            description: Repository description
            private: Whether the repository should be private
            
        Returns:
            str: The clone URL of the created repository
        """
        if not self.token:
            raise GitError("Token is required to create remote repository")
        
        if self.platform == 'github':
            return self._create_github_repo(username, repo_name, description, private)
        elif self.platform == 'gitlab':
            return self._create_gitlab_repo(username, repo_name, description, private)
        else:
            raise GitError(f"Unsupported platform: {self.platform}")
    
    def _create_github_repo(
        self,
        username: str,
        repo_name: str,
        description: str,
        private: bool
    ) -> str:
        """Create a GitHub repository."""
        headers = {
            'Authorization': f'token {self.token}',
            'Accept': 'application/vnd.github.v3+json',
            'Content-Type': 'application/json'
        }
        
        data = {
            'name': repo_name,
            'description': description,
            'private': private,
            'auto_init': False,
            'has_issues': True,
            'has_projects': True,
            'has_wiki': True
        }
        
        # Try to create in user account first, then in organization
        try:
            url = f"{self.api_base}/user/repos"
            response = requests.post(url, headers=headers, json=data)
            
            if response.status_code == 201:
                repo_info = response.json()
                logger.info(f"Created GitHub repository: {repo_info['clone_url']}")
                return repo_info['clone_url']
            elif response.status_code == 422:
                # Repository already exists, return existing URL
                existing_url = f"https://github.com/{username}/{repo_name}.git"
                logger.warning(f"Repository already exists: {existing_url}")
                return existing_url
            else:
                # Try creating in organization
                org_url = f"{self.api_base}/orgs/{username}/repos"
                org_response = requests.post(org_url, headers=headers, json=data)
                
                if org_response.status_code == 201:
                    repo_info = org_response.json()
                    logger.info(f"Created GitHub repository in org: {repo_info['clone_url']}")
                    return repo_info['clone_url']
                else:
                    raise GitError(f"Failed to create GitHub repository: {org_response.text}")
                    
        except requests.RequestException as e:
            raise GitError(f"Failed to create GitHub repository: {e}")
    
    def _create_gitlab_repo(
        self,
        username: str,
        repo_name: str,
        description: str,
        private: bool
    ) -> str:
        """Create a GitLab repository."""
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        
        data = {
            'name': repo_name,
            'description': description,
            'visibility': 'private' if private else 'public',
            'initialize_with_readme': False,
            'issues_enabled': True,
            'merge_requests_enabled': True,
            'wiki_enabled': True
        }
        
        try:
            url = f"{self.api_base}/projects"
            response = requests.post(url, headers=headers, json=data)
            
            if response.status_code == 201:
                repo_info = response.json()
                logger.info(f"Created GitLab repository: {repo_info['http_url_to_repo']}")
                return repo_info['http_url_to_repo']
            elif response.status_code == 400:
                # Repository might already exist
                existing_url = f"https://gitlab.com/{username}/{repo_name}.git"
                logger.warning(f"Repository might already exist: {existing_url}")
                return existing_url
            else:
                raise GitError(f"Failed to create GitLab repository: {response.text}")
                
        except requests.RequestException as e:
            raise GitError(f"Failed to create GitLab repository: {e}")
    
    def push_to_remote(self, repo_path: Path, remote_url: str, branch: str = 'main'):
        """Push local repository to remote."""
        # Create authenticated URL for pushing
        auth_url = self._create_authenticated_url(remote_url)
        
        try:
            # Add remote origin with authentication
            self._run_git_command(['remote', 'add', 'origin', auth_url], cwd=repo_path)
            
            # Push to remote
            self._run_git_command(['push', '-u', 'origin', branch], cwd=repo_path)
            
            logger.info(f"Pushed to remote: {remote_url}")
            
        except subprocess.CalledProcessError as e:
            # If remote already exists, try to set URL and push
            try:
                self._run_git_command(['remote', 'set-url', 'origin', auth_url], cwd=repo_path)
                self._run_git_command(['push', '-u', 'origin', branch], cwd=repo_path)
                logger.info(f"Updated remote and pushed to: {remote_url}")
            except subprocess.CalledProcessError as e2:
                raise GitError(f"Failed to push to remote: {e2}")
    
    def _create_authenticated_url(self, remote_url: str) -> str:
        """Create an authenticated URL for Git operations."""
        if not self.token:
            return remote_url
        
        # Convert HTTPS URL to include token authentication
        if remote_url.startswith('https://github.com/'):
            # For GitHub, use token as username
            return remote_url.replace('https://github.com/', f'https://{self.token}@github.com/')
        elif remote_url.startswith('https://gitlab.com/'):
            # For GitLab, use oauth2 as username and token as password
            return remote_url.replace('https://gitlab.com/', f'https://oauth2:{self.token}@gitlab.com/')
        
        return remote_url
    
    def _run_git_command(self, command: list, cwd: Path) -> str:
        """Run a git command and return output."""
        full_command = ['git'] + command
        
        try:
            result = subprocess.run(
                full_command,
                cwd=cwd,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Git command failed: {' '.join(full_command)}")
            logger.error(f"Error output: {e.stderr}")
            raise
    
    def check_git_installed(self) -> bool:
        """Check if Git is installed and available."""
        try:
            subprocess.run(['git', '--version'], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    
    def check_auth(self) -> bool:
        """Check if the token is valid for the platform."""
        if not self.token:
            return False
        
        headers = {
            'Authorization': f'token {self.token}' if self.platform == 'github' else f'Bearer {self.token}',
            'Accept': 'application/vnd.github.v3+json' if self.platform == 'github' else 'application/json'
        }
        
        try:
            url = f"{self.api_base}/user"
            response = requests.get(url, headers=headers)
            return response.status_code == 200
            
        except requests.RequestException:
            return False

class GitError(Exception):
    """Exception raised for Git operations."""
    pass