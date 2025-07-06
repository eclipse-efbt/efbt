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

import django
import os
from django.apps import AppConfig
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class RunGitHubIntegrationService(AppConfig):
    """
    Django AppConfig for running GitHub integration services.
    
    This entry point provides access to GitHub file fetching and repository
    cloning functionality through the process step architecture.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    def ready(self):
        """
        Prepare and execute GitHub integration services.
        
        This method sets up the necessary contexts and provides access to
        GitHub file fetching and repository cloning services.
        """
        from pybirdai.process_steps.utils_integration.github_integration.github_file_fetcher import (
            GitHubFileFetcherProcessStep
        )
        from pybirdai.process_steps.utils_integration.github_integration.clone_repo_service import (
            CloneRepoServiceProcessStep
        )
        from pybirdai.context.context import Context
        
        logger.info("Initializing GitHub Integration Service entry point")
        
        try:
            # Create context for GitHub integration services
            context = Context()
            
            # Initialize GitHub file fetcher process step
            github_fetcher_step = GitHubFileFetcherProcessStep(context)
            context.github_fetcher_step = github_fetcher_step
            
            # Initialize clone repo service process step
            clone_repo_step = CloneRepoServiceProcessStep(context)
            context.clone_repo_step = clone_repo_step
            
            # Store context globally for access by other components
            if not hasattr(settings, 'GITHUB_INTEGRATION_CONTEXT'):
                settings.GITHUB_INTEGRATION_CONTEXT = context
            
            logger.info("GitHub Integration Service entry point initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize GitHub Integration Service: {e}")
            raise


def fetch_files_from_github(repository_url: str, target_directory: str = None, **kwargs):
    """
    Entry point function for fetching files from GitHub repository.
    
    Args:
        repository_url (str): GitHub repository URL
        target_directory (str): Local directory to save files
        **kwargs: Additional parameters for fetching
        
    Returns:
        dict: Result dictionary with success status and details
    """
    try:
        from pybirdai.process_steps.utils_integration.github_integration.github_file_fetcher import (
            GitHubFileFetcherProcessStep
        )
        
        step = GitHubFileFetcherProcessStep()
        result = step.execute(
            base_url=repository_url,
            target_directory=target_directory,
            **kwargs
        )
        
        logger.info(f"GitHub file fetching completed: {result.get('message', 'Success')}")
        return result
        
    except Exception as e:
        logger.error(f"GitHub file fetching failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'GitHub file fetching failed'
        }


def clone_repository(repository_url: str, token: str = None, **kwargs):
    """
    Entry point function for cloning GitHub repository.
    
    Args:
        repository_url (str): GitHub repository URL to clone
        token (str): Optional GitHub token for authentication
        **kwargs: Additional parameters
        
    Returns:
        dict: Result dictionary with success status and details
    """
    try:
        from pybirdai.process_steps.utils_integration.github_integration.clone_repo_service import (
            CloneRepoServiceProcessStep
        )
        
        step = CloneRepoServiceProcessStep()
        result = step.execute(
            repository_url=repository_url,
            token=token,
            **kwargs
        )
        
        logger.info(f"Repository cloning completed: {result.get('message', 'Success')}")
        return result
        
    except Exception as e:
        logger.error(f"Repository cloning failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Repository cloning failed'
        }


# Convenience functions for backward compatibility
def get_github_file_fetcher(repository_url: str):
    """
    Get a GitHub file fetcher instance.
    
    Args:
        repository_url (str): GitHub repository URL
        
    Returns:
        GitHubFileFetcher: Configured fetcher instance
    """
    from pybirdai.process_steps.utils_integration.github_integration.github_file_fetcher import (
        GitHubFileFetcher
    )
    
    return GitHubFileFetcher(repository_url)


def get_clone_repo_service(token: str = None):
    """
    Get a clone repository service instance.
    
    Args:
        token (str): Optional GitHub token
        
    Returns:
        CloneRepoService: Configured service instance
    """
    from pybirdai.process_steps.utils_integration.github_integration.clone_repo_service import (
        CloneRepoService
    )
    
    return CloneRepoService(token)