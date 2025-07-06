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
from typing import Union, List

logger = logging.getLogger(__name__)


class RunBirdEcbDataFetcher(AppConfig):
    """
    Django AppConfig for running Bird ECB data fetching services.
    
    This entry point provides access to European Central Bank's Bird API
    data fetching functionality through the process step architecture.
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    def ready(self):
        """
        Prepare and execute Bird ECB data fetching services.
        
        This method sets up the necessary contexts and provides access to
        Bird ECB API data fetching services.
        """
        from pybirdai.process_steps.utils_integration.external_data_fetching.bird_ecb_website_fetcher import (
            BirdEcbWebsiteFetcherProcessStep
        )
        from pybirdai.context.context import Context
        
        logger.info("Initializing Bird ECB Data Fetcher entry point")
        
        try:
            # Create context for Bird ECB services
            context = Context()
            
            # Initialize Bird ECB fetcher process step
            bird_ecb_step = BirdEcbWebsiteFetcherProcessStep(context)
            context.bird_ecb_step = bird_ecb_step
            
            # Store context globally for access by other components
            if not hasattr(settings, 'BIRD_ECB_CONTEXT'):
                settings.BIRD_ECB_CONTEXT = context
            
            logger.info("Bird ECB Data Fetcher entry point initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Bird ECB Data Fetcher: {e}")
            raise


def fetch_all_bird_data(output_dir: str = "resources/technical_export/"):
    """
    Entry point function for fetching all Bird ECB data.
    
    Args:
        output_dir (str): Directory to save the fetched data
        
    Returns:
        dict: Result dictionary with success status and details
    """
    try:
        from pybirdai.process_steps.utils_integration.external_data_fetching.bird_ecb_website_fetcher import (
            BirdEcbWebsiteFetcherProcessStep
        )
        
        step = BirdEcbWebsiteFetcherProcessStep()
        result = step.execute(
            operation="fetch_all",
            output_dir=output_dir
        )
        
        logger.info(f"Bird ECB data fetching completed: {result.get('message', 'Success')}")
        return result
        
    except Exception as e:
        logger.error(f"Bird ECB data fetching failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Bird ECB data fetching failed'
        }


def fetch_specific_bird_data(tree_root_ids: Union[str, List[str]], 
                           tree_root_type: str = "FRAMEWORK",
                           output_dir: str = "results/csv", **kwargs):
    """
    Entry point function for fetching specific Bird ECB data.
    
    Args:
        tree_root_ids (str or list): Tree root IDs for specific fetching
        tree_root_type (str): Type of tree root (e.g., 'FRAMEWORK', 'CUBE')
        output_dir (str): Directory to save the fetched data
        **kwargs: Additional parameters for fetching
        
    Returns:
        dict: Result dictionary with success status and details
    """
    try:
        from pybirdai.process_steps.utils_integration.external_data_fetching.bird_ecb_website_fetcher import (
            BirdEcbWebsiteFetcherProcessStep
        )
        
        step = BirdEcbWebsiteFetcherProcessStep()
        result = step.execute(
            operation="fetch_specific",
            tree_root_ids=tree_root_ids,
            tree_root_type=tree_root_type,
            output_dir=output_dir,
            **kwargs
        )
        
        logger.info(f"Specific Bird ECB data fetching completed: {result.get('message', 'Success')}")
        return result
        
    except Exception as e:
        logger.error(f"Specific Bird ECB data fetching failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Specific Bird ECB data fetching failed'
        }


# Convenience functions for backward compatibility
def get_bird_ecb_client():
    """
    Get a Bird ECB client instance.
    
    Returns:
        BirdEcbClient: Configured client instance
    """
    from pybirdai.process_steps.utils_integration.external_data_fetching.bird_ecb_website_fetcher import (
        BirdEcbClient
    )
    
    return BirdEcbClient()


def get_bird_ecb_website_client():
    """
    Get a Bird ECB website client instance.
    
    Returns:
        BirdEcbWebsiteClient: Configured website client instance
    """
    from pybirdai.process_steps.utils_integration.external_data_fetching.bird_ecb_website_fetcher import (
        BirdEcbWebsiteClient
    )
    
    return BirdEcbWebsiteClient()


def build_bird_ecb_url(tree_root_ids: Union[str, List[str]], 
                      tree_root_type: str = "FRAMEWORK", **params):
    """
    Build a Bird ECB API URL with specified parameters.
    
    Args:
        tree_root_ids (str or list): Tree root IDs
        tree_root_type (str): Type of tree root
        **params: Additional URL parameters
        
    Returns:
        str: Complete Bird ECB API URL
    """
    client = get_bird_ecb_client()
    
    # Set required parameters
    client.set_tree_root_ids(tree_root_ids)
    client.set_tree_root_type(tree_root_type)
    
    # Set optional parameters
    if 'format' in params:
        client.set_format(params['format'])
    if 'include_mapping_content' in params:
        client.include_mapping_content(params['include_mapping_content'])
    if 'include_rendering_content' in params:
        client.include_rendering_content(params['include_rendering_content'])
    if 'include_transformation_content' in params:
        client.include_transformation_content(params['include_transformation_content'])
    if 'only_currently_valid_metadata' in params:
        client.only_currently_valid_metadata(params['only_currently_valid_metadata'])
    
    return client.build_url()