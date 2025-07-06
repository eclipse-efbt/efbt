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

"""
Bird ECB Client Library - Process Step Implementation

This process step provides a structured interface to generate URLs for the European Central Bank's
Bird (Banks' Integrated Reporting Dictionary) API.
"""

from urllib.parse import urlencode
import requests
import zipfile
import os
import io
import logging
from typing import Dict, Any, Optional, List, Union

logger = logging.getLogger(__name__)


class BirdEcbWebsiteFetcherProcessStep:
    """
    Process step for fetching data from the European Central Bank's Bird API.
    Refactored from utils.bird_ecb_website_fetcher to follow process step patterns.
    """
    
    def __init__(self, context=None):
        """
        Initialize the Bird ECB website fetcher process step.
        
        Args:
            context: The context object containing configuration settings.
        """
        self.context = context
        
    def execute(self, operation: str = "fetch_all", output_dir: str = "resources/technical_export/", 
                tree_root_ids: Union[str, List[str]] = None, tree_root_type: str = "FRAMEWORK", 
                **kwargs) -> Dict[str, Any]:
        """
        Execute the Bird ECB data fetching process.
        
        Args:
            operation (str): Operation type - "fetch_all" or "fetch_specific"
            output_dir (str): Directory to save the fetched data
            tree_root_ids (str or list): Tree root IDs for specific fetching
            tree_root_type (str): Type of tree root (e.g., 'FRAMEWORK', 'CUBE')
            **kwargs: Additional parameters for the fetching process
            
        Returns:
            dict: Result dictionary with success status and details
        """
        try:
            client = BirdEcbWebsiteClient()
            
            if operation == "fetch_all":
                result_path = client.request_and_save_all(output_dir)
                result = {
                    'success': True,
                    'operation': 'fetch_all',
                    'output_path': result_path,
                    'message': f'All Bird ECB data fetched to {result_path}'
                }
            elif operation == "fetch_specific":
                if not tree_root_ids:
                    raise ValueError("tree_root_ids is required for fetch_specific operation")
                
                result_path = client.request_and_save(
                    tree_root_ids=tree_root_ids,
                    tree_root_type=tree_root_type,
                    output_dir=output_dir,
                    **kwargs
                )
                result = {
                    'success': True,
                    'operation': 'fetch_specific',
                    'output_path': result_path,
                    'tree_root_ids': tree_root_ids,
                    'tree_root_type': tree_root_type,
                    'message': f'Specific Bird ECB data fetched to {result_path}'
                }
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            if self.context:
                self.context.bird_ecb_client = client
                
            return result
            
        except Exception as e:
            logger.error(f"Failed to fetch Bird ECB data: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Bird ECB data fetching failed'
            }


class BirdEcbClient:
    """Client for the European Central Bank's Bird API."""

    BASE_URL = "https://bird.ecb.europa.eu/excel/tree"

    def __init__(self):
        """Initialize the Bird ECB client."""
        self.params = {
            "includeMappingContent": False,
            "includeRenderingContent": False,
            "includeTransformationContent": False,
            "onlyCurrentlyValidMetadata": False,
            "format": "csv"
        }
        self.tree_root_ids = None
        self.tree_root_type = None

    def set_tree_root_ids(self, ids):
        """Set the tree root IDs parameter.

        Args:
            ids (str or list): The tree root ID(s) to query.
                If a list is provided, it will be comma-separated.
        """
        if isinstance(ids, list):
            self.tree_root_ids = ",".join(ids)
        else:
            self.tree_root_ids = ids
        return self

    def set_tree_root_type(self, type_name):
        """Set the tree root type parameter.

        Args:
            type_name (str): The type of tree root (e.g., 'FRAMEWORK', 'CUBE').
        """
        self.tree_root_type = type_name
        return self

    def set_format(self, format_type):
        """Set the output format.

        Args:
            format_type (str): The desired output format (e.g., 'csv', 'json', 'xml').
        """
        self.params["format"] = format_type
        return self

    def include_mapping_content(self, include=True):
        """Set whether to include mapping content.

        Args:
            include (bool): Whether to include mapping content.
        """
        self.params["includeMappingContent"] = include
        return self

    def include_rendering_content(self, include=True):
        """Set whether to include rendering content.

        Args:
            include (bool): Whether to include rendering content.
        """
        self.params["includeRenderingContent"] = include
        return self

    def include_transformation_content(self, include=True):
        """Set whether to include transformation content.

        Args:
            include (bool): Whether to include transformation content.
        """
        self.params["includeTransformationContent"] = include
        return self

    def only_currently_valid_metadata(self, only_valid=True):
        """Set whether to return only currently valid metadata.

        Args:
            only_valid (bool): Whether to return only currently valid metadata.
        """
        self.params["onlyCurrentlyValidMetadata"] = only_valid
        return self

    def build_url(self):
        """Build and return the Bird ECB API URL.

        Returns:
            str: The complete URL for the Bird ECB API request.

        Raises:
            ValueError: If required parameters are missing.
        """
        if not self.tree_root_ids:
            raise ValueError("Tree root IDs must be specified using set_tree_root_ids()")

        if not self.tree_root_type:
            raise ValueError("Tree root type must be specified using set_tree_root_type()")

        all_params = {
            "treeRootIds": self.tree_root_ids,
            "treeRootType": self.tree_root_type,
            **self.params
        }

        query_string = urlencode(all_params)
        return f"{self.BASE_URL}?{query_string}"


class BirdEcbWebsiteClient:
    """Enhanced Bird ECB Website client with process step integration."""
    
    def __init__(self):
        """Initialize the Bird ECB Website client."""
        self.client = BirdEcbClient()

    def request_and_save_all(self, output_dir="resources/technical_export/"):
        """
        Request all Bird ECB data and save to the specified directory.
        
        Args:
            output_dir (str): Directory to save the downloaded files
            
        Returns:
            str: Path to the results directory
        """
        link = "https://bird.ecb.europa.eu/excel?entities=all&onlyCurrentlyValidMetadata=false&format=csv"
        path_to_results = output_dir
        RESPONSE_ZIP = "response.zip"
        
        logger.info(f"Fetching all Bird ECB data to {output_dir}")
        
        os.makedirs(path_to_results, exist_ok=True)
        response = requests.get(link)
        response.raise_for_status()

        # Save response to temporary ZIP file
        with open(RESPONSE_ZIP, "wb") as f:
            f.write(response.content)

        # Extract contents and clean up
        with zipfile.ZipFile(RESPONSE_ZIP, 'r') as zip_ref:
            for file in zip_ref.infolist():
                zip_ref.extract(file, path_to_results)

        os.remove(RESPONSE_ZIP)
        logger.info(f"Bird ECB data successfully saved to {path_to_results}")
        return path_to_results

    def request_and_save(self, tree_root_ids, tree_root_type="FRAMEWORK", output_dir="results/csv",
                         format_type="csv", include_mapping_content=False,
                         include_rendering_content=False, include_transformation_content=False,
                         only_currently_valid_metadata=False):
        """
        Request specific data from the Bird ECB API and save it to a file.

        Args:
            tree_root_ids (str or list): The tree root ID(s) to query
            tree_root_type (str): The type of tree root (default: "FRAMEWORK")
            output_dir (str): Directory to save the output files (default: "results/csv")
            format_type (str): Output format (default: "csv")
            include_mapping_content (bool): Include mapping content (default: False)
            include_rendering_content (bool): Include rendering content (default: False)
            include_transformation_content (bool): Include transformation content (default: False)
            only_currently_valid_metadata (bool): Only currently valid metadata (default: False)

        Returns:
            str: Path to the saved file or directory
        """
        logger.info(f"Fetching specific Bird ECB data for {tree_root_ids}")
        
        # Configure the client
        self.client.set_tree_root_ids(tree_root_ids) \
                   .set_tree_root_type(tree_root_type) \
                   .set_format(format_type) \
                   .include_mapping_content(include_mapping_content) \
                   .include_rendering_content(include_rendering_content) \
                   .include_transformation_content(include_transformation_content) \
                   .only_currently_valid_metadata(only_currently_valid_metadata)

        # Build the URL and make the request
        url = self.client.build_url()
        logger.info(f"Requesting data from: {url}")
        
        response = requests.get(url)
        response.raise_for_status()

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Determine the file extension based on format
        if format_type.lower() == "csv":
            extension = "csv"
        elif format_type.lower() == "json":
            extension = "json"
        elif format_type.lower() == "xml":
            extension = "xml"
        else:
            extension = "txt"

        # Create filename
        if isinstance(tree_root_ids, list):
            filename = f"bird_data_{'_'.join(tree_root_ids)}.{extension}"
        else:
            filename = f"bird_data_{tree_root_ids}.{extension}"

        file_path = os.path.join(output_dir, filename)

        # Save the response
        if response.headers.get('content-type', '').startswith('application/zip'):
            # Handle ZIP response
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                zip_ref.extractall(output_dir)
            logger.info(f"ZIP data extracted to {output_dir}")
            return output_dir
        else:
            # Handle direct file response
            with open(file_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"Data saved to {file_path}")
            return file_path