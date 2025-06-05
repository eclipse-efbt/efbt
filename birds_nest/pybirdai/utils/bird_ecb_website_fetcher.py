"""
Bird ECB Client Library

This library provides a simple interface to generate URLs for the European Central Bank's
Bird (Banks' Integrated Reporting Dictionary) API.
"""

from urllib.parse import urlencode
import requests
import zipfile
import os
import io

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
    def __init__(self):
        """Initialize the Bird ECB Website client."""
        self.client = BirdEcbClient()

    def request_and_save_all(self, output_dir="resources/technical_export/"):
        link = "https://bird.ecb.europa.eu/excel?entities=all&onlyCurrentlyValidMetadata=false&format=csv"
        path_to_results = output_dir
        RESPONSE_ZIP = "response.zip"
        os.makedirs(path_to_results, exist_ok=True)
        response = requests.get(link)

        # Save response to temporary ZIP file
        with open(RESPONSE_ZIP, "wb") as f:
            f.write(response.content)

        # Extract contents and clean up
        with zipfile.ZipFile(RESPONSE_ZIP, 'r') as zip_ref:
            for file in zip_ref.infolist():
                zip_ref.extract(file, path_to_results)

        os.remove(RESPONSE_ZIP)
        return path_to_results

    def request_and_save(self, tree_root_ids, tree_root_type="FRAMEWORK", output_dir="results/csv",
                         format_type="csv", include_mapping_content=False,
                         include_rendering_content=False, include_transformation_content=False,
                         only_currently_valid_metadata=False):
        """Request data from the Bird ECB API and save it to a file.

        Args:
            tree_root_ids (str or list): The tree root ID(s) to query (e.g., 'ANCRDT').
            tree_root_type (str): The type of tree root (e.g., 'FRAMEWORK', 'CUBE').
            output_dir (str): Base directory for saving the results.
            format_type (str): The desired output format (e.g., 'csv', 'json', 'xml').
            include_mapping_content (bool): Whether to include mapping content.
            include_rendering_content (bool): Whether to include rendering content.
            include_transformation_content (bool): Whether to include transformation content.
            only_currently_valid_metadata (bool): Whether to return only currently valid metadata.

        Returns:
            str: Path to the directory where the files were extracted.
        """
        # Create a directory name from the tree_root_ids
        if isinstance(tree_root_ids, list):
            dir_name = "_".join(tree_root_ids).lower()
        else:
            dir_name = tree_root_ids.lower()

        path_to_results = output_dir or f"{output_dir}/{dir_name}"
        RESPONSE_ZIP = "response.zip"

        if not os.path.exists(path_to_results):
            os.makedirs(path_to_results, exist_ok=True)

        # Configure all client parameters
        self.client.set_tree_root_ids(tree_root_ids)
        self.client.set_tree_root_type(tree_root_type)
        self.client.set_format(format_type)
        self.client.include_mapping_content(include_mapping_content)
        self.client.include_rendering_content(include_rendering_content)
        self.client.include_transformation_content(include_transformation_content)
        self.client.only_currently_valid_metadata(only_currently_valid_metadata)

        # Build URL and make request
        link = self.client.build_url()
        response = requests.get(link)

        # Save response to temporary ZIP file
        with open(RESPONSE_ZIP, "wb") as f:
            f.write(response.content)

        # Extract contents and clean up
        with zipfile.ZipFile(RESPONSE_ZIP, 'r') as zip_ref:
            for file in zip_ref.infolist():
                zip_ref.extract(file, path_to_results)

        os.remove(RESPONSE_ZIP)
        return path_to_results

def main():
    client = BirdEcbWebsiteClient()
    output_dir = client.request_and_save(
        tree_root_ids="ANCRDT",
        tree_root_type="FRAMEWORK",
        output_dir="resources/technical_export",
        format_type="csv",
        include_mapping_content=False,
        include_rendering_content=False,
        include_transformation_content=False,
        only_currently_valid_metadata=False
    )
    print(f"Results saved to: {output_dir}")

if __name__ == "__main__":
    main()
