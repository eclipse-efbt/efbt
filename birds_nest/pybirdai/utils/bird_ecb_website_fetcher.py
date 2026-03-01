# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
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
    METADATA_EXPORT_URL = "https://bird.ecb.europa.eu/excel/export/metadata"

    def __init__(self):
        """Initialize the Bird ECB Website client."""
        self.client = BirdEcbClient()

    def request_and_save_all(self, output_dir="artefacts/smcubes_artefacts/"):
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


    def request_logical_transformation_rules(self, output_dir="artefacts/smcubes_artefacts"):
        """Request logical transformation rules from ECB API.

        This uses the POST /excel/export/metadata endpoint to download
        logical transformation rules (sddlogicaltransformationrule).

        Args:
            output_dir (str): Directory to save the extracted CSV files.

        Returns:
            str: Path to the CSV file containing logical transformation rules.
        """
        RESPONSE_ZIP = "response_transformation_rules.zip"

        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        # POST request payload for logical transformation rules
        payload = {
            "entities": ["sddlogicaltransformationrule"],
            "format": "csv",
            "tids": None,
            "validOn": None
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "x-sdd-app": "true",
            "x-sdd-correlation-description": "Export Data"
        }

        response = requests.post(
            self.METADATA_EXPORT_URL,
            json=payload,
            headers=headers
        )

        if response.status_code != 200:
            raise Exception(f"Failed to fetch logical transformation rules: HTTP {response.status_code}")

        # Save response to temporary ZIP file
        with open(RESPONSE_ZIP, "wb") as f:
            f.write(response.content)

        # Extract contents and clean up
        with zipfile.ZipFile(RESPONSE_ZIP, 'r') as zip_ref:
            for file in zip_ref.infolist():
                zip_ref.extract(file, output_dir)

        os.remove(RESPONSE_ZIP)

        # Return path to the expected CSV file
        # Note: ECB API returns 'logical_transformation_rule.csv' (not sddlogicaltransformationrule.csv)
        csv_path = os.path.join(output_dir, "logical_transformation_rule.csv")
        return csv_path

    def request_member_link(self, output_dir="artefacts/smcubes_artefacts"):
        """Request member_link (cube_structure_item_link) data from ECB API.

        This uses the POST /excel/export/metadata endpoint to download
        member link data needed for ANCRDT derivation rules.

        Args:
            output_dir (str): Directory to save the extracted CSV files.

        Returns:
            str: Path to the CSV file containing member link data.
        """
        RESPONSE_ZIP = "response_member_link.zip"

        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        # POST request payload for member link data
        payload = {
            "entities": ["sddcubestructureitemlink"],
            "format": "csv",
            "tids": None,
            "validOn": None
        }

        headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "x-sdd-app": "true",
            "x-sdd-correlation-description": "Export Data"
        }

        response = requests.post(
            self.METADATA_EXPORT_URL,
            json=payload,
            headers=headers
        )

        if response.status_code != 200:
            raise Exception(f"Failed to fetch member link data: HTTP {response.status_code}")

        # Save response to temporary ZIP file
        with open(RESPONSE_ZIP, "wb") as f:
            f.write(response.content)

        # Extract contents and clean up
        with zipfile.ZipFile(RESPONSE_ZIP, 'r') as zip_ref:
            for file in zip_ref.infolist():
                zip_ref.extract(file, output_dir)

        os.remove(RESPONSE_ZIP)

        # Return path to the expected CSV file
        # Note: ECB API returns 'cube_structure_item_link.csv'
        csv_path = os.path.join(output_dir, "cube_structure_item_link.csv")
        return csv_path

    def request_ancrdt_member_link(self, output_dir="resources/derivation_files"):
        """Request ANCRDT member link data from ECB API for derivation generation.

        This uses the GET /excel/tree endpoint with BIRD and ANCRDT frameworks
        to download member_link.csv needed for ANCRDT derivation rules.

        Args:
            output_dir (str): Directory to save the CSV file.

        Returns:
            str: Path to the CSV file containing member link data for derivations.
        """
        RESPONSE_ZIP = "response_ancrdt_member_link.zip"

        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        # Build the GET URL for BIRD and ANCRDT frameworks
        # Note: treeRootIds needs to appear twice for multiple values
        url = (
            "https://bird.ecb.europa.eu/excel/tree?"
            "treeRootIds=BIRD&treeRootIds=ANCRDT&"
            "treeRootType=FRAMEWORK&"
            "includeMappingContent=false&"
            "includeRenderingContent=false&"
            "includeTransformationContent=false&"
            "onlyCurrentlyValidMetadata=false&"
            "format=csv"
        )

        headers = {
            "Accept": "*/*",
            "Referer": "https://bird.ecb.europa.eu/cm",
            "x-sdd-app": "true",
            "x-sdd-correlation-description": "Export Data"
        }

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch ANCRDT member link data: HTTP {response.status_code}")

        # Save response to temporary ZIP file
        with open(RESPONSE_ZIP, "wb") as f:
            f.write(response.content)

        # Extract only member_link.csv from the ZIP
        output_csv_path = os.path.join(output_dir, "member_link_for_derivation.csv")
        with zipfile.ZipFile(RESPONSE_ZIP, 'r') as zip_ref:
            # Find member_link.csv in the ZIP
            member_link_file = None
            for filename in zip_ref.namelist():
                if filename.endswith('member_link.csv') or filename == 'member_link.csv':
                    member_link_file = filename
                    break

            if member_link_file:
                # Extract only member_link.csv and rename it
                with zip_ref.open(member_link_file) as source:
                    with open(output_csv_path, 'wb') as target:
                        target.write(source.read())
            else:
                raise Exception("member_link.csv not found in the ZIP response")

        os.remove(RESPONSE_ZIP)

        return output_csv_path


def main():
    client = BirdEcbWebsiteClient()
    output_dir = client.request_and_save(
        tree_root_ids="ANCRDT",
        tree_root_type="FRAMEWORK",
        output_dir="artefacts/smcubes_artefacts",
        format_type="csv",
        include_mapping_content=False,
        include_rendering_content=False,
        include_transformation_content=False,
        only_currently_valid_metadata=False
    )
    print(f"Results saved to: {output_dir}")

if __name__ == "__main__":
    main()
