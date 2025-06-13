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

import os
import django
from django.db import models
from django.conf import settings
import sys
import numpy as np
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("visualization_service.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class DjangoSetup:
    @staticmethod
    def configure_django():
        """Configure Django settings without starting the application"""
        if not settings.configured:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
            sys.path.insert(0, project_root)
            os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'
            logger.info("Configuring Django with settings module: %s", os.environ['DJANGO_SETTINGS_MODULE'])
            django.setup()
            logger.debug("Django setup complete")

class DatabaseConnector:

    @staticmethod
    def get_cube_links_for_cube(cube_id):
        """Get all cube links that involve a specific cube (either as primary or foreign)"""
        logger.info("Getting cube links for cube_id: %s", cube_id)
        DjangoSetup.configure_django()
        from pybirdai.bird_meta_data_model import CUBE_LINK
        links = CUBE_LINK.objects.filter(
            models.Q(primary_cube_id=cube_id) |
            models.Q(foreign_cube_id=cube_id)
        ).select_related('primary_cube_id', 'foreign_cube_id')
        logger.debug("Found %d cube links for cube_id: %s", len(links), cube_id)
        return links

    @staticmethod
    def get_all_cube_links():
        """Get cube structure item links for a specific cube link"""
        logger.info("Getting all cube links")
        DjangoSetup.configure_django()
        from pybirdai.bird_meta_data_model import CUBE_LINK
        links = CUBE_LINK.objects.all()
        logger.debug("Found %d total cube links", len(links))
        return links

    @staticmethod
    def get_cube_structure_item_links(cube_link):
        """Get cube structure item links for a specific cube link"""
        logger.info("Getting cube structure item links for cube_link_id: %s", cube_link.cube_link_id)
        DjangoSetup.configure_django()
        from pybirdai.bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK
        links = CUBE_STRUCTURE_ITEM_LINK.objects.select_related(
            'primary_cube_variable_code',
            'foreign_cube_variable_code',
            'cube_link_id'
        ).filter(cube_link_id=cube_link)
        logger.debug("Found %d structure item links for cube_link_id: %s", len(links), cube_link.cube_link_id)
        return links

    @classmethod
    def get_linked_cube_structure_items(cls, cube_link):
        """Get quadruples of linked cube structure items"""
        logger.info("Building linked cube structure items for cube_link_id: %s", cube_link.cube_link_id)
        DjangoSetup.configure_django()
        from pybirdai.bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK,CUBE_STRUCTURE,CUBE_STRUCTURE_ITEM


        linked_items = []
        structure_item_links = cls.get_cube_structure_item_links(cube_link)

        for link in structure_item_links:
            linked_items.append((
                link.cube_link_id.primary_cube_id,
                link.primary_cube_variable_code,
                link.cube_link_id.foreign_cube_id,
                link.foreign_cube_variable_code
            ))

        logger.debug("Generated %d linked cube structure items", len(linked_items))
        return linked_items

    @staticmethod
    def create_visualization_json(linked_cube_structure_items):

        DjangoSetup.configure_django()
        from pybirdai.bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK,CUBE_STRUCTURE,CUBE_STRUCTURE_ITEM

        """Create JSON structure for visualization"""
        logger.info("Creating visualization JSON from %d linked items", len(linked_cube_structure_items))
        nodes = {}
        edges = []

        for primary_cube, primary_item, foreign_cube, foreign_item in linked_cube_structure_items:
            # Add nodes
            if primary_cube.cube_id not in nodes:
                logger.debug("Adding primary cube node: %s", primary_cube.name)
                nodes[primary_cube.cube_id] = {
                    'id': primary_cube.cube_id,
                    'name': primary_cube.name,
                    'code': primary_cube.cube_id,
                    'items': [],
                    'is_source': True
                }
            nodes[primary_cube.cube_id]['items'].append({
                'code': primary_item.variable_id.variable_id,
                'name': primary_item.description
            })

            if foreign_cube.cube_id not in nodes:
                logger.debug("Adding foreign cube node: %s", foreign_cube.name)
                nodes[foreign_cube.cube_id] = {
                    'id': foreign_cube.cube_id,
                    'name': foreign_cube.name,
                    'code': foreign_cube.cube_id,
                    'items': [],
                    'is_source': False
                }
            nodes[foreign_cube.cube_id]['items'].append({
                'code': foreign_item.variable_id.variable_id,
                'name': foreign_item.description
            })

            # Add edges
            logger.debug("Adding edge: %s -> %s", primary_cube.name, foreign_cube.name)
            edges.append({
                'source': primary_cube.name,
                'target': foreign_cube.name,
                'sourceItem': primary_item.variable_id.variable_id,
                'targetItem': foreign_item.variable_id.variable_id,
                'linkType': "primary" # Default linkType since we no longer have link object
            })



            # Change for #1533
            #
            foreign_structure = CUBE_STRUCTURE.objects.get(cube=foreign_cube)
            items = CUBE_STRUCTURE_ITEM.objects.all().filter(cube_structure_id = foreign_structure)
            items_as_node = {tuple(_.values()) for _ in nodes[foreign_cube.cube_id]['items']}

            for item in items:
                if (item.variable_id.variable_id,item.description) not in items_as_node:
                    nodes[foreign_cube.cube_id]['items'].append({
                        'code': item.variable_id.variable_id,
                        'name': item.description
                    })

        json_data = {
            'nodes': list(nodes.values()),
            'edges': edges
        }
        logger.info("Created visualization JSON with %d nodes and %d edges",
                   len(json_data['nodes']), len(json_data['edges']))
        return json_data

def return_line_break_at_23_char(string):
    if len(string) > 23:
        chunks = []
        for i in range(0, len(string), 23):
            chunks.append(string[i:i+23])
        return "\n".join(chunks)
    return string

class NetworkGraphGenerationService:
    @staticmethod
    def create_graph(json_data, file_name="", in_md=False):
        """Create a Mermaid chart visualization from JSON data"""
        logger.info("Creating graph visualization for file: %s", file_name)
        # Begin building the Mermaid flowchart definition
        mermaid_chart = "```mermaid\ngraph LR\n"  # Changed to TB (top to bottom)
        mermaid_chart += "    direction LR\n"     # Explicitly set direction

        # Organize nodes by type
        source_cubes = []
        source_items = []
        target_items = []
        target_cubes = []

        logger.debug("Organizing nodes by type")
        # Group nodes by type first
        for node in json_data['nodes']:
            is_source = node.get('is_source', any(edge['source'] == node['name'] for edge in json_data['edges']))
            node_id = "cube_" + ''.join(c if c.isalnum() else '_' for c in node['name'])

            if is_source:
                logger.debug("Adding source cube: %s", node['name'])
                source_cubes.append((node_id, node))
                for item in node['items']:
                    item_id = f"{node_id}_{item['code']}"
                    source_items.append((item_id, item, node_id))
            else:
                logger.debug("Adding target cube: %s", node['name'])
                target_cubes.append((node_id, node))
                for item in node['items']:
                    item_id = f"{node_id}_{item['code']}"
                    target_items.append((item_id, item, node_id))

        logger.debug("Adding source cubes to chart: %d cubes", len(source_cubes))
        # Add source cubes and items in subgraphs
        for node_id, node in source_cubes:
            # Create a subgraph for each source cube with its items
            mermaid_chart += f"    subgraph {node_id}_group[\"{node['name']}\"]\n"
            mermaid_chart += f"        {node_id}((\"{return_line_break_at_23_char(node['name'])}\"));\n"

            # Add items that belong to this cube in the subgraph
            for item_id, item, parent_node_id in source_items:
                if parent_node_id == node_id:
                    mermaid_chart += f"        {item_id}[{item['code']}];\n"
                    # Connect source cube to source item
                    if f"        {node_id} --> {item_id};\n" not in mermaid_chart:
                        mermaid_chart += f"        {node_id} --> {item_id};\n"

            mermaid_chart += "    end\n"

        logger.debug("Adding target cubes to chart: %d cubes", len(target_cubes))
        # Add target cubes and items in subgraphs
        for node_id, node in target_cubes:
            # Create a subgraph for each target cube with its items
            mermaid_chart += f"    subgraph {node_id}_group[\"{node['name']}\"];\n"
            mermaid_chart += f"        {node_id}((\"{return_line_break_at_23_char(node['name'])}\"));\n"

            # Add items that belong to this cube in the subgraph
            for item_id, item, parent_node_id in target_items:
                if parent_node_id == node_id:
                    mermaid_chart += f"        {item_id}{{{return_line_break_at_23_char(item['code'])}}};\n"
                    # Connect target item to target cube within the subgraph
                    if f"    {item_id} --> {node_id};\n" not in mermaid_chart:
                        mermaid_chart += f"        {item_id} --> {node_id};\n"

            mermaid_chart += "    end\n"

        logger.debug("Connecting target items to target cubes")
        # Connect target items to target cubes
        for item_id, item, node_id in target_items:
            if f"    {item_id} --> {node_id};\n" not in mermaid_chart:
                mermaid_chart += f"    {item_id} --> {node_id};\n"

        logger.debug("Adding cross connections between items: %d edges", len(json_data['edges']))
        # Add cross connections between items
        for edge in json_data['edges']:
            source_id = "cube_" + ''.join(c if c.isalnum() else '_' for c in edge['source'])
            target_id = "cube_" + ''.join(c if c.isalnum() else '_' for c in edge['target'])

            source_item_id = f"{source_id}_{edge['sourceItem']}"
            target_item_id = f"{target_id}_{edge['targetItem']}"

            # Connect source item to target item with a dashed line
            if f"    {source_item_id} --- {target_item_id};\n" not in mermaid_chart:
                mermaid_chart += f"    {source_item_id} --- {target_item_id};\n"

        # Define colors
        source_cube_color = "#FF9933"
        target_cube_color = "#FFCC33"
        source_item_color = "#99CCFF"
        target_item_color = "#99FF99"

        logger.debug("Adding styling to chart")
        # Add styling
        mermaid_chart += "    classDef sourceCube fill:#FF9933,stroke:#333,stroke-width:2px;\n"
        mermaid_chart += "    classDef targetCube fill:#FFCC33,stroke:#333,stroke-width:2px;\n"
        mermaid_chart += "    classDef sourceItem fill:#99CCFF,stroke:#333,stroke-width:1px;\n"
        mermaid_chart += "    classDef targetItem fill:#99FF99,stroke:#333,stroke-width:1px;\n"

        # Apply classes to nodes
        for node_id, node in source_cubes:
            mermaid_chart += f"    class {node_id} sourceCube;\n"

        for item_id, item, node_id in source_items:
            mermaid_chart += f"    class {item_id} sourceItem;\n"

        for item_id, item, node_id in target_items:
            mermaid_chart += f"    class {item_id} targetItem;\n"

        for node_id, node in target_cubes:
            mermaid_chart += f"    class {node_id} targetCube;\n"
            # Add legend as a subgraph

        logger.debug("Adding legend to chart")
        mermaid_chart += """    subgraph Legend["Legend"]
        direction LR
        source_cube_legend[("Source Cube")]:::sourceCube
        source_item_legend["Source Item"]:::sourceItem
        target_item_legend{"Target Item"}:::targetItem
        target_cube_legend[("Target Cube")]:::targetCube

        source_cube_legend --> source_item_legend
        source_item_legend --- target_item_legend
        target_item_legend --> target_cube_legend

        class source_cube_legend sourceCube
        class source_item_legend sourceItem
        class target_item_legend targetItem
        class target_cube_legend targetCube
    end
    """

        # End Mermaid block
        mermaid_chart += "```\n\n"

        # Add a title section before the Mermaid chart
        # title = f"# Mapping Visualization: {json_data['nodes'][1]['name']} to {json_data['nodes'][0]['name']}\n\n"

        # Complete markdown content
        logger.debug("Preparing final output content")
        markdown_content = mermaid_chart.replace("```mermaid","").replace("```","")

        html_content = f"""
        <!doctype html>
        <html lang="en">
          <body>
          <h1 style="font-family: Arial, sans-serif; color: #333; margin: 20px 0; text-align: center;">{file_name.replace('.html', '')}</h1>
            <pre class="mermaid">
{markdown_content}
            </pre>
            <script type="module">
              import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
            </script>
          </body>
        </html>
        """

        # Save to file
        output_folder = "results/generated_linking_visualisations/"
        output_file = file_name

        logger.info("Saving visualization to file: %s%s", output_folder, output_file)
        try:
            os.makedirs(output_folder, exist_ok=True)
            with open(output_folder+output_file, 'w') as f:
                f.write(html_content)
            logger.info("File saved successfully")
        except Exception as e:
            logger.error("Error saving file: %s", str(e))
            raise

        if in_md:
            return markdown_content
        return html_content


def process_cube_visualization(cube_id, join_identifier=None, in_md=False):
    """
    Process cube visualization for a given cube_id and optional join_identifier

    Args:
        cube_id: The ID of the cube to visualize
        join_identifier: Optional filter for join identifiers

    Returns:
        The file path of the generated visualization
    """
    logger.info("Processing cube visualization for cube_id: %s, join_identifier: %s",
                cube_id, join_identifier if join_identifier else "None")

    # Get all cube links for this cube
    all_cube_links = DatabaseConnector.get_cube_links_for_cube(cube_id)
    logger.info("Found %d total cube links for cube_id: %s", len(all_cube_links), cube_id)

    # Filter cube links based on join identifier
    cube_links = []
    for cube_link in all_cube_links:
        if join_identifier is None and not cube_link.join_identifier:
            # If no join_identifier was specified and this link has none
            cube_links.append(cube_link)
        elif join_identifier and cube_link.join_identifier == join_identifier:
            # If join_identifier was specified and matches this link
            cube_links.append(cube_link)

    logger.info("Filtered to %d cube links matching join_identifier criteria", len(cube_links))

    # Process the filtered cube links
    json_list = []
    for cube_link in cube_links:
        logger.debug("Processing cube_link: %s", cube_link.cube_link_id)
        linked_cube_structure_items = DatabaseConnector.get_linked_cube_structure_items(
            cube_link)
        json_list.append(DatabaseConnector.create_visualization_json(linked_cube_structure_items))

    # Merge all the JSONs for this join identifier
    merged_json = {'nodes': [], 'edges': []}
    for json_data in json_list:
        merged_json['nodes'].extend(json_data['nodes'])
        merged_json['edges'].extend(json_data['edges'])

    logger.info("Merged JSON contains %d nodes and %d edges",
               len(merged_json['nodes']), len(merged_json['edges']))

    # Generate a filename that includes the join identifier
    if join_identifier is None:
        mermaid_file_name = f"{cube_id}_no_join_identifier.html"
    else:
        # Sanitize join identifier for filename
        safe_join_id = ''.join(c if c.isalnum() else '_' for c in join_identifier)
        mermaid_file_name = f"{cube_id}_{safe_join_id}.html"

    logger.info("Generated filename: %s", mermaid_file_name)

    # Create the visualization using the identifier-specific filename
    return NetworkGraphGenerationService.create_graph(merged_json, mermaid_file_name, in_md=in_md)


if __name__ == "__main__":
    logger.info("Starting visualization service")
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        logger.error("Invalid command line arguments")
        print("Usage: python visualisation_service.py <cube_id> [<join_identifier>]")
        sys.exit(1)

    cube_id = sys.argv[1]
    join_identifier = sys.argv[2] if len(sys.argv) == 3 else None

    logger.info("Command line arguments - cube_id: %s, join_identifier: %s",
               cube_id, join_identifier if join_identifier else "None")

    try:
        html_content = process_cube_visualization(cube_id, join_identifier)
        logger.info("Visualization generation completed successfully")
    except Exception as e:
        logger.error("Error generating visualization: %s", str(e), exc_info=True)
        raise
