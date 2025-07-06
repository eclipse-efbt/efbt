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

from django.apps import AppConfig
import logging
import os
import sys

logger = logging.getLogger(__name__)


class VisualizationServiceProcessorConfig(AppConfig):
    """
    Django AppConfig for Visualization Service Processing operations.
    Provides cube visualization and network graph generation functionality.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'pybirdai.entry_points.visualization_service_processor'
    verbose_name = 'Visualization Service Processor'

    def ready(self):
        """Initialize the visualization service processor when Django starts."""
        logger.info("Visualization Service Processor initialized")


def configure_django_for_visualization():
    """
    Configure Django settings for visualization service.
    
    Returns:
        dict: Configuration result
    """
    try:
        from django.conf import settings
        import django
        
        if not settings.configured:
            # Set up Django settings module for birds_nest
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
            sys.path.insert(0, project_root)
            os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'
            django.setup()
            
            logger.info("Django configured for visualization service")
            
            return {
                'success': True,
                'message': 'Django configuration completed successfully'
            }
        else:
            return {
                'success': True,
                'message': 'Django already configured'
            }
            
    except Exception as e:
        logger.error(f"Django configuration failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Django configuration failed'
        }


def get_cube_links_for_cube(cube_id):
    """
    Get all cube links that involve a specific cube.
    
    Args:
        cube_id: The ID of the cube to get links for
        
    Returns:
        dict: Result with cube links data
    """
    logger.info(f"Getting cube links for cube_id: {cube_id}")
    
    try:
        # Configure Django if needed
        config_result = configure_django_for_visualization()
        if not config_result.get('success'):
            return config_result
        
        from pybirdai.bird_meta_data_model import CUBE_LINK
        from django.db import models
        
        links = CUBE_LINK.objects.filter(
            models.Q(primary_cube_id=cube_id) |
            models.Q(foreign_cube_id=cube_id)
        ).select_related('primary_cube_id', 'foreign_cube_id')
        
        logger.debug(f"Found {len(links)} cube links for cube_id: {cube_id}")
        
        return {
            'success': True,
            'cube_links': links,
            'count': len(links),
            'message': f'Found {len(links)} cube links for cube {cube_id}'
        }
        
    except Exception as e:
        logger.error(f"Failed to get cube links for cube {cube_id}: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to get cube links'
        }


def get_all_cube_links():
    """
    Get all cube links in the system.
    
    Returns:
        dict: Result with all cube links data
    """
    logger.info("Getting all cube links")
    
    try:
        # Configure Django if needed
        config_result = configure_django_for_visualization()
        if not config_result.get('success'):
            return config_result
        
        from pybirdai.bird_meta_data_model import CUBE_LINK
        
        links = CUBE_LINK.objects.all()
        
        logger.debug(f"Found {len(links)} total cube links")
        
        return {
            'success': True,
            'cube_links': links,
            'count': len(links),
            'message': f'Found {len(links)} total cube links'
        }
        
    except Exception as e:
        logger.error(f"Failed to get all cube links: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to get all cube links'
        }


def get_cube_structure_item_links(cube_link):
    """
    Get cube structure item links for a specific cube link.
    
    Args:
        cube_link: The cube link object to get structure item links for
        
    Returns:
        dict: Result with structure item links data
    """
    logger.info(f"Getting cube structure item links for cube_link_id: {cube_link.cube_link_id}")
    
    try:
        # Configure Django if needed
        config_result = configure_django_for_visualization()
        if not config_result.get('success'):
            return config_result
        
        from pybirdai.bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK
        
        links = CUBE_STRUCTURE_ITEM_LINK.objects.select_related(
            'primary_cube_variable_code',
            'foreign_cube_variable_code',
            'cube_link_id'
        ).filter(cube_link_id=cube_link)
        
        logger.debug(f"Found {len(links)} structure item links for cube_link_id: {cube_link.cube_link_id}")
        
        return {
            'success': True,
            'structure_item_links': links,
            'count': len(links),
            'message': f'Found {len(links)} structure item links'
        }
        
    except Exception as e:
        logger.error(f"Failed to get structure item links: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to get structure item links'
        }


def get_linked_cube_structure_items(cube_link):
    """
    Get quadruples of linked cube structure items.
    
    Args:
        cube_link: The cube link object to process
        
    Returns:
        dict: Result with linked cube structure items
    """
    logger.info(f"Building linked cube structure items for cube_link_id: {cube_link.cube_link_id}")
    
    try:
        # Get structure item links
        links_result = get_cube_structure_item_links(cube_link)
        if not links_result.get('success'):
            return links_result
        
        linked_items = []
        structure_item_links = links_result['structure_item_links']
        
        for link in structure_item_links:
            linked_items.append((
                link.cube_link_id.primary_cube_id,
                link.primary_cube_variable_code,
                link.cube_link_id.foreign_cube_id,
                link.foreign_cube_variable_code
            ))
        
        logger.debug(f"Generated {len(linked_items)} linked cube structure items")
        
        return {
            'success': True,
            'linked_items': linked_items,
            'count': len(linked_items),
            'message': f'Generated {len(linked_items)} linked cube structure items'
        }
        
    except Exception as e:
        logger.error(f"Failed to get linked cube structure items: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to get linked cube structure items'
        }


def create_visualization_json(linked_cube_structure_items):
    """
    Create JSON structure for visualization.
    
    Args:
        linked_cube_structure_items: List of linked cube structure items
        
    Returns:
        dict: Result with visualization JSON data
    """
    logger.info(f"Creating visualization JSON from {len(linked_cube_structure_items)} linked items")
    
    try:
        # Configure Django if needed
        config_result = configure_django_for_visualization()
        if not config_result.get('success'):
            return config_result
        
        from pybirdai.bird_meta_data_model import CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM
        
        nodes = {}
        edges = []
        
        for primary_cube, primary_item, foreign_cube, foreign_item in linked_cube_structure_items:
            # Add nodes
            if primary_cube.cube_id not in nodes:
                logger.debug(f"Adding primary cube node: {primary_cube.name}")
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
                logger.debug(f"Adding foreign cube node: {foreign_cube.name}")
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
            logger.debug(f"Adding edge: {primary_cube.name} -> {foreign_cube.name}")
            edges.append({
                'source': primary_cube.name,
                'target': foreign_cube.name,
                'sourceItem': primary_item.variable_id.variable_id,
                'targetItem': foreign_item.variable_id.variable_id,
                'linkType': "primary"
            })
            
            # Add all items from foreign cube structure
            try:
                foreign_structure = CUBE_STRUCTURE.objects.get(cube=foreign_cube)
                items = CUBE_STRUCTURE_ITEM.objects.filter(cube_structure_id=foreign_structure)
                items_as_node = {tuple(_.values()) for _ in nodes[foreign_cube.cube_id]['items']}
                
                for item in items:
                    if (item.variable_id.variable_id, item.description) not in items_as_node:
                        nodes[foreign_cube.cube_id]['items'].append({
                            'code': item.variable_id.variable_id,
                            'name': item.description
                        })
            except Exception as structure_error:
                logger.warning(f"Failed to add foreign cube structure items: {structure_error}")
        
        json_data = {
            'nodes': list(nodes.values()),
            'edges': edges
        }
        
        logger.info(f"Created visualization JSON with {len(json_data['nodes'])} nodes and {len(json_data['edges'])} edges")
        
        return {
            'success': True,
            'visualization_json': json_data,
            'nodes_count': len(json_data['nodes']),
            'edges_count': len(json_data['edges']),
            'message': f'Created visualization JSON with {len(json_data["nodes"])} nodes and {len(json_data["edges"])} edges'
        }
        
    except Exception as e:
        logger.error(f"Failed to create visualization JSON: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to create visualization JSON'
        }


def format_text_with_line_breaks(text, max_length=23):
    """
    Format text with line breaks at specified character limit.
    
    Args:
        text (str): Text to format
        max_length (int): Maximum length before line break
        
    Returns:
        str: Formatted text with line breaks
    """
    if len(text) > max_length:
        chunks = []
        for i in range(0, len(text), max_length):
            chunks.append(text[i:i+max_length])
        return "\\n".join(chunks)
    return text


def create_mermaid_graph(json_data, file_name="", output_format="html"):
    """
    Create a Mermaid chart visualization from JSON data.
    
    Args:
        json_data: Visualization JSON data
        file_name (str): Name for the output file
        output_format (str): Output format - "html" or "markdown"
        
    Returns:
        dict: Result with generated visualization content
    """
    logger.info(f"Creating Mermaid graph visualization for file: {file_name}")
    
    try:
        # Begin building the Mermaid flowchart definition
        mermaid_chart = "```mermaid\\ngraph LR\\n"
        mermaid_chart += "    direction LR\\n"
        
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
                logger.debug(f"Adding source cube: {node['name']}")
                source_cubes.append((node_id, node))
                for item in node['items']:
                    item_id = f"{node_id}_{item['code']}"
                    source_items.append((item_id, item, node_id))
            else:
                logger.debug(f"Adding target cube: {node['name']}")
                target_cubes.append((node_id, node))
                for item in node['items']:
                    item_id = f"{node_id}_{item['code']}"
                    target_items.append((item_id, item, node_id))
        
        logger.debug(f"Adding source cubes to chart: {len(source_cubes)} cubes")
        # Add source cubes and items in subgraphs
        for node_id, node in source_cubes:
            mermaid_chart += f"    subgraph {node_id}_group[\\\"{node['name']}\\\"]\\n"
            mermaid_chart += f"        {node_id}((\\"{format_text_with_line_breaks(node['name'])}\\"));\\n"
            
            # Add items that belong to this cube in the subgraph
            for item_id, item, parent_node_id in source_items:
                if parent_node_id == node_id:
                    mermaid_chart += f"        {item_id}[{item['code']}];\\n"
                    # Connect source cube to source item
                    if f"        {node_id} --> {item_id};\\n" not in mermaid_chart:
                        mermaid_chart += f"        {node_id} --> {item_id};\\n"
            
            mermaid_chart += "    end\\n"
        
        logger.debug(f"Adding target cubes to chart: {len(target_cubes)} cubes")
        # Add target cubes and items in subgraphs
        for node_id, node in target_cubes:
            mermaid_chart += f"    subgraph {node_id}_group[\\\"{node['name']}\\\"];\\n"
            mermaid_chart += f"        {node_id}((\\"{format_text_with_line_breaks(node['name'])}\\"));\\n"
            
            # Add items that belong to this cube in the subgraph
            for item_id, item, parent_node_id in target_items:
                if parent_node_id == node_id:
                    mermaid_chart += f"        {item_id}{{{format_text_with_line_breaks(item['code'])}}};\\n"
                    # Connect target item to target cube within the subgraph
                    if f"    {item_id} --> {node_id};\\n" not in mermaid_chart:
                        mermaid_chart += f"        {item_id} --> {node_id};\\n"
            
            mermaid_chart += "    end\\n"
        
        logger.debug(f"Adding cross connections between items: {len(json_data['edges'])} edges")
        # Add cross connections between items
        for edge in json_data['edges']:
            source_id = "cube_" + ''.join(c if c.isalnum() else '_' for c in edge['source'])
            target_id = "cube_" + ''.join(c if c.isalnum() else '_' for c in edge['target'])
            
            source_item_id = f"{source_id}_{edge['sourceItem']}"
            target_item_id = f"{target_id}_{edge['targetItem']}"
            
            # Connect source item to target item with a dashed line
            if f"    {source_item_id} --- {target_item_id};\\n" not in mermaid_chart:
                mermaid_chart += f"    {source_item_id} --- {target_item_id};\\n"
        
        logger.debug("Adding styling to chart")
        # Add styling
        mermaid_chart += "    classDef sourceCube fill:#FF9933,stroke:#333,stroke-width:2px;\\n"
        mermaid_chart += "    classDef targetCube fill:#FFCC33,stroke:#333,stroke-width:2px;\\n"
        mermaid_chart += "    classDef sourceItem fill:#99CCFF,stroke:#333,stroke-width:1px;\\n"
        mermaid_chart += "    classDef targetItem fill:#99FF99,stroke:#333,stroke-width:1px;\\n"
        
        # Apply classes to nodes
        for node_id, node in source_cubes:
            mermaid_chart += f"    class {node_id} sourceCube;\\n"
        
        for item_id, item, node_id in source_items:
            mermaid_chart += f"    class {item_id} sourceItem;\\n"
        
        for item_id, item, node_id in target_items:
            mermaid_chart += f"    class {item_id} targetItem;\\n"
        
        for node_id, node in target_cubes:
            mermaid_chart += f"    class {node_id} targetCube;\\n"
        
        logger.debug("Adding legend to chart")
        # Add legend
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
        mermaid_chart += "```\\n\\n"
        
        # Prepare content based on format
        if output_format.lower() == "html":
            markdown_content = mermaid_chart.replace("```mermaid", "").replace("```", "")
            
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
                  mermaid.initialize({{
                    maxTextSize: 90000
                  }});
                </script>
              </body>
            </html>
            """
            
            return {
                'success': True,
                'content': html_content,
                'format': 'html',
                'message': 'HTML Mermaid graph created successfully'
            }
        else:
            return {
                'success': True,
                'content': mermaid_chart,
                'format': 'markdown',
                'message': 'Markdown Mermaid graph created successfully'
            }
        
    except Exception as e:
        logger.error(f"Failed to create Mermaid graph: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to create Mermaid graph'
        }


def save_visualization_to_file(content, file_path, create_directory=True):
    """
    Save visualization content to file.
    
    Args:
        content (str): Content to save
        file_path (str): Path to save file
        create_directory (bool): Whether to create directory if it doesn't exist
        
    Returns:
        dict: Result with success status
    """
    logger.info(f"Saving visualization to file: {file_path}")
    
    try:
        if create_directory:
            directory = os.path.dirname(file_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
        
        with open(file_path, 'w') as f:
            f.write(content)
        
        logger.info("File saved successfully")
        
        return {
            'success': True,
            'file_path': file_path,
            'message': f'Visualization saved to {file_path}'
        }
        
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to save visualization file'
        }


def process_cube_visualization(cube_id, join_identifier=None, output_format="html"):
    """
    Process cube visualization for a given cube_id and optional join_identifier.
    
    Args:
        cube_id: The ID of the cube to visualize
        join_identifier: Optional filter for join identifiers
        output_format (str): Output format - "html" or "markdown"
        
    Returns:
        dict: Result with visualization processing information
    """
    logger.info(f"Processing cube visualization for cube_id: {cube_id}, join_identifier: {join_identifier or 'None'}")
    
    try:
        # Get all cube links for this cube
        cube_links_result = get_cube_links_for_cube(cube_id)
        if not cube_links_result.get('success'):
            return cube_links_result
        
        all_cube_links = cube_links_result['cube_links']
        logger.info(f"Found {len(all_cube_links)} total cube links for cube_id: {cube_id}")
        
        # Filter cube links based on join identifier
        cube_links = []
        for cube_link in all_cube_links:
            if join_identifier is None and not cube_link.join_identifier:
                # If no join_identifier was specified and this link has none
                cube_links.append(cube_link)
            elif join_identifier and cube_link.join_identifier == join_identifier:
                # If join_identifier was specified and matches this link
                cube_links.append(cube_link)
        
        logger.info(f"Filtered to {len(cube_links)} cube links matching join_identifier criteria")
        
        # Process the filtered cube links
        json_list = []
        for cube_link in cube_links:
            logger.debug(f"Processing cube_link: {cube_link.cube_link_id}")
            
            linked_items_result = get_linked_cube_structure_items(cube_link)
            if linked_items_result.get('success'):
                viz_json_result = create_visualization_json(linked_items_result['linked_items'])
                if viz_json_result.get('success'):
                    json_list.append(viz_json_result['visualization_json'])
        
        # Merge all the JSONs for this join identifier
        merged_json = {'nodes': [], 'edges': []}
        for json_data in json_list:
            merged_json['nodes'].extend(json_data['nodes'])
            merged_json['edges'].extend(json_data['edges'])
        
        logger.info(f"Merged JSON contains {len(merged_json['nodes'])} nodes and {len(merged_json['edges'])} edges")
        
        # Generate a filename that includes the join identifier
        if join_identifier is None:
            file_name = f"{cube_id}_no_join_identifier.html"
        else:
            # Sanitize join identifier for filename
            safe_join_id = ''.join(c if c.isalnum() else '_' for c in join_identifier)
            file_name = f"{cube_id}_{safe_join_id}.html"
        
        logger.info(f"Generated filename: {file_name}")
        
        # Create the visualization
        graph_result = create_mermaid_graph(merged_json, file_name, output_format)
        if not graph_result.get('success'):
            return graph_result
        
        # Save to file
        output_folder = "results/generated_linking_visualisations/"
        full_file_path = os.path.join(output_folder, file_name)
        
        save_result = save_visualization_to_file(graph_result['content'], full_file_path)
        if not save_result.get('success'):
            return save_result
        
        return {
            'success': True,
            'file_path': full_file_path,
            'content': graph_result['content'],
            'format': output_format,
            'cube_id': cube_id,
            'join_identifier': join_identifier,
            'nodes_count': len(merged_json['nodes']),
            'edges_count': len(merged_json['edges']),
            'message': f'Cube visualization processed successfully and saved to {full_file_path}'
        }
        
    except Exception as e:
        logger.error(f"Error processing cube visualization: {e}")
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to process cube visualization'
        }


class VisualizationServiceProcessor:
    """
    Main visualization service processor class providing high-level interface.
    Handles cube visualization, network graph generation, and Mermaid chart creation.
    """
    
    def __init__(self):
        """Initialize the visualization service processor."""
        logger.info("VisualizationServiceProcessor initialized")
    
    def create_cube_visualization_workflow(self, cube_ids, join_identifiers=None, output_formats=None):
        """
        Create visualization workflow for multiple cubes.
        
        Args:
            cube_ids (list): List of cube IDs to visualize
            join_identifiers (list): Optional list of join identifiers
            output_formats (list): List of output formats
            
        Returns:
            dict: Complete workflow results
        """
        if output_formats is None:
            output_formats = ['html']
        
        workflow_results = {
            'success': True,
            'processed_cubes': [],
            'failed_cubes': [],
            'total_visualizations': 0,
            'errors': []
        }
        
        try:
            for cube_id in cube_ids:
                for output_format in output_formats:
                    if join_identifiers:
                        for join_identifier in join_identifiers:
                            viz_result = process_cube_visualization(
                                cube_id, 
                                join_identifier, 
                                output_format
                            )
                            
                            if viz_result.get('success'):
                                workflow_results['processed_cubes'].append({
                                    'cube_id': cube_id,
                                    'join_identifier': join_identifier,
                                    'format': output_format,
                                    'file_path': viz_result.get('file_path')
                                })
                                workflow_results['total_visualizations'] += 1
                            else:
                                workflow_results['failed_cubes'].append({
                                    'cube_id': cube_id,
                                    'join_identifier': join_identifier,
                                    'format': output_format,
                                    'error': viz_result.get('error')
                                })
                                workflow_results['errors'].append(viz_result.get('error'))
                    else:
                        viz_result = process_cube_visualization(
                            cube_id, 
                            None, 
                            output_format
                        )
                        
                        if viz_result.get('success'):
                            workflow_results['processed_cubes'].append({
                                'cube_id': cube_id,
                                'join_identifier': None,
                                'format': output_format,
                                'file_path': viz_result.get('file_path')
                            })
                            workflow_results['total_visualizations'] += 1
                        else:
                            workflow_results['failed_cubes'].append({
                                'cube_id': cube_id,
                                'join_identifier': None,
                                'format': output_format,
                                'error': viz_result.get('error')
                            })
                            workflow_results['errors'].append(viz_result.get('error'))
            
            # Determine overall success
            workflow_results['success'] = len(workflow_results['failed_cubes']) == 0
            
        except Exception as e:
            workflow_results['success'] = False
            workflow_results['errors'].append(f"Workflow error: {str(e)}")
            logger.error(f"Visualization workflow error: {e}")
        
        return workflow_results


# Convenience function for backwards compatibility
def run_visualization_service_operations():
    """Get a configured visualization service processor instance."""
    return VisualizationServiceProcessor()


# Export main functions for easy access
__all__ = [
    'VisualizationServiceProcessorConfig',
    'configure_django_for_visualization',
    'get_cube_links_for_cube',
    'get_all_cube_links',
    'get_cube_structure_item_links',
    'get_linked_cube_structure_items',
    'create_visualization_json',
    'create_mermaid_graph',
    'save_visualization_to_file',
    'process_cube_visualization',
    'VisualizationServiceProcessor',
    'run_visualization_service_operations'
]