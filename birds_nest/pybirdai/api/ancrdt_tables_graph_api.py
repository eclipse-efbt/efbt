# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
ANCRDT Tables Graph API

Provides JSON data for visualizing ANCRDT table relationships in a Cytoscape.js graph.
"""

import csv
import os
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.conf import settings


def _load_join_configurations():
    """
    Load join configurations from CSV files.

    Returns:
        tuple: (join_definitions dict, rolc_joins dict)
    """
    joins_config_dir = os.path.join(settings.BASE_DIR, 'artefacts', 'joins_configuration')

    # Load join definitions
    join_defs = {}
    join_def_file = os.path.join(joins_config_dir, 'join_for_product_il_definitions_ANCRDT_REF.csv')
    if os.path.exists(join_def_file):
        with open(join_def_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get('Name', '').strip()
                if name:
                    join_defs[name] = {
                        'main_table': row.get('Main Table', '').strip(),
                        'filter': row.get('Filter', '').strip(),
                        'related_tables': [t.strip() for t in row.get('Related Tables', '').split(':') if t.strip()],
                        'comments': row.get('Comments', '').strip()
                    }

    # Load ROLC to join mappings
    rolc_joins = {}
    rolc_join_file = os.path.join(joins_config_dir, 'join_for_product_to_reference_category_ANCRDT_REF.csv')
    if os.path.exists(rolc_join_file):
        with open(rolc_join_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rolc = row.get('rolc', '').strip()
                join_id = row.get('join_identifier', '').strip()
                if rolc and join_id:
                    if rolc not in rolc_joins:
                        rolc_joins[rolc] = []
                    rolc_joins[rolc].append(join_id)

    return join_defs, rolc_joins


@require_http_methods(["GET"])
def get_ancrdt_tables_graph(request):
    """
    Returns graph data for ANCRDT table relationships.

    Returns JSON with:
    - nodes: ANCRDT cubes, join configuration subgraphs (compound nodes), and IL tables
    - edges: Join relationships between tables
    """
    try:
        join_defs, rolc_joins = _load_join_configurations()

        nodes = []
        edges = []
        node_ids = set()
        edge_ids = set()  # Track edge IDs to avoid duplicates

        # Define ANCRDT cube colors
        ancrdt_cubes = {
            'ANCRDT_INSTRMNT_C_1': {'label': 'ANCRDT Instrument', 'color': '#4A90E2'},
            'ANCRDT_FNNCL_C_1': {'label': 'ANCRDT Financial', 'color': '#50C878'},
            'ANCRDT_ACCNTNG_C_1': {'label': 'ANCRDT Accounting', 'color': '#FFB84D'},
            'ANCRDT_ENTTY_C_1': {'label': 'ANCRDT Counterparty', 'color': '#FF6B6B'},
            'ANCRDT_PRTCTN_RCVD_C_1': {'label': 'ANCRDT Protection', 'color': '#9370DB'},
        }

        # Subgraph colors for join configurations
        subgraph_colors = [
            '#2C3E50', '#34495E', '#1ABC9C', '#16A085', '#27AE60',
            '#2980B9', '#8E44AD', '#2C3E50', '#F39C12', '#D35400'
        ]

        # Add ANCRDT cube nodes
        for cube_id, cube_info in ancrdt_cubes.items():
            if cube_id not in node_ids:
                nodes.append({
                    'id': cube_id,
                    'label': cube_info['label'],
                    'type': 'ancrdt_cube',
                    'color': cube_info['color'],
                    'details': {
                        'cube_id': cube_id,
                        'joins': rolc_joins.get(cube_id, [])
                    }
                })
                node_ids.add(cube_id)

        # Process join definitions - create subgraph parent nodes
        for idx, (join_name, join_def) in enumerate(join_defs.items()):
            main_table = join_def['main_table']
            related_tables = join_def['related_tables']
            filter_table = join_def['filter']

            # Create join configuration as a compound/parent node (subgraph)
            join_node_id = f'join_{join_name.replace(" ", "_")}'
            subgraph_color = subgraph_colors[idx % len(subgraph_colors)]

            if join_node_id not in node_ids:
                nodes.append({
                    'id': join_node_id,
                    'label': join_name,
                    'type': 'join_subgraph',
                    'color': subgraph_color,
                    'details': {
                        'join_name': join_name,
                        'main_table': main_table,
                        'filter': filter_table,
                        'related_tables': related_tables,
                        'comments': join_def.get('comments', '')
                    }
                })
                node_ids.add(join_node_id)

            # Add main table node as child of join subgraph
            if main_table:
                table_node_id = f'{join_node_id}_{main_table}'
                if table_node_id not in node_ids:
                    nodes.append({
                        'id': table_node_id,
                        'label': main_table,
                        'type': 'il_table',
                        'color': '#2E8B57',
                        'parent': join_node_id,
                        'details': {
                            'table_type': 'main',
                            'join_name': join_name
                        }
                    })
                    node_ids.add(table_node_id)

            # Add filter table node as child
            if filter_table:
                filter_node_id = f'{join_node_id}_{filter_table}'
                if filter_node_id not in node_ids:
                    nodes.append({
                        'id': filter_node_id,
                        'label': filter_table,
                        'type': 'filter_table',
                        'color': '#CD853F',
                        'parent': join_node_id,
                        'details': {
                            'table_type': 'filter',
                            'join_name': join_name
                        }
                    })
                    node_ids.add(filter_node_id)

            # Add related tables as children
            for related_table in related_tables:
                if related_table:
                    related_node_id = f'{join_node_id}_{related_table}'
                    if related_node_id not in node_ids:
                        is_assignment = 'ASSGNMNT' in related_table or 'RL' in related_table
                        nodes.append({
                            'id': related_node_id,
                            'label': related_table,
                            'type': 'assignment_table' if is_assignment else 'il_table',
                            'color': '#FFA500' if is_assignment else '#2E8B57',
                            'parent': join_node_id,
                            'details': {
                                'table_type': 'assignment' if is_assignment else 'related',
                                'join_name': join_name
                            }
                        })
                        node_ids.add(related_node_id)

            # Create edges from ANCRDT cubes to join subgraphs
            for rolc, join_ids in rolc_joins.items():
                if join_name in join_ids:
                    edge_id = f'{rolc}_to_{join_node_id}'
                    if edge_id not in edge_ids:
                        edges.append({
                            'id': edge_id,
                            'source': rolc,
                            'target': join_node_id,
                            'type': 'joins_to',
                            'label': '',
                            'details': {
                                'join_name': join_name
                            }
                        })
                        edge_ids.add(edge_id)

            # Create internal edges within the subgraph (main -> related)
            if main_table:
                main_node_id = f'{join_node_id}_{main_table}'
                for related_table in related_tables:
                    if related_table:
                        related_node_id = f'{join_node_id}_{related_table}'
                        edge_id = f'{main_node_id}_to_{related_node_id}'
                        if edge_id not in edge_ids:
                            edges.append({
                                'id': edge_id,
                                'source': main_node_id,
                                'target': related_node_id,
                                'type': 'relates_to',
                                'label': '',
                                'details': {
                                    'join_name': join_name
                                }
                            })
                            edge_ids.add(edge_id)

                # Edge from main to filter
                if filter_table:
                    filter_node_id = f'{join_node_id}_{filter_table}'
                    edge_id = f'{main_node_id}_to_{filter_node_id}'
                    if edge_id not in edge_ids:
                        edges.append({
                            'id': edge_id,
                            'source': main_node_id,
                            'target': filter_node_id,
                            'type': 'filtered_by',
                            'label': 'filter',
                            'details': {
                                'join_name': join_name
                            }
                        })
                        edge_ids.add(edge_id)

        return JsonResponse({
            'nodes': nodes,
            'edges': edges,
            'summary': {
                'ancrdt_cubes': len([n for n in nodes if n['type'] == 'ancrdt_cube']),
                'join_subgraphs': len([n for n in nodes if n['type'] == 'join_subgraph']),
                'il_tables': len([n for n in nodes if n['type'] == 'il_table']),
                'assignment_tables': len([n for n in nodes if n['type'] == 'assignment_table']),
                'filter_tables': len([n for n in nodes if n['type'] == 'filter_table']),
                'total_edges': len(edges),
                'join_definitions': len(join_defs),
                'rolc_mappings': len(rolc_joins)
            }
        })

    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error in get_ancrdt_tables_graph: {str(e)}", exc_info=True)

        return JsonResponse({
            'error': 'Failed to load ANCRDT tables graph data',
            'message': str(e)
        }, status=500)
