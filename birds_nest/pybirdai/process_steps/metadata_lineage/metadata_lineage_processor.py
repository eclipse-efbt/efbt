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
#

import json
import os
from typing import Dict, List, Set, Tuple
from django.db import transaction

from pybirdai.models.bird_meta_data_model import (
    CUBE, CUBE_STRUCTURE, CUBE_STRUCTURE_ITEM, VARIABLE,
    MEMBER, CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK,
    COMBINATION, COMBINATION_ITEM, MAINTENANCE_AGENCY
)
from pybirdai.models.lineage_model import DataItem, Process, Relationship


class MetadataLineageProcessor:
    """
    Processes BIRD metadata to create metadata lineage graph.
    
    This processor creates DataItem, Process, and Relationship instances
    that represent the metadata lineage (as opposed to data lineage).
    """
    
    def __init__(self, sdd_context):
        self.sdd_context = sdd_context
        self.created_data_items = {}  # Cache for created data items
        self.created_processes = {}   # Cache for created processes
        
    @transaction.atomic
    def process_input_tables(self):
        """
        Process input tables and their columns to create data items.
        
        For each cube structure related to input tables:
        - Create a data item with type 'table'
        - Create data items with type 'column' for each cube structure item
        """
        print("Processing input tables...")
        
        # Find input layer cubes (None type are typically input cubes)
        input_cubes = CUBE.objects.filter(
            cube_type__isnull=True
        )
        
        for cube in input_cubes:
            # Create table data item
            table_data_item = self._create_or_get_data_item(
                type='table',
                location=cube.cube_id,
                description=f"Input table: {cube.cube_id}"
            )
            
            # Get cube structure items for this cube
            if cube.cube_structure_id:
                cube_structure_items = CUBE_STRUCTURE_ITEM.objects.filter(
                    cube_structure_id=cube.cube_structure_id
                )
                
                for item in cube_structure_items:
                    # Create column data item
                    column_location = f"{cube.cube_id}.{item.variable_id.variable_id}"
                    column_data_item = self._create_or_get_data_item(
                        type='column',
                        location=column_location,
                        description=f"Column {item.variable_id.variable_id} in table {cube.cube_id}"
                    )
    
    @transaction.atomic
    def process_output_tables(self):
        """
        Process output tables and their columns to create data items.
        
        For each cube structure related to output tables:
        - Create a data item with type 'table'
        - Create data items with type 'column' for each cube structure item
        """
        print("Processing output tables...")
        
        # Find output layer cubes (RC type are report cubes)
        output_cubes = CUBE.objects.filter(
            cube_type='RC'
        )
        
        for cube in output_cubes:
            # Create table data item
            table_data_item = self._create_or_get_data_item(
                type='table',
                location=cube.cube_id,
                description=f"Output table: {cube.cube_id}"
            )
            
            # Get cube structure items for this cube
            if cube.cube_structure_id:
                cube_structure_items = CUBE_STRUCTURE_ITEM.objects.filter(
                    cube_structure_id=cube.cube_structure_id
                )
                
                for item in cube_structure_items:
                    # Create column data item
                    column_location = f"{cube.cube_id}.{item.variable_id.variable_id}"
                    column_data_item = self._create_or_get_data_item(
                        type='column',
                        location=column_location,
                        description=f"Column {item.variable_id.variable_id} in table {cube.cube_id}"
                    )
                    
                    # Check for cube structure item links (column-to-column lineage)
                    item_links = CUBE_STRUCTURE_ITEM_LINK.objects.filter(
                        cube_structure_item=item
                    )
                    
                    for link in item_links:
                        # Create column-to-column process
                        process_name = f"Transform_{link.foreign_cube_variable_id}_to_{item.variable_id.variable_id}"
                        process = self._create_or_get_process(
                            name=process_name,
                            type='column_to_column',
                            function_type='basic',
                            description=f"Transform from {link.foreign_cube_variable_id} to {item.variable_id.variable_id}"
                        )
                        
                        # Create consume relationship
                        source_column_location = f"{link.foreign_cube_id}.{link.foreign_cube_variable_id}"
                        source_data_item = self._create_or_get_data_item(
                            type='column',
                            location=source_column_location,
                            description=f"Source column for transformation"
                        )
                        
                        self._create_relationship(
                            process=process,
                            data_item=source_data_item,
                            type='consumes'
                        )
                        
                        # Create produce relationship
                        self._create_relationship(
                            process=process,
                            data_item=column_data_item,
                            type='produces'
                        )
    
    @transaction.atomic
    def process_product_specific_joins(self):
        """
        Process product-specific joins created by cube_links.
        
        1. Identify cube_links between output and input layers
        2. Group cube_links by report and product
        3. Create join tables and processes
        """
        print("Processing product-specific joins...")
        
        # Get cube links between layers
        cube_links = CUBE_LINK.objects.select_related(
            'parent_cube', 'child_cube'
        ).all()
        
        # Group by report and product
        join_groups = {}
        for link in cube_links:
            if link.parent_cube.cube_type == 'OUTPUT':
                # Create a key for grouping
                report_name = link.parent_cube.cube_id.split('_')[0] if '_' in link.parent_cube.cube_id else 'DEFAULT'
                product_name = link.child_cube.maintenance_agency.agency_id if link.child_cube.maintenance_agency else 'DEFAULT'
                key = (report_name, product_name, link.parent_cube.cube_id)
                
                if key not in join_groups:
                    join_groups[key] = []
                join_groups[key].append(link)
        
        # Process each group
        for (report_name, product_name, output_layer_name), links in join_groups.items():
            # Create product-specific join table data item
            join_table_name = f"{output_layer_name}.{product_name}"
            join_table_data_item = self._create_or_get_data_item(
                type='table',
                location=join_table_name,
                description=f"Product-specific join table for {product_name} in {output_layer_name}"
            )
            
            # Create join process
            join_process_name = f"Join_{output_layer_name}_{product_name}"
            join_process = self._create_or_get_process(
                name=join_process_name,
                type='table_to_table',
                function_type='join',
                description=f"Join operation for {product_name} tables to create {join_table_name}"
            )
            
            # Track source tables
            source_tables = set()
            for link in links:
                source_tables.add(link.child_cube.cube_id)
            
            # Create consume relationships for source tables
            for source_table_id in source_tables:
                source_table_data_item = self._create_or_get_data_item(
                    type='table',
                    location=source_table_id,
                    description=f"Source table for join"
                )
                
                self._create_relationship(
                    process=join_process,
                    data_item=source_table_data_item,
                    type='consumes'
                )
            
            # Create produce relationship for join table
            self._create_relationship(
                process=join_process,
                data_item=join_table_data_item,
                type='produces'
            )
            
            # Create columns in the join table and processes from join to output
            parent_cube = links[0].parent_cube if links else None
            if parent_cube:
                cube_structures = CUBE_STRUCTURE.objects.filter(cube=parent_cube)
                
                for cube_structure in cube_structures:
                    items = CUBE_STRUCTURE_ITEM.objects.filter(cube_structure=cube_structure)
                    
                    for item in items:
                        # Create column in join table
                        join_column_location = f"{join_table_name}.{item.variable_id.variable_id}"
                        join_column_data_item = self._create_or_get_data_item(
                            type='column',
                            location=join_column_location,
                            description=f"Column {item.variable_id.variable_id} in join table {join_table_name}"
                        )
                        
                        # Create column in output table
                        output_column_location = f"{output_layer_name}.{item.variable_id.variable_id}"
                        output_column_data_item = self._create_or_get_data_item(
                            type='column',
                            location=output_column_location,
                            description=f"Column {item.variable_id.variable_id} in output table {output_layer_name}"
                        )
                        
                        # Create process from join column to output column
                        column_process_name = f"Copy_{join_column_location}_to_{output_column_location}"
                        column_process = self._create_or_get_process(
                            name=column_process_name,
                            type='column_to_column',
                            function_type='basic',
                            description=f"Copy column from join table to output table"
                        )
                        
                        self._create_relationship(
                            process=column_process,
                            data_item=join_column_data_item,
                            type='consumes'
                        )
                        
                        self._create_relationship(
                            process=column_process,
                            data_item=output_column_data_item,
                            type='produces'
                        )
    
    @transaction.atomic
    def process_datapoints(self):
        """
        Process datapoints and their associated combinations.
        
        Each datapoint:
        - Is treated as a table with a single column
        - Has one associated combination
        - The column represents the metric
        """
        print("Processing datapoints...")
        
        combinations = COMBINATION.objects.all()
        
        for combination in combinations:
            
            # Create table data item for the combination/datapoint
            table_data_item = self._create_or_get_data_item(
                type='table',
                location=combination.combination_id,
                description=f"Datapoint table for {combination.combination_id}"
            )
            
            # Create column data item for the metric
            if combination.metric:
                metric_column_location = f"{combination.combination_id}.{combination.metric.variable_id}"
                metric_column_data_item = self._create_or_get_data_item(
                    type='column',
                    location=metric_column_location,
                    description=f"Metric column {combination.metric.variable_id} for datapoint"
                )
                
                # Create aggregation process
                agg_process_name = f"Aggregate_{combination.combination_id}"
                agg_process = self._create_or_get_process(
                    name=agg_process_name,
                    type='columns_to_table',
                    function_type='aggregate_metric',
                    description=f"Aggregation for metric {combination.metric.variable_id}"
                )
                
                # Process combination items to find source columns
                combination_items = COMBINATION_ITEM.objects.filter(
                    combination_id=combination
                ).select_related('variable_id')
                
                source_columns = set()
                for item in combination_items:
                    if item.variable_id:
                        # Find which output table this variable might be in
                        # This is a simplified approach - in reality you'd need more context
                        output_cubes = CUBE.objects.filter(cube_type='RC')
                        for cube in output_cubes:
                            # Check if this variable exists in this cube's structure
                            if cube.cube_structure_id and CUBE_STRUCTURE_ITEM.objects.filter(
                                cube_structure_id=cube.cube_structure_id,
                                variable_id=item.variable_id
                            ).exists():
                                source_column_location = f"{cube.cube_id}.{item.variable_id.variable_id}"
                                source_columns.add(source_column_location)
                                break
                
                # Create consume relationships for source columns
                for source_col_loc in source_columns:
                    source_data_item = self._create_or_get_data_item(
                        type='column',
                        location=source_col_loc,
                        description=f"Source column for aggregation"
                    )
                    
                    self._create_relationship(
                        process=agg_process,
                        data_item=source_data_item,
                        type='consumes'
                    )
                
                # Create produce relationship for metric column
                self._create_relationship(
                    process=agg_process,
                    data_item=metric_column_data_item,
                    type='produces'
                )
                
                # Also indicate that the process produces the table
                self._create_relationship(
                    process=agg_process,
                    data_item=table_data_item,
                    type='produces'
                )
    
    def export_lineage_to_json(self, output_path: str):
        """
        Export the complete metadata lineage graph to JSON format.
        """
        print(f"Exporting metadata lineage to {output_path}...")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Collect all data
        data_items = DataItem.objects.all()
        processes = Process.objects.all()
        relationships = Relationship.objects.select_related('process', 'data_item').all()
        
        # Build the lineage graph
        lineage_data = {
            'data_items': [
                {
                    'id': item.id,
                    'type': item.type,
                    'location': item.location,
                    'description': item.description
                }
                for item in data_items
            ],
            'processes': [
                {
                    'id': process.id,
                    'name': process.name,
                    'type': process.type,
                    'function_type': process.function_type,
                    'description': process.description,
                    'source_reference': process.source_reference
                }
                for process in processes
            ],
            'relationships': [
                {
                    'id': rel.id,
                    'process_id': rel.process_id,
                    'process_name': rel.process.name,
                    'data_item_id': rel.data_item_id,
                    'data_item_location': rel.data_item.location,
                    'type': rel.type
                }
                for rel in relationships
            ]
        }
        
        # Write to file
        with open(output_path, 'w') as f:
            json.dump(lineage_data, f, indent=2)
        
        print(f"Exported {len(data_items)} data items, {len(processes)} processes, and {len(relationships)} relationships")
    
    def _create_or_get_data_item(self, type: str, location: str, description: str = None) -> DataItem:
        """
        Create or retrieve a data item, using cache to avoid duplicates.
        """
        cache_key = f"{type}:{location}"
        
        if cache_key in self.created_data_items:
            return self.created_data_items[cache_key]
        
        data_item, created = DataItem.objects.get_or_create(
            type=type,
            location=location,
            defaults={'description': description}
        )
        
        self.created_data_items[cache_key] = data_item
        return data_item
    
    def _create_or_get_process(self, name: str, type: str, function_type: str, 
                              description: str = None, source_reference: str = None) -> Process:
        """
        Create or retrieve a process, using cache to avoid duplicates.
        """
        cache_key = name
        
        if cache_key in self.created_processes:
            return self.created_processes[cache_key]
        
        process, created = Process.objects.get_or_create(
            name=name,
            defaults={
                'type': type,
                'function_type': function_type,
                'description': description,
                'source_reference': source_reference
            }
        )
        
        self.created_processes[cache_key] = process
        return process
    
    def _create_relationship(self, process: Process, data_item: DataItem, type: str):
        """
        Create a relationship between a process and data item if it doesn't exist.
        """
        Relationship.objects.get_or_create(
            process=process,
            data_item=data_item,
            type=type
        )