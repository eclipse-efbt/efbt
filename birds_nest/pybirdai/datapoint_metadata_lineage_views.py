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


from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import ensure_csrf_cookie, csrf_exempt
from django.db import transaction
import json
import os
from django.conf import settings

from pybirdai.models.bird_meta_data_model import COMBINATION
from pybirdai.models.lineage_model import DataItem, Process, Relationship
from pybirdai.context.sdd_context_django import SDDContext
from pybirdai.process_steps.metadata_lineage.metadata_lineage_processor import MetadataLineageProcessor


@ensure_csrf_cookie
def datapoint_metadata_lineage_viewer(request, datapoint_id):
    """
    Main view for displaying metadata lineage visualization for a specific datapoint
    """
    datapoint = get_object_or_404(COMBINATION, combination_id=datapoint_id)
    
    context = {
        'datapoint': datapoint,
        'datapoint_id': datapoint_id,
        'combination_id': datapoint.combination_id,
    }
    return render(request, 'pybirdai/datapoint_metadata_lineage.html', context)


@csrf_exempt
@require_http_methods(["GET", "POST"])
def process_datapoint_metadata_lineage(request, datapoint_id):
    """
    API endpoint to process and fetch metadata lineage data for a specific datapoint
    
    GET: Returns existing metadata lineage if available
    POST: Processes metadata lineage for the datapoint and returns the results
    """
    datapoint = get_object_or_404(COMBINATION, combination_id=datapoint_id)
    
    try:
        if request.method == "POST":
            # Clear existing metadata lineage for this datapoint (optional)
            clear_existing = request.POST.get('clear_existing', 'false').lower() == 'true'
            
            if clear_existing:
                # Clear existing relationships for this datapoint
                _clear_datapoint_metadata_lineage(datapoint)
            
            # Process metadata lineage
            base_dir = settings.BASE_DIR
            sdd_context = SDDContext()
            sdd_context.file_directory = os.path.join(base_dir, 'resources')
            
            processor = MetadataLineageProcessor(sdd_context)
            
            # Process the specific datapoint and export results
            with transaction.atomic():
                _process_single_datapoint(processor, datapoint)
                
                # Get the lineage data for this datapoint (inside transaction to ensure visibility)
                lineage_data = _get_datapoint_lineage_data(datapoint)
            
            # Export results to JSON
            output_path = os.path.join(
                base_dir, 'results', 'metadata_lineage', 
                f'datapoint_{datapoint_id}_lineage.json'
            )
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save to file
            with open(output_path, 'w') as f:
                json.dump(lineage_data, f, indent=2)
            
            return JsonResponse({
                'success': True,
                'message': f'Metadata lineage processed for datapoint {datapoint_id}',
                'output_file': output_path,
                'lineage': lineage_data
            })
        
        else:  # GET request
            # Return existing lineage data
            lineage_data = _get_datapoint_lineage_data(datapoint)
            
            return JsonResponse({
                'success': True,
                'datapoint_id': datapoint_id,
                'lineage': lineage_data
            })
    
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error in process_datapoint_metadata_lineage: {error_details}")
        
        return JsonResponse({
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__,
            'datapoint_id': datapoint_id
        }, status=500)


@require_http_methods(["GET"])
def get_datapoint_metadata_lineage_graph(request, datapoint_id):
    """
    API endpoint to get metadata lineage in a graph format suitable for visualization
    """
    datapoint = get_object_or_404(COMBINATION, combination_id=datapoint_id)
    
    try:
        lineage_data = _get_datapoint_lineage_data(datapoint)
        
        # Convert to graph format for visualization
        nodes = []
        edges = []
        
        # Add data item nodes
        for item in lineage_data['data_items']:
            nodes.append({
                'id': f"data_{item['id']}",
                'label': item['location'],
                'type': item['type'],
                'category': 'data_item',
                'description': item.get('description', '')
            })
        
        # Add process nodes
        for process in lineage_data['processes']:
            nodes.append({
                'id': f"process_{process['id']}",
                'label': process['name'],
                'type': process['type'],
                'function_type': process['function_type'],
                'category': 'process',
                'description': process.get('description', '')
            })
        
        # Add edges from relationships
        for rel in lineage_data['relationships']:
            if rel['type'] == 'consumes':
                edges.append({
                    'source': f"data_{rel['data_item_id']}",
                    'target': f"process_{rel['process_id']}",
                    'type': 'consumes',
                    'label': 'consumes'
                })
            else:  # produces
                edges.append({
                    'source': f"process_{rel['process_id']}",
                    'target': f"data_{rel['data_item_id']}",
                    'type': 'produces',
                    'label': 'produces'
                })
        
        return JsonResponse({
            'success': True,
            'datapoint_id': datapoint_id,
            'graph': {
                'nodes': nodes,
                'edges': edges
            }
        })
    
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error in get_datapoint_metadata_lineage_graph: {error_details}")
        
        return JsonResponse({
            'success': False,
            'error': str(e),
            'datapoint_id': datapoint_id
        }, status=500)


def _process_single_datapoint(processor, datapoint):
    """
    Process complete metadata lineage for a single datapoint (combination)
    
    This traces the full lineage including:
    1. Datapoint aggregation
    2. Product-specific joins
    3. Column-to-column transformations
    4. Source table relationships
    """
    combination = datapoint
    
    print(f"Processing complete metadata lineage for {combination.combination_id}")
    
    # Step 1: Create the datapoint table and metric column
    table_data_item = processor._create_or_get_data_item(
        type='table',
        location=combination.combination_id,
        description=f"Datapoint table for {combination.combination_id}"
    )
    
    metric_column_data_item = None
    if combination.metric:
        metric_column_location = f"{combination.combination_id}.{combination.metric.variable_id}"
        metric_column_data_item = processor._create_or_get_data_item(
            type='column',
            location=metric_column_location,
            description=f"Metric column {combination.metric.variable_id} for datapoint"
        )
    
    # Step 2: Process the complete lineage chain
    processed_tables = set()
    _trace_complete_lineage(processor, combination, metric_column_data_item, processed_tables)


def _trace_complete_lineage(processor, combination, target_column, processed_tables, depth=0):
    """
    Trace the TRANSITIVE metadata lineage for a datapoint.
    
    Start from the specific datapoint and work backwards through actual dependencies:
    1. Start with the related output table for this specific combination
    2. Find cube_links that feed this specific table
    3. Follow column transformations for columns actually used
    4. Recursively trace backwards through the dependency chain
    """
    from pybirdai.models.bird_meta_data_model import (
        COMBINATION_ITEM, CUBE, CUBE_STRUCTURE_ITEM, CUBE_LINK, 
        CUBE_STRUCTURE_ITEM_LINK, MAINTENANCE_AGENCY
    )
    
    indent = "  " * depth
    print(f"{indent}Tracing transitive lineage for {combination.combination_id} at depth {depth}")
    
    # Step 1: Find the SPECIFIC output table for this combination
    # Extract the base table name (F_05_01_REF_FINREP_3_0_152589_REF -> F_05_01_REF_FINREP_3_0)
    parts = combination.combination_id.split('_')
    if len(parts) >= 2 and parts[-1] == 'REF':
        combination_base = '_'.join(parts[:-2])
    else:
        combination_base = combination.combination_id
    
    print(f"{indent}Looking for specific output table: {combination_base}")
    
    # Find the specific output table
    try:
        specific_output_cube = CUBE.objects.get(
            cube_id=combination_base,
            cube_type='RC'
        )
        print(f"{indent}Found specific output table: {specific_output_cube.cube_id}")
    except CUBE.DoesNotExist:
        print(f"{indent}No specific output table found for {combination_base}")
        return
    
    if specific_output_cube.cube_id in processed_tables:
        print(f"{indent}Already processed {specific_output_cube.cube_id}")
        return
    processed_tables.add(specific_output_cube.cube_id)
    
    # Step 2: Process ONLY this specific output table
    _trace_single_output_table(processor, specific_output_cube, combination, processed_tables, depth)
    
    # Step 3: Create aggregate_metric and filter processes for the specific combination
    # This should only happen once for the main output table, not during recursion
    if target_column and combination.metric:
        print(f"{indent}Creating aggregate_metric and filter processes for main output table")
        
        # Create aggregate_metric process (column_to_column)
        # This should consume only the metric column from the output table
        metric_col_location = f"{specific_output_cube.cube_id}.{combination.metric.variable_id}"
        metric_col_data_item = processor._create_or_get_data_item(
            type='column',
            location=metric_col_location,
            description=f"Metric column {combination.metric.variable_id} from output table"
        )
        
        agg_process_name = f"Aggregate_{combination.combination_id}"
        agg_process = processor._create_or_get_process(
            name=agg_process_name,
            type='column_to_column',
            function_type='aggregate_metric',
            description=f"Aggregate metric {combination.metric.variable_id} to datapoint"
        )
        
        # Aggregate process consumes the metric column and produces the datapoint metric
        processor._create_relationship(
            process=agg_process,
            data_item=metric_col_data_item,
            type='consumes'
        )
        
        processor._create_relationship(
            process=agg_process,
            data_item=target_column,
            type='produces'
        )
        
        print(f"{indent}Created aggregate_metric process: {combination.metric.variable_id} -> {target_column.location}")
        
        # Create filtering process (columns_to_table) 
        # This should consume the filter fields (combination items) from the output table
        combination_items = COMBINATION_ITEM.objects.filter(
            combination_id=combination
        ).select_related('variable_id')
        
        if combination_items.exists():
            filter_process_name = f"Filter_{combination.combination_id}"
            filter_process = processor._create_or_get_process(
                name=filter_process_name,
                type='columns_to_table',
                function_type='filter',
                description=f"Filter combination items to create output table subset"
            )
            
            # Create output table data item for the filter process to produce
            output_table_item = processor._create_or_get_data_item(
                type='table',
                location=specific_output_cube.cube_id,
                description=f"Filtered output table: {specific_output_cube.cube_id}"
            )
            
            # Filter process consumes the filter fields (combination items) from the output table
            for combo_item in combination_items:
                if combo_item.variable_id:
                    filter_col_location = f"{specific_output_cube.cube_id}.{combo_item.variable_id.variable_id}"
                    filter_col_data_item = processor._create_or_get_data_item(
                        type='column',
                        location=filter_col_location,
                        description=f"Filter field {combo_item.variable_id.variable_id}"
                    )
                    
                    processor._create_relationship(
                        process=filter_process,
                        data_item=filter_col_data_item,
                        type='consumes'
                    )
                    
                    print(f"{indent}Filter consumes: {filter_col_location}")
            
            # Filter process produces the output table (or filtered subset)
            processor._create_relationship(
                process=filter_process,
                data_item=output_table_item,
                type='produces'
            )
            
            print(f"{indent}Created filter process with {combination_items.count()} filter fields")


def _trace_single_output_table(processor, output_cube, combination, processed_tables, depth):
    """
    Trace the lineage for a single specific output table
    FOCUSED approach: Only trace dependencies that are directly relevant to the specific datapoint
    """
    from pybirdai.models.bird_meta_data_model import (
        COMBINATION_ITEM, CUBE, CUBE_STRUCTURE_ITEM, CUBE_LINK, 
        CUBE_STRUCTURE_ITEM_LINK, MAINTENANCE_AGENCY
    )
    
    indent = "  " * depth
    print(f"{indent}Processing output table: {output_cube.cube_id}")
    
    # Early termination: If we're at depth > 0 and this is not the main output table,
    # we should be very selective about what we trace
    main_output_table = combination.combination_id.split('_')
    if len(main_output_table) >= 2 and main_output_table[-1] == 'REF':
        main_table_name = '_'.join(main_output_table[:-2])
    else:
        main_table_name = combination.combination_id
    
    # If this is not the main output table and we're deep in recursion, limit processing
    if depth > 0 and output_cube.cube_id != main_table_name:
        # Only process this table if it has columns that are directly used in our combination
        combination_items = COMBINATION_ITEM.objects.filter(
            combination_id=combination
        ).select_related('variable_id')
        
        used_columns = set()
        for combo_item in combination_items:
            if combo_item.variable_id:
                used_columns.add(combo_item.variable_id.variable_id)
        
        # Check if this table actually has any of the columns we need
        if output_cube.cube_structure_id:
            relevant_columns = CUBE_STRUCTURE_ITEM.objects.filter(
                cube_structure_id=output_cube.cube_structure_id,
                variable_id__variable_id__in=used_columns
            ).count()
            
            if relevant_columns == 0:
                print(f"{indent}Skipping {output_cube.cube_id} - no relevant columns for this datapoint")
                return
    
    # Create output table data item
    output_table_item = processor._create_or_get_data_item(
        type='table',
        location=output_cube.cube_id,
        description=f"Output table: {output_cube.cube_id}"
    )
    
    # Step 1: Process ONLY the columns that are used in this specific combination
    combination_items = COMBINATION_ITEM.objects.filter(
        combination_id=combination
    ).select_related('variable_id')
    
    used_columns = set()
    for combo_item in combination_items:
        if combo_item.variable_id:
            used_columns.add(combo_item.variable_id.variable_id)
    
    print(f"{indent}Combination uses {len(used_columns)} columns: {list(used_columns)[:5]}...")
    
    # Process columns in output table, but ONLY for columns used in the combination
    if output_cube.cube_structure_id:
        output_columns = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id=output_cube.cube_structure_id,
            variable_id__variable_id__in=used_columns  # Only process columns used in the combination
        ).select_related('variable_id')
        
        print(f"{indent}Found {output_columns.count()} relevant columns in {output_cube.cube_id}")
        
        for col_item in output_columns:
            if not col_item.variable_id:
                continue
                
            # Create output column data item
            output_col_location = f"{output_cube.cube_id}.{col_item.variable_id.variable_id}"
            output_col_item = processor._create_or_get_data_item(
                type='column',
                location=output_col_location,
                description=f"Output column {col_item.variable_id.variable_id}"
            )
            
            # Check for column-to-column transformations for THIS specific column
            col_links = CUBE_STRUCTURE_ITEM_LINK.objects.filter(
                primary_cube_variable_code=col_item
            )
            
            for col_link in col_links:
                if col_link.foreign_cube_variable_code and col_link.foreign_cube_variable_code.variable_id:
                    # Create source column data item  
                    foreign_var_id = col_link.foreign_cube_variable_code.variable_id.variable_id
                    target_var_id = col_item.variable_id.variable_id
                    source_col_location = f"{col_link.cube_link_id.foreign_cube_id.cube_id}.{foreign_var_id}"
                    source_col_item = processor._create_or_get_data_item(
                        type='column',
                        location=source_col_location,
                        description=f"Source column {foreign_var_id}"
                    )
                    
                    # Create column-to-column transformation process
                    transform_process_name = f"Transform_{foreign_var_id}_to_{target_var_id}"
                    transform_process = processor._create_or_get_process(
                        name=transform_process_name,
                        type='column_to_column',
                        function_type='basic',
                        description=f"Column transformation from {foreign_var_id} to {target_var_id}"
                    )
                    
                    # Create relationships
                    processor._create_relationship(
                        process=transform_process,
                        data_item=source_col_item,
                        type='consumes'
                    )
                    processor._create_relationship(
                        process=transform_process,
                        data_item=output_col_item,
                        type='produces'
                    )
                    
                    print(f"{indent}Created column transformation: {foreign_var_id} -> {target_var_id}")
                    
                    # Recursively trace the source table if not already processed
                    # But only if we're not too deep and the source is relevant
                    source_cube = col_link.cube_link_id.foreign_cube_id
                    if source_cube.cube_id not in processed_tables and depth < 2:  # Reduced depth limit
                        _trace_single_output_table(processor, source_cube, combination, processed_tables, depth + 1)
    
    # Step 2: Process cube_links, but ONLY for the main output table (depth 0)
    if depth == 0:  # Only process cube links for the main output table
        cube_links_as_primary = CUBE_LINK.objects.filter(primary_cube_id=output_cube)
        cube_links_as_foreign = CUBE_LINK.objects.filter(foreign_cube_id=output_cube)
        
        print(f"{indent}Cube links check for MAIN table {output_cube.cube_id}:")
        print(f"{indent}  As primary_cube: {cube_links_as_primary.count()} links")
        print(f"{indent}  As foreign_cube: {cube_links_as_foreign.count()} links")
        
        # Use whichever direction has results
        if cube_links_as_foreign.exists():
            cube_links = cube_links_as_foreign
            print(f"{indent}Using foreign_cube direction: {cube_links.count()} cube links")
        elif cube_links_as_primary.exists():
            cube_links = cube_links_as_primary
            print(f"{indent}Using primary_cube direction: {cube_links.count()} cube links")
        else:
            cube_links = CUBE_LINK.objects.none()
            print(f"{indent}No cube links found in either direction")
        
        if cube_links.exists():
            print(f"{indent}Processing {cube_links.count()} cube links for MAIN table {output_cube.cube_id}")
            
            # Group by product name (extracted from cube_link_id)
            # cube_link_id format: F_05_01_REF_FINREP_3_0:INSTRMNT:Other loans
            product_groups = {}
            for link in cube_links:
                # Extract product name from cube_link_id
                cube_link_parts = link.cube_link_id.split(':')
                if len(cube_link_parts) >= 3:
                    product_name = cube_link_parts[2]  # "Other loans"
                else:
                    product_name = 'DEFAULT'
                
                # Determine the source cube based on direction
                source_cube = None
                if hasattr(link, 'foreign_cube_id') and link.foreign_cube_id and link.foreign_cube_id != output_cube:
                    source_cube = link.foreign_cube_id
                elif hasattr(link, 'primary_cube_id') and link.primary_cube_id and link.primary_cube_id != output_cube:
                    source_cube = link.primary_cube_id
                
                if source_cube and (not source_cube.cube_type or source_cube.cube_type != 'RC'):
                    if product_name not in product_groups:
                        product_groups[product_name] = []
                    product_groups[product_name].append((link, source_cube))
                    print(f"{indent}Added link to product group '{product_name}': {source_cube.cube_id} -> {output_cube.cube_id}")
            
            # Process each product group according to spec: {output_layer_name}.{product_name}
            for product_name, link_source_pairs in product_groups.items():
                # Create product-specific join table: {output_layer_name}.{product_name}
                join_table_name = f"{output_cube.cube_id}.{product_name}"
                join_table_item = processor._create_or_get_data_item(
                    type='table',
                    location=join_table_name,
                    description=f"Product-specific join table for {product_name}"
                )
                print(f"{indent}Created product-specific table: {join_table_name}")
                
                # Create join process for this product
                join_process_name = f"Join_{output_cube.cube_id}_{product_name.replace(' ', '_')}"
                join_process = processor._create_or_get_process(
                    name=join_process_name,
                    type='table_to_table',
                    function_type='join',
                    description=f"Product-specific join for {product_name}"
                )
                
                # Process source tables in this product group
                source_tables = set(source_cube for _, source_cube in link_source_pairs)
                
                for source_cube in source_tables:
                    # Create source table data item
                    source_table_item = processor._create_or_get_data_item(
                        type='table',
                        location=source_cube.cube_id,
                        description=f"Input table: {source_cube.cube_id}"
                    )
                    
                    # Join process consumes from input tables
                    processor._create_relationship(
                        process=join_process,
                        data_item=source_table_item,
                        type='consumes'
                    )
                    
                    print(f"{indent}Join consumes INPUT table: {source_cube.cube_id}")
                    
                    # Create column-level relationships for relevant columns
                    if source_cube.cube_structure_id:
                        input_columns = CUBE_STRUCTURE_ITEM.objects.filter(
                            cube_structure_id=source_cube.cube_structure_id,
                            variable_id__variable_id__in=used_columns
                        ).select_related('variable_id')
                        
                        for input_col in input_columns:
                            if input_col.variable_id:
                                # Create input column data item
                                input_col_location = f"{source_cube.cube_id}.{input_col.variable_id.variable_id}"
                                input_col_item = processor._create_or_get_data_item(
                                    type='column',
                                    location=input_col_location,
                                    description=f"Input column {input_col.variable_id.variable_id}"
                                )
                                
                                # Create column transformation process
                                col_transform_name = f"Transform_{source_cube.cube_id}_{input_col.variable_id.variable_id}_to_{product_name.replace(' ', '_')}"
                                col_transform_process = processor._create_or_get_process(
                                    name=col_transform_name,
                                    type='column_to_column',
                                    function_type='basic',
                                    description=f"Transform column {input_col.variable_id.variable_id} for {product_name}"
                                )
                                
                                # Input column is CONSUMED (not produced)
                                processor._create_relationship(
                                    process=col_transform_process,
                                    data_item=input_col_item,
                                    type='consumes'
                                )
                                
                                # Create corresponding output column in join table
                                join_col_location = f"{join_table_name}.{input_col.variable_id.variable_id}"
                                join_col_item = processor._create_or_get_data_item(
                                    type='column',
                                    location=join_col_location,
                                    description=f"Join table column {input_col.variable_id.variable_id}"
                                )
                                
                                # Join table column is PRODUCED
                                processor._create_relationship(
                                    process=col_transform_process,
                                    data_item=join_col_item,
                                    type='produces'
                                )
                                
                                print(f"{indent}  Column transform: {input_col_location} -> {join_col_location}")
                
                # Join process produces the product-specific join table
                processor._create_relationship(
                    process=join_process,
                    data_item=join_table_item,
                    type='produces'
                )
                
                # Create process from join table to output table
                copy_process_name = f"Copy_{product_name.replace(' ', '_')}_to_{output_cube.cube_id}"
                copy_process = processor._create_or_get_process(
                    name=copy_process_name,
                    type='table_to_table',
                    function_type='basic',
                    description=f"Copy from {product_name} join table to output table"
                )
                
                # Copy process consumes from join table
                processor._create_relationship(
                    process=copy_process,
                    data_item=join_table_item,
                    type='consumes'
                )
                # Copy process produces the output table
                processor._create_relationship(
                    process=copy_process,
                    data_item=output_table_item,
                    type='produces'
                )
                
                # SPEC REQUIREMENT: Create column-to-column processes from join table columns to output table columns
                # "For each column in the related output layer: Create corresponding column data items in the join table
                # Create processes that: Consume from product-specific join table, Produce to output layer table"
                
                # Get all relevant columns in the output table for this product
                if output_cube.cube_structure_id:
                    output_columns_for_product = CUBE_STRUCTURE_ITEM.objects.filter(
                        cube_structure_id=output_cube.cube_structure_id,
                        variable_id__variable_id__in=used_columns  # Only process columns used in the combination
                    ).select_related('variable_id')
                    
                    for output_col in output_columns_for_product:
                        if output_col.variable_id:
                            # Create output table column data item
                            output_col_location = f"{output_cube.cube_id}.{output_col.variable_id.variable_id}"
                            output_col_item = processor._create_or_get_data_item(
                                type='column',
                                location=output_col_location,
                                description=f"Output column {output_col.variable_id.variable_id}"
                            )
                            
                            # Create corresponding join table column data item
                            join_col_location = f"{join_table_name}.{output_col.variable_id.variable_id}"
                            join_col_item = processor._create_or_get_data_item(
                                type='column',
                                location=join_col_location,
                                description=f"Join table column {output_col.variable_id.variable_id} for {product_name}"
                            )
                            
                            # Create column-to-column process from join table to output table
                            col_copy_process_name = f"Copy_{product_name.replace(' ', '_')}_{output_col.variable_id.variable_id}_to_output"
                            col_copy_process = processor._create_or_get_process(
                                name=col_copy_process_name,
                                type='column_to_column',
                                function_type='basic',
                                description=f"Copy column {output_col.variable_id.variable_id} from {product_name} join table to output table"
                            )
                            
                            # Column copy process consumes from join table column
                            processor._create_relationship(
                                process=col_copy_process,
                                data_item=join_col_item,
                                type='consumes'
                            )
                            
                            # Column copy process produces the output table column
                            processor._create_relationship(
                                process=col_copy_process,
                                data_item=output_col_item,
                                type='produces'
                            )
                            
                            print(f"{indent}  Created column copy: {join_col_location} -> {output_col_location}")
    else:
        print(f"{indent}Skipping cube links processing for depth {depth} table {output_cube.cube_id}")


def _get_datapoint_lineage_data(datapoint):
    """
    Get metadata lineage data related to a specific datapoint (combination)
    FOCUSED approach: Only return lineage items that are part of the transitive chain for this datapoint
    """
    combination_id = datapoint.combination_id
    
    # Extract the base table name for focused filtering
    parts = combination_id.split('_')
    if len(parts) >= 2 and parts[-1] == 'REF':
        base_table_name = '_'.join(parts[:-2])
    else:
        base_table_name = combination_id
    
    print(f"Getting focused lineage for {combination_id}, base table: {base_table_name}")
    
    from django.db.models import Q
    
    # Start with processes directly related to this specific combination
    direct_processes = Process.objects.filter(
        name__contains=combination_id
    )
    print(f"Found {direct_processes.count()} direct processes")
    
    # Also include processes related to the base output table and product-specific joins
    base_related_processes = Process.objects.filter(
        name__contains=base_table_name
    ).exclude(
        name__contains=combination_id  # Don't double-count
    )
    print(f"Found {base_related_processes.count()} base table processes")
    
    # Also include column-to-column copy processes for product-specific joins
    # These have names like: Copy_Other_loans_TYP_CLLTRL_to_output
    column_copy_processes = Process.objects.filter(
        Q(name__startswith="Copy_Other_loans_") |
        Q(name__startswith="Copy_Non_Negotiable_bonds_") |
        Q(name__startswith="Copy_Advances_that_are_not_loans_") |
        Q(name__startswith="Copy_Trade_receivables_") |
        Q(name__startswith="Copy_On_demand_and_short_notice_") |
        Q(name__startswith="Copy_Finance_leases_") |
        Q(name__startswith="Copy_Reverse_repurchase_agreements_") |
        Q(name__startswith="Copy_Credit_card_debt_")
    ).filter(
        name__endswith="_to_output",
        type='column_to_column'
    )
    print(f"Found {column_copy_processes.count()} column copy processes")
    
    # Also include transform processes from input tables to product-specific joins
    # These have names like: Transform_PRTY_INSTTTNL_SCTR_to_Other_loans
    transform_to_join_processes = Process.objects.filter(
        Q(name__contains="to_Other_loans") |
        Q(name__contains="to_Non_Negotiable_bonds") |
        Q(name__contains="to_Advances_that_are_not_loans") |
        Q(name__contains="to_Trade_receivables") |
        Q(name__contains="to_On_demand_and_short_notice") |
        Q(name__contains="to_Finance_leases") |
        Q(name__contains="to_Reverse_repurchase_agreements") |
        Q(name__contains="to_Credit_card_debt")
    ).filter(
        name__startswith="Transform_",
        type='column_to_column'
    )
    print(f"Found {transform_to_join_processes.count()} transform to join processes")
    
    # Combine all relevant processes using distinct() instead of union()
    all_relevant_processes = Process.objects.filter(
        Q(name__contains=combination_id) | 
        Q(name__contains=base_table_name) |
        Q(id__in=column_copy_processes.values_list('id', flat=True)) |
        Q(id__in=transform_to_join_processes.values_list('id', flat=True))
    ).distinct()
    
    # Get all relationships for these focused processes
    focused_relationships = Relationship.objects.filter(
        process__in=all_relevant_processes
    ).select_related('process', 'data_item')
    
    # Get all data items connected to focused processes
    focused_data_item_ids = set(rel.data_item_id for rel in focused_relationships)
    focused_data_items = DataItem.objects.filter(id__in=focused_data_item_ids)
    
    print(f"Focused approach:")
    print(f"  Processes: {all_relevant_processes.count()}")
    print(f"  Data items: {focused_data_items.count()}")
    print(f"  Relationships: {focused_relationships.count()}")
    
    # Filter out unrelated output tables - only include tables that are:
    # 1. The specific datapoint table (combination_id)
    # 2. The main output table (base_table_name)
    # 3. Product-specific tables (base_table_name.ProductName)
    # 4. Input layer tables (no specific filtering needed for these)
    filtered_data_items = []
    for item in focused_data_items:
        location = item.location
        
        # Always include datapoint-specific items
        if combination_id in location:
            filtered_data_items.append(item)
            continue
            
        # Include the main output table
        if location == base_table_name:
            filtered_data_items.append(item)
            continue
            
        # Include product-specific tables and their columns (format: base_table_name.ProductName or base_table_name.ProductName.ColumnName)
        if location.startswith(f"{base_table_name}.") and not any(
            other_table in location for other_table in [
                'F_01_', 'F_04_', 'F_06_', 'F_07_', 'F_08_', 'F_09_', 'F_13_', 'F_14_', 'F_15_',
                'F_18_', 'F_19_', 'F_20_', 'F_21_', 'F_31_', 'F_40_', 'F_41_', 'F_42_'
            ] if other_table != base_table_name[:5]  # Allow tables with same prefix
        ):
            filtered_data_items.append(item)
            continue
            
        # Include input layer tables (anything that doesn't look like a report table)
        if not location.startswith('F_') or '.' in location:
            # This is likely an input table or column from input/product tables
            filtered_data_items.append(item)
            continue
            
        # Exclude other output tables that aren't related to this datapoint
        print(f"Filtering out unrelated table: {location}")
    
    print(f"After filtering: {len(filtered_data_items)} data items")
    
    # Build the response with filtered data
    return {
        'data_items': [
            {
                'id': item.id,
                'type': item.type,
                'location': item.location,
                'description': item.description
            }
            for item in filtered_data_items
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
            for process in all_relevant_processes
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
            for rel in focused_relationships
            # Only include relationships where both ends are in our filtered data items
            if rel.data_item_id in [item.id for item in filtered_data_items]
        ]
    }


def _clear_datapoint_metadata_lineage(datapoint):
    """
    Clear existing metadata lineage for a datapoint (combination)
    """
    combination_id = datapoint.combination_id
    
    # Find processes related to this combination
    processes = Process.objects.filter(
        name__contains=combination_id
    )
    
    # Delete relationships first
    Relationship.objects.filter(process__in=processes).delete()
    
    # Delete processes
    processes.delete()
    
    # Delete data items related to this datapoint
    DataItem.objects.filter(
        location__startswith=combination_id
    ).delete()