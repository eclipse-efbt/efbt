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

"""
BPMN Metadata Lineage Processor

This processor creates metadata lineage using BPMN_lite models instead of the Process/DataItem models.
It follows BPMN standards with minor modifications:

- UserTask: Represents input data items that are consumed
- ServiceTask: Represents output data items that are produced  
- SequenceFlow: Represents the flow/process connecting tasks (similar to Process in metadata lineage)
- SubProcess: Container for related workflow elements (e.g., datapoint processing workflow)

The processor creates BPMN workflows that show the metadata lineage from input tables through
product-specific joins to output tables, following the same logic as the original metadata
lineage processor but storing results in BPMN_lite format.
"""

from django.db import transaction
from pybirdai.models.bpmn_lite_models import (
    UserTask, ServiceTask, SequenceFlow, SubProcess, 
    SubProcessFlowElement, WorkflowModule
)


class BPMNMetadataLineageProcessor:
    """
    Processes metadata lineage and stores it using BPMN_lite models.
    
    Creates BPMN workflows where:
    - UserTask = Input data items (tables/columns that are consumed)
    - ServiceTask = Output data items (tables/columns that are produced)
    - SequenceFlow = Transformation processes connecting input to output
    - SubProcess = Container for the complete datapoint workflow
    """
    
    def __init__(self, sdd_context):
        self.sdd_context = sdd_context
        self.created_tasks = {}  # Cache for created tasks
        self.created_flows = {}  # Cache for created flows
        
    def process_datapoint_metadata_lineage(self, datapoint):
        """
        Create BPMN workflow for a specific datapoint's metadata lineage.
        
        Args:
            datapoint: COMBINATION object representing the datapoint
        """
        print(f"Creating BPMN workflow for datapoint: {datapoint.combination_id}")
        
        # Create main SubProcess to contain the workflow
        workflow_subprocess = self._create_or_get_subprocess(
            id=f"workflow_{datapoint.combination_id}",
            name=f"Metadata Lineage Workflow: {datapoint.combination_id}",
            description=f"Complete metadata lineage workflow for datapoint {datapoint.combination_id}"
        )
        
        # Create workflow module to organize the workflow
        workflow_module = self._create_or_get_workflow_module(
            module_id=f"module_{datapoint.combination_id}",
            module_name=f"Datapoint {datapoint.combination_id} Lineage Module"
        )
        
        # Process the complete lineage chain similar to the original processor
        self._process_complete_bpmn_lineage(workflow_subprocess, datapoint)
        
        return workflow_subprocess
    
    def _create_or_get_subprocess(self, id, name, description):
        """Create or get existing SubProcess."""
        subprocess, created = SubProcess.objects.get_or_create(
            id=id,
            defaults={
                'name': name,
                'description': description
            }
        )
        if created:
            print(f"Created SubProcess: {name}")
        return subprocess
    
    def _create_or_get_workflow_module(self, module_id, module_name):
        """Create or get existing WorkflowModule."""
        module, created = WorkflowModule.objects.get_or_create(
            module_id=module_id,
            defaults={
                'module_name': module_name
            }
        )
        if created:
            print(f"Created WorkflowModule: {module_name}")
        return module
    
    def _create_or_get_user_task(self, id, name, description, entity_reference=None):
        """
        Create or get UserTask (represents input/consumed data items).
        
        Args:
            id: Unique identifier for the task
            name: Task name
            description: Task description
            entity_reference: Reference to the data entity (table/column location)
        """
        cache_key = f"user_{id}"
        if cache_key in self.created_tasks:
            return self.created_tasks[cache_key]
        
        task, created = UserTask.objects.get_or_create(
            id=id,
            defaults={
                'name': name,
                'description': description,
                'entity_reference': entity_reference
            }
        )
        
        if created:
            print(f"Created UserTask (input): {name}")
        
        self.created_tasks[cache_key] = task
        return task
    
    def _create_or_get_service_task(self, id, name, description, enriched_attribute_reference=None):
        """
        Create or get ServiceTask (represents output/produced data items).
        
        Args:
            id: Unique identifier for the task
            name: Task name
            description: Task description
            enriched_attribute_reference: Reference to the produced data entity
        """
        cache_key = f"service_{id}"
        if cache_key in self.created_tasks:
            return self.created_tasks[cache_key]
        
        task, created = ServiceTask.objects.get_or_create(
            id=id,
            defaults={
                'name': name,
                'description': description,
                'enriched_attribute_reference': enriched_attribute_reference
            }
        )
        
        if created:
            print(f"Created ServiceTask (output): {name}")
        
        self.created_tasks[cache_key] = task
        return task
    
    def _create_or_get_sequence_flow(self, id, name, source_task, target_task, description=None):
        """
        Create or get SequenceFlow (represents transformation process).
        
        Args:
            id: Unique identifier for the flow
            name: Flow name
            source_task: Task that produces the data (UserTask or ServiceTask)
            target_task: Task that consumes the data (UserTask or ServiceTask)
            description: Flow description
        """
        cache_key = f"flow_{id}"
        if cache_key in self.created_flows:
            return self.created_flows[cache_key]
        
        flow, created = SequenceFlow.objects.get_or_create(
            id=id,
            defaults={
                'name': name,
                'description': description,
                'source_ref': source_task,
                'target_ref': target_task
            }
        )
        
        if created:
            print(f"Created SequenceFlow: {source_task.name} -> {target_task.name}")
        
        self.created_flows[cache_key] = flow
        return flow
    
    def _add_element_to_subprocess(self, subprocess, element, element_type):
        """Add a flow element to a subprocess."""
        SubProcessFlowElement.objects.get_or_create(
            sub_process=subprocess,
            flow_element_type=element_type,
            flow_element_id=element.id
        )
    
    def _process_complete_bpmn_lineage(self, workflow_subprocess, datapoint):
        """
        Process complete BPMN lineage similar to the original processor.
        
        This follows the same logic as _trace_complete_lineage but creates BPMN elements.
        """
        from pybirdai.models.bird_meta_data_model import (
            COMBINATION_ITEM, CUBE, CUBE_STRUCTURE_ITEM, CUBE_LINK, 
            CUBE_STRUCTURE_ITEM_LINK
        )
        
        # Step 1: Create main datapoint ServiceTask (output)
        datapoint_service_task = self._create_or_get_service_task(
            id=f"datapoint_{datapoint.combination_id}",
            name=f"Datapoint: {datapoint.combination_id}",
            description=f"Final datapoint result for {datapoint.combination_id}",
            enriched_attribute_reference=datapoint.combination_id
        )
        
        self._add_element_to_subprocess(workflow_subprocess, datapoint_service_task, 'ServiceTask')
        
        # Step 2: Create metric column ServiceTask if exists
        if datapoint.metric:
            metric_service_task = self._create_or_get_service_task(
                id=f"metric_{datapoint.combination_id}_{datapoint.metric.variable_id}",
                name=f"Metric: {datapoint.metric.variable_id}",
                description=f"Metric column {datapoint.metric.variable_id} for datapoint",
                enriched_attribute_reference=f"{datapoint.combination_id}.{datapoint.metric.variable_id}"
            )
            
            self._add_element_to_subprocess(workflow_subprocess, metric_service_task, 'ServiceTask')
            
            # Create flow from datapoint to metric
            metric_flow = self._create_or_get_sequence_flow(
                id=f"flow_datapoint_to_metric_{datapoint.combination_id}",
                name="Datapoint to Metric",
                source_task=datapoint_service_task,
                target_task=metric_service_task,
                description="Flow from datapoint to metric column"
            )
            
            self._add_element_to_subprocess(workflow_subprocess, metric_flow, 'SequenceFlow')
        
        # Step 3: Process output tables and their lineage
        self._trace_bpmn_output_table(workflow_subprocess, datapoint)
    
    def _trace_bpmn_output_table(self, workflow_subprocess, datapoint):
        """
        Trace BPMN lineage for output tables, similar to _trace_single_output_table.
        """
        from pybirdai.models.bird_meta_data_model import (
            COMBINATION_ITEM, CUBE, CUBE_STRUCTURE_ITEM, CUBE_LINK, 
            CUBE_STRUCTURE_ITEM_LINK
        )
        
        # Find the main output table for this datapoint
        parts = datapoint.combination_id.split('_')
        if len(parts) >= 2 and parts[-1] == 'REF':
            base_table_name = '_'.join(parts[:-2])
        else:
            base_table_name = datapoint.combination_id
        
        try:
            output_cube = CUBE.objects.get(cube_id=base_table_name)
        except CUBE.DoesNotExist:
            print(f"Output cube not found: {base_table_name}")
            return
        
        print(f"Processing BPMN lineage for output table: {output_cube.cube_id}")
        
        # Create ServiceTask for output table
        output_table_service_task = self._create_or_get_service_task(
            id=f"output_table_{output_cube.cube_id}",
            name=f"Output Table: {output_cube.cube_id}",
            description=f"Output table {output_cube.cube_id}",
            enriched_attribute_reference=output_cube.cube_id
        )
        
        self._add_element_to_subprocess(workflow_subprocess, output_table_service_task, 'ServiceTask')
        
        # Get columns used in this combination
        combination_items = COMBINATION_ITEM.objects.filter(
            combination_id=datapoint
        ).select_related('variable_id')
        
        used_columns = set()
        for combo_item in combination_items:
            if combo_item.variable_id:
                used_columns.add(combo_item.variable_id.variable_id)
        
        print(f"Processing {len(used_columns)} columns for BPMN lineage")
        
        # Process columns in output table
        if output_cube.cube_structure_id:
            output_columns = CUBE_STRUCTURE_ITEM.objects.filter(
                cube_structure_id=output_cube.cube_structure_id,
                variable_id__variable_id__in=used_columns
            ).select_related('variable_id')
            
            for col_item in output_columns:
                if not col_item.variable_id:
                    continue
                
                # Create ServiceTask for output column
                output_col_service_task = self._create_or_get_service_task(
                    id=f"output_col_{output_cube.cube_id}_{col_item.variable_id.variable_id}",
                    name=f"Output Column: {col_item.variable_id.variable_id}",
                    description=f"Output column {col_item.variable_id.variable_id} in {output_cube.cube_id}",
                    enriched_attribute_reference=f"{output_cube.cube_id}.{col_item.variable_id.variable_id}"
                )
                
                self._add_element_to_subprocess(workflow_subprocess, output_col_service_task, 'ServiceTask')
        
        # Process cube links (product-specific joins)
        cube_links = CUBE_LINK.objects.filter(foreign_cube_id=output_cube)
        
        if cube_links.exists():
            print(f"Processing {cube_links.count()} cube links for BPMN workflows")
            
            # Group by product name
            product_groups = {}
            for link in cube_links:
                cube_link_parts = link.cube_link_id.split(':')
                if len(cube_link_parts) >= 3:
                    product_name = cube_link_parts[2]
                else:
                    product_name = 'DEFAULT'
                
                # Determine source cube
                source_cube = None
                if hasattr(link, 'primary_cube_id') and link.primary_cube_id and link.primary_cube_id != output_cube:
                    source_cube = link.primary_cube_id
                
                if source_cube and (not source_cube.cube_type or source_cube.cube_type != 'RC'):
                    if product_name not in product_groups:
                        product_groups[product_name] = []
                    product_groups[product_name].append((link, source_cube))
            
            # Process each product group
            for product_name, link_source_pairs in product_groups.items():
                self._process_bpmn_product_group(
                    workflow_subprocess, output_cube, product_name, 
                    link_source_pairs, used_columns
                )
    
    def _process_bpmn_product_group(self, workflow_subprocess, output_cube, product_name, 
                                   link_source_pairs, used_columns):
        """
        Process a product-specific group and create corresponding BPMN elements.
        """
        from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE_ITEM
        
        print(f"Creating BPMN workflow for product group: {product_name}")
        
        # Create ServiceTask for product-specific join table
        join_table_name = f"{output_cube.cube_id}.{product_name}"
        join_table_service_task = self._create_or_get_service_task(
            id=f"join_table_{output_cube.cube_id}_{product_name.replace(' ', '_')}",
            name=f"Join Table: {product_name}",
            description=f"Product-specific join table for {product_name}",
            enriched_attribute_reference=join_table_name
        )
        
        self._add_element_to_subprocess(workflow_subprocess, join_table_service_task, 'ServiceTask')
        
        # Process source tables and create UserTasks for input data
        source_tables = set(source_cube for _, source_cube in link_source_pairs)
        
        for source_cube in source_tables:
            # Create UserTask for input table (consumed data)
            source_table_user_task = self._create_or_get_user_task(
                id=f"input_table_{source_cube.cube_id}",
                name=f"Input Table: {source_cube.cube_id}",
                description=f"Input table {source_cube.cube_id}",
                entity_reference=source_cube.cube_id
            )
            
            self._add_element_to_subprocess(workflow_subprocess, source_table_user_task, 'UserTask')
            
            # Create SequenceFlow from input table to join table
            table_join_flow = self._create_or_get_sequence_flow(
                id=f"flow_{source_cube.cube_id}_to_join_{product_name.replace(' ', '_')}",
                name=f"Join {source_cube.cube_id} for {product_name}",
                source_task=source_table_user_task,
                target_task=join_table_service_task,
                description=f"Join process from {source_cube.cube_id} for product {product_name}"
            )
            
            self._add_element_to_subprocess(workflow_subprocess, table_join_flow, 'SequenceFlow')
            
            # Create UserTasks for input columns and flows for column transformations
            if source_cube.cube_structure_id:
                input_columns = CUBE_STRUCTURE_ITEM.objects.filter(
                    cube_structure_id=source_cube.cube_structure_id,
                    variable_id__variable_id__in=used_columns
                ).select_related('variable_id')
                
                for input_col in input_columns:
                    if input_col.variable_id:
                        # Create UserTask for input column
                        input_col_user_task = self._create_or_get_user_task(
                            id=f"input_col_{source_cube.cube_id}_{input_col.variable_id.variable_id}",
                            name=f"Input Column: {input_col.variable_id.variable_id}",
                            description=f"Input column {input_col.variable_id.variable_id} from {source_cube.cube_id}",
                            entity_reference=f"{source_cube.cube_id}.{input_col.variable_id.variable_id}"
                        )
                        
                        self._add_element_to_subprocess(workflow_subprocess, input_col_user_task, 'UserTask')
                        
                        # Create ServiceTask for join table column
                        join_col_service_task = self._create_or_get_service_task(
                            id=f"join_col_{product_name.replace(' ', '_')}_{input_col.variable_id.variable_id}",
                            name=f"Join Column: {input_col.variable_id.variable_id}",
                            description=f"Join table column {input_col.variable_id.variable_id} for {product_name}",
                            enriched_attribute_reference=f"{join_table_name}.{input_col.variable_id.variable_id}"
                        )
                        
                        self._add_element_to_subprocess(workflow_subprocess, join_col_service_task, 'ServiceTask')
                        
                        # Create SequenceFlow for column transformation
                        col_transform_flow = self._create_or_get_sequence_flow(
                            id=f"flow_transform_{source_cube.cube_id}_{input_col.variable_id.variable_id}_to_{product_name.replace(' ', '_')}",
                            name=f"Transform {input_col.variable_id.variable_id}",
                            source_task=input_col_user_task,
                            target_task=join_col_service_task,
                            description=f"Transform column {input_col.variable_id.variable_id} for {product_name}"
                        )
                        
                        self._add_element_to_subprocess(workflow_subprocess, col_transform_flow, 'SequenceFlow')
        
        # Create SequenceFlow from join table to output table
        join_to_output_flow = self._create_or_get_sequence_flow(
            id=f"flow_join_{product_name.replace(' ', '_')}_to_output",
            name=f"Copy {product_name} to Output",
            source_task=join_table_service_task,
            target_task=self._get_or_create_output_table_task(output_cube),
            description=f"Copy from {product_name} join table to output table"
        )
        
        self._add_element_to_subprocess(workflow_subprocess, join_to_output_flow, 'SequenceFlow')
        
        # Create column-to-column flows from join to output
        self._create_bpmn_column_copy_flows(
            workflow_subprocess, output_cube, product_name, used_columns
        )
    
    def _get_or_create_output_table_task(self, output_cube):
        """Get or create ServiceTask for output table."""
        return self._create_or_get_service_task(
            id=f"output_table_{output_cube.cube_id}",
            name=f"Output Table: {output_cube.cube_id}",
            description=f"Output table {output_cube.cube_id}",
            enriched_attribute_reference=output_cube.cube_id
        )
    
    def _create_bpmn_column_copy_flows(self, workflow_subprocess, output_cube, product_name, used_columns):
        """
        Create column-to-column copy flows from join table columns to output table columns.
        """
        from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE_ITEM
        
        if not output_cube.cube_structure_id:
            return
        
        join_table_name = f"{output_cube.cube_id}.{product_name}"
        
        output_columns = CUBE_STRUCTURE_ITEM.objects.filter(
            cube_structure_id=output_cube.cube_structure_id,
            variable_id__variable_id__in=used_columns
        ).select_related('variable_id')
        
        for output_col in output_columns:
            if output_col.variable_id:
                # Create/get ServiceTask for join table column
                join_col_service_task = self._create_or_get_service_task(
                    id=f"join_col_{product_name.replace(' ', '_')}_{output_col.variable_id.variable_id}",
                    name=f"Join Column: {output_col.variable_id.variable_id}",
                    description=f"Join table column {output_col.variable_id.variable_id} for {product_name}",
                    enriched_attribute_reference=f"{join_table_name}.{output_col.variable_id.variable_id}"
                )
                
                # Create ServiceTask for output column
                output_col_service_task = self._create_or_get_service_task(
                    id=f"output_col_{output_cube.cube_id}_{output_col.variable_id.variable_id}",
                    name=f"Output Column: {output_col.variable_id.variable_id}",
                    description=f"Output column {output_col.variable_id.variable_id}",
                    enriched_attribute_reference=f"{output_cube.cube_id}.{output_col.variable_id.variable_id}"
                )
                
                self._add_element_to_subprocess(workflow_subprocess, output_col_service_task, 'ServiceTask')
                
                # Create SequenceFlow for column copy
                col_copy_flow = self._create_or_get_sequence_flow(
                    id=f"flow_copy_{product_name.replace(' ', '_')}_{output_col.variable_id.variable_id}_to_output",
                    name=f"Copy {output_col.variable_id.variable_id} to Output",
                    source_task=join_col_service_task,
                    target_task=output_col_service_task,
                    description=f"Copy column {output_col.variable_id.variable_id} from {product_name} join table to output table"
                )
                
                self._add_element_to_subprocess(workflow_subprocess, col_copy_flow, 'SequenceFlow')
                
                print(f"Created BPMN column copy flow: {join_table_name}.{output_col.variable_id.variable_id} -> {output_cube.cube_id}.{output_col.variable_id.variable_id}")