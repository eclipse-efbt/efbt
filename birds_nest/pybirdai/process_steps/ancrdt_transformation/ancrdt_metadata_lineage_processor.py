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
ANACREDIT Metadata Lineage Processor

This processor creates metadata lineage for ANACREDIT's dataset-based structure
using BPMN_lite models. Unlike template-based frameworks (FINREP/COREP),
ANACREDIT has predefined output tables and uses join configuration CSVs.

The processor traces lineage from:
- Input Layer Cubes (ILC) - source tables
- Through CUBE_LINK relationships (joins)
- To Report Output Layer Cubes (ROLC) - output tables

BPMN Elements:
- UserTask: Represents input data items (ILC tables/columns)
- ServiceTask: Represents output data items (ROLC tables/columns)
- SequenceFlow: Represents join processes connecting input to output
- SubProcess: Container for the complete workflow
"""

from django.db import transaction
from pybirdai.models.bpmn_lite_models import (
    UserTask, ServiceTask, SequenceFlow, SubProcess,
    SubProcessFlowElement, WorkflowModule
)


# All ANACREDIT output tables (must match CUBE records in database)
OUTPUT_TABLES = [
    'ANCRDT_INSTRMNT_C_1',
    'ANCRDT_FNNCL_C_1',
    'ANCRDT_ACCNTNG_C_1',
    'ANCRDT_ENTTY_C_1',
    'ANCRDT_ENTTY_DFLT_C_1',
    'ANCRDT_ENTTY_RSK_C_1',
    'ANCRDT_PRTCTN_RCVD_C_1',
    'ANCRDT_INSTRMNT_PRTCTN_RCVD_C_1',
    'ANCRDT_JNT_LBLTS_C_1',
    'ANCRDT_ENTTY_INSTRMNT_C_1'
]


class AncrdtMetadataLineageProcessor:
    """
    Processes metadata lineage for ANACREDIT and stores it using BPMN_lite models.

    Creates BPMN workflows where:
    - UserTask = Input data items (ILC tables/columns that are consumed)
    - ServiceTask = Output data items (ROLC tables/columns that are produced)
    - SequenceFlow = Join processes connecting input to output
    - SubProcess = Container for the complete output table workflow
    """

    def __init__(self, sdd_context=None):
        self.sdd_context = sdd_context
        self.created_tasks = {}  # Cache for created tasks
        self.created_flows = {}  # Cache for created flows

    def process_all_output_tables(self):
        """
        Process metadata lineage for all ANACREDIT output tables.

        Returns:
            dict: Summary of processed tables with their workflow SubProcess objects
        """
        results = {}

        for output_table in OUTPUT_TABLES:
            try:
                subprocess = self.process_output_table_lineage(output_table)
                results[output_table] = {
                    'success': True,
                    'subprocess': subprocess
                }
                print(f"✓ Processed lineage for {output_table}")
            except Exception as e:
                results[output_table] = {
                    'success': False,
                    'error': str(e)
                }
                print(f"✗ Failed to process {output_table}: {e}")

        return results

    def process_output_table_lineage(self, output_table_id: str):
        """
        Create BPMN workflow for a specific ANACREDIT output table's metadata lineage.

        Args:
            output_table_id: The CUBE ID of the output table (e.g., 'ANCRDT_INSTRMNT_C_1')

        Returns:
            SubProcess: The created workflow subprocess
        """
        from pybirdai.models.bird_meta_data_model import (
            CUBE, CUBE_LINK, CUBE_STRUCTURE_ITEM, CUBE_STRUCTURE_ITEM_LINK
        )

        print(f"Creating BPMN workflow for ANACREDIT output table: {output_table_id}")

        # Get the output cube
        try:
            output_cube = CUBE.objects.get(cube_id=output_table_id)
        except CUBE.DoesNotExist:
            print(f"Output cube not found: {output_table_id}")
            raise ValueError(f"Output cube not found: {output_table_id}")

        # Create main SubProcess to contain the workflow
        workflow_subprocess = self._create_or_get_subprocess(
            id=f"ancrdt_workflow_{output_table_id}",
            name=f"ANACREDIT Lineage: {output_table_id}",
            description=f"Complete metadata lineage workflow for ANACREDIT output table {output_table_id}"
        )

        # Create workflow module
        workflow_module = self._create_or_get_workflow_module(
            module_id=f"ancrdt_module_{output_table_id}",
            module_name=f"ANACREDIT {output_table_id} Lineage Module"
        )

        # Create ServiceTask for the output table
        output_table_task = self._create_or_get_service_task(
            id=f"ancrdt_output_{output_table_id}",
            name=f"Output: {output_table_id}",
            description=f"ANACREDIT output table {output_table_id}",
            enriched_attribute_reference=output_table_id
        )
        self._add_element_to_subprocess(workflow_subprocess, output_table_task, 'ServiceTask')

        # Get all CUBE_LINK records where this table is the foreign (output) cube
        cube_links = CUBE_LINK.objects.filter(
            foreign_cube_id=output_cube
        ).select_related('primary_cube_id')

        if not cube_links.exists():
            print(f"  No cube links found for {output_table_id}")
            return workflow_subprocess

        # Group cube links by join_identifier (product)
        join_groups = {}
        for link in cube_links:
            join_id = link.join_identifier or 'DEFAULT'
            if join_id not in join_groups:
                join_groups[join_id] = []
            join_groups[join_id].append(link)

        print(f"  Found {len(join_groups)} join group(s) for {output_table_id}")

        # Process each join group
        for join_identifier, links in join_groups.items():
            self._process_join_group(
                workflow_subprocess,
                output_cube,
                output_table_task,
                join_identifier,
                links
            )

        return workflow_subprocess

    def _process_join_group(self, workflow_subprocess, output_cube, output_table_task,
                            join_identifier, cube_links):
        """
        Process a join group and create corresponding BPMN elements.

        Args:
            workflow_subprocess: The parent SubProcess
            output_cube: The output CUBE object
            output_table_task: The ServiceTask for the output table
            join_identifier: The join identifier (product name)
            cube_links: List of CUBE_LINK objects for this join group
        """
        from pybirdai.models.bird_meta_data_model import (
            CUBE_STRUCTURE_ITEM, CUBE_STRUCTURE_ITEM_LINK
        )

        safe_join_id = join_identifier.replace(' ', '_').replace(':', '_')
        print(f"    Processing join group: {join_identifier}")

        # Create a ServiceTask for the join process
        join_task = self._create_or_get_service_task(
            id=f"ancrdt_join_{output_cube.cube_id}_{safe_join_id}",
            name=f"Join: {join_identifier}",
            description=f"Join process for {join_identifier} to {output_cube.cube_id}",
            enriched_attribute_reference=f"{output_cube.cube_id}.{join_identifier}"
        )
        self._add_element_to_subprocess(workflow_subprocess, join_task, 'ServiceTask')

        # Create flow from join to output table
        join_to_output_flow = self._create_or_get_sequence_flow(
            id=f"ancrdt_flow_join_{safe_join_id}_to_{output_cube.cube_id}",
            name=f"Join to Output",
            source_task=join_task,
            target_task=output_table_task,
            description=f"Flow from {join_identifier} join to output table {output_cube.cube_id}"
        )
        self._add_element_to_subprocess(workflow_subprocess, join_to_output_flow, 'SequenceFlow')

        # Process each cube link in the group
        processed_input_cubes = set()
        for cube_link in cube_links:
            primary_cube = cube_link.primary_cube_id
            if not primary_cube or primary_cube.cube_id in processed_input_cubes:
                continue

            processed_input_cubes.add(primary_cube.cube_id)

            # Create UserTask for input table
            input_table_task = self._create_or_get_user_task(
                id=f"ancrdt_input_{primary_cube.cube_id}",
                name=f"Input: {primary_cube.cube_id}",
                description=f"Input table {primary_cube.cube_id}",
                entity_reference=primary_cube.cube_id
            )
            self._add_element_to_subprocess(workflow_subprocess, input_table_task, 'UserTask')

            # Create flow from input to join
            input_to_join_flow = self._create_or_get_sequence_flow(
                id=f"ancrdt_flow_{primary_cube.cube_id}_to_join_{safe_join_id}",
                name=f"Input to Join",
                source_task=input_table_task,
                target_task=join_task,
                description=f"Flow from {primary_cube.cube_id} to {join_identifier} join"
            )
            self._add_element_to_subprocess(workflow_subprocess, input_to_join_flow, 'SequenceFlow')

            # Process column-level lineage via CUBE_STRUCTURE_ITEM_LINK
            self._process_column_lineage(
                workflow_subprocess,
                cube_link,
                primary_cube,
                output_cube,
                join_identifier,
                input_table_task,
                output_table_task
            )

    def _process_column_lineage(self, workflow_subprocess, cube_link, input_cube, output_cube,
                                 join_identifier, input_table_task, output_table_task):
        """
        Process column-level lineage from CUBE_STRUCTURE_ITEM_LINK records.
        """
        from pybirdai.models.bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK

        safe_join_id = join_identifier.replace(' ', '_').replace(':', '_')

        # Get column-level links
        column_links = CUBE_STRUCTURE_ITEM_LINK.objects.filter(
            cube_link_id=cube_link
        ).select_related(
            'primary_cube_variable_code',
            'primary_cube_variable_code__variable_id',
            'foreign_cube_variable_code',
            'foreign_cube_variable_code__variable_id'
        )

        for col_link in column_links:
            primary_csi = col_link.primary_cube_variable_code
            foreign_csi = col_link.foreign_cube_variable_code

            if not primary_csi or not foreign_csi:
                continue

            primary_var = primary_csi.variable_id
            foreign_var = foreign_csi.variable_id

            if not primary_var or not foreign_var:
                continue

            # Create UserTask for input column
            input_col_task = self._create_or_get_user_task(
                id=f"ancrdt_col_{input_cube.cube_id}_{primary_var.variable_id}",
                name=f"Column: {primary_var.variable_id}",
                description=f"Input column {primary_var.variable_id} from {input_cube.cube_id}",
                entity_reference=f"{input_cube.cube_id}.{primary_var.variable_id}"
            )
            self._add_element_to_subprocess(workflow_subprocess, input_col_task, 'UserTask')

            # Create ServiceTask for output column
            output_col_task = self._create_or_get_service_task(
                id=f"ancrdt_col_{output_cube.cube_id}_{foreign_var.variable_id}",
                name=f"Column: {foreign_var.variable_id}",
                description=f"Output column {foreign_var.variable_id} in {output_cube.cube_id}",
                enriched_attribute_reference=f"{output_cube.cube_id}.{foreign_var.variable_id}"
            )
            self._add_element_to_subprocess(workflow_subprocess, output_col_task, 'ServiceTask')

            # Create flow for column transformation
            col_flow = self._create_or_get_sequence_flow(
                id=f"ancrdt_colflow_{input_cube.cube_id}_{primary_var.variable_id}_to_{output_cube.cube_id}_{foreign_var.variable_id}_{safe_join_id}",
                name=f"Map {primary_var.variable_id}",
                source_task=input_col_task,
                target_task=output_col_task,
                description=f"Column mapping via {join_identifier}: {primary_var.variable_id} -> {foreign_var.variable_id}"
            )
            self._add_element_to_subprocess(workflow_subprocess, col_flow, 'SequenceFlow')

    # -------------------------------------------------------------------------
    # BPMN Element Creation Methods (same pattern as BPMNMetadataLineageProcessor)
    # -------------------------------------------------------------------------

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
            print(f"  Created SubProcess: {name}")
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
            print(f"  Created WorkflowModule: {module_name}")
        return module

    def _create_or_get_user_task(self, id, name, description, entity_reference=None):
        """Create or get UserTask (represents input/consumed data items)."""
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

        self.created_tasks[cache_key] = task
        return task

    def _create_or_get_service_task(self, id, name, description, enriched_attribute_reference=None):
        """Create or get ServiceTask (represents output/produced data items)."""
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

        self.created_tasks[cache_key] = task
        return task

    def _create_or_get_sequence_flow(self, id, name, source_task, target_task, description=None):
        """Create or get SequenceFlow (represents transformation process)."""
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

        self.created_flows[cache_key] = flow
        return flow

    def _add_element_to_subprocess(self, subprocess, element, element_type):
        """Add a flow element to a subprocess."""
        SubProcessFlowElement.objects.get_or_create(
            sub_process=subprocess,
            flow_element_type=element_type,
            flow_element_id=element.id
        )


def run_ancrdt_metadata_lineage():
    """
    Entry point function for running ANACREDIT metadata lineage processing.
    """
    processor = AncrdtMetadataLineageProcessor()
    results = processor.process_all_output_tables()

    # Summary
    success_count = sum(1 for r in results.values() if r['success'])
    print(f"\nANACREDIT Metadata Lineage Processing Complete")
    print(f"  Successful: {success_count}/{len(results)}")

    return results
