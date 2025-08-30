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
from django.db.models import Q

from pybirdai.models.bird_meta_data_model import COMBINATION
from pybirdai.models.bpmn_lite_models import (
    UserTask, ServiceTask, SequenceFlow, SubProcess, 
    SubProcessFlowElement, WorkflowModule
)
from pybirdai.context.sdd_context_django import SDDContext
from pybirdai.process_steps.metadata_lineage.bpmn_metadata_lineage_processor import BPMNMetadataLineageProcessor


@ensure_csrf_cookie
def datapoint_bpmn_metadata_lineage_viewer(request, datapoint_id):
    """
    Main view for displaying BPMN metadata lineage visualization for a specific datapoint
    """
    datapoint = get_object_or_404(COMBINATION, combination_id=datapoint_id)
    
    context = {
        'datapoint': datapoint,
        'datapoint_id': datapoint_id,
        'combination_id': datapoint.combination_id,
    }
    return render(request, 'pybirdai/datapoint_bpmn_metadata_lineage.html', context)


@csrf_exempt
@require_http_methods(["GET", "POST"])
def process_datapoint_bpmn_metadata_lineage(request, datapoint_id):
    """
    API endpoint to process and fetch BPMN metadata lineage data for a specific datapoint
    
    GET: Returns existing BPMN metadata lineage if available
    POST: Processes BPMN metadata lineage for the datapoint and returns the results
    """
    datapoint = get_object_or_404(COMBINATION, combination_id=datapoint_id)
    
    try:
        if request.method == "POST":
            # Clear existing BPMN metadata lineage for this datapoint (optional)
            clear_existing = request.POST.get('clear_existing', 'false').lower() == 'true'
            
            if clear_existing:
                # Clear existing BPMN lineage for this datapoint
                _clear_datapoint_bpmn_metadata_lineage(datapoint)
            
            # Process BPMN metadata lineage
            base_dir = settings.BASE_DIR
            sdd_context = SDDContext()
            sdd_context.file_directory = os.path.join(base_dir, 'resources')
            
            processor = BPMNMetadataLineageProcessor(sdd_context)
            
            # Process the specific datapoint and export results
            with transaction.atomic():
                workflow_subprocess = processor.process_datapoint_metadata_lineage(datapoint)
                
                # Get the BPMN lineage data for this datapoint (inside transaction to ensure visibility)
                lineage_data = _get_datapoint_bpmn_lineage_data(datapoint)
            
            # Export results to JSON
            output_path = os.path.join(
                base_dir, 'results', 'bpmn_metadata_lineage', 
                f'datapoint_{datapoint_id}_bpmn_lineage.json'
            )
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save to file
            with open(output_path, 'w') as f:
                json.dump(lineage_data, f, indent=2)
            
            return JsonResponse({
                'success': True,
                'message': f'BPMN metadata lineage processed for datapoint {datapoint_id}',
                'output_file': output_path,
                'lineage': lineage_data
            })
        
        else:  # GET request
            # Return existing BPMN lineage data
            lineage_data = _get_datapoint_bpmn_lineage_data(datapoint)
            
            return JsonResponse({
                'success': True,
                'datapoint_id': datapoint_id,
                'lineage': lineage_data
            })
    
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error in process_datapoint_bpmn_metadata_lineage: {error_details}")
        
        return JsonResponse({
            'success': False,
            'error': str(e),
            'datapoint_id': datapoint_id
        }, status=500)


@csrf_exempt
@require_http_methods(["GET"])
def get_datapoint_bpmn_metadata_lineage_graph(request, datapoint_id):
    """
    API endpoint to get BPMN metadata lineage data formatted for graph visualization
    """
    datapoint = get_object_or_404(COMBINATION, combination_id=datapoint_id)
    
    try:
        lineage_data = _get_datapoint_bpmn_lineage_data(datapoint)
        
        # Convert to graph format for visualization
        nodes = []
        edges = []
        
        # Add UserTask nodes (input data items)
        for user_task in lineage_data['user_tasks']:
            nodes.append({
                'id': f"user_task_{user_task['id']}",
                'label': user_task['name'],
                'type': user_task['type'],
                'category': 'input',
                'description': user_task.get('description', ''),
                'entity_reference': user_task.get('entity_reference', '')
            })
        
        # Add ServiceTask nodes (output data items)
        for service_task in lineage_data['service_tasks']:
            nodes.append({
                'id': f"service_task_{service_task['id']}",
                'label': service_task['name'],
                'type': service_task['type'],
                'category': 'output',
                'description': service_task.get('description', ''),
                'enriched_attribute_reference': service_task.get('enriched_attribute_reference', '')
            })
        
        # Add edges from SequenceFlows
        for sequence_flow in lineage_data['sequence_flows']:
            if sequence_flow['source_ref'] and sequence_flow['target_ref']:
                edges.append({
                    'source': f"user_task_{sequence_flow['source_ref']}" if sequence_flow['source_type'] == 'UserTask' else f"service_task_{sequence_flow['source_ref']}",
                    'target': f"user_task_{sequence_flow['target_ref']}" if sequence_flow['target_type'] == 'UserTask' else f"service_task_{sequence_flow['target_ref']}",
                    'type': 'sequence_flow',
                    'label': sequence_flow.get('name', 'Flow'),
                    'description': sequence_flow.get('description', '')
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
        print(f"Error in get_datapoint_bpmn_metadata_lineage_graph: {error_details}")
        
        return JsonResponse({
            'success': False,
            'error': str(e),
            'datapoint_id': datapoint_id
        }, status=500)


def _clear_datapoint_bpmn_metadata_lineage(datapoint):
    """
    Clear existing BPMN metadata lineage for a datapoint
    """
    combination_id = datapoint.combination_id
    
    print(f"Clearing BPMN metadata lineage for {combination_id}")
    
    # Find and delete the workflow subprocess and related elements
    subprocess_id = f"workflow_{combination_id}"
    try:
        subprocess = SubProcess.objects.get(id=subprocess_id)
        
        # Delete all flow elements in the subprocess
        SubProcessFlowElement.objects.filter(sub_process=subprocess).delete()
        
        # Delete the subprocess itself
        subprocess.delete()
        
        print(f"Cleared SubProcess: {subprocess_id}")
    except SubProcess.DoesNotExist:
        print(f"No existing SubProcess found for {subprocess_id}")
    
    # Clean up related tasks and flows that are no longer referenced
    _cleanup_orphaned_bpmn_elements(combination_id)


def _cleanup_orphaned_bpmn_elements(combination_id):
    """
    Clean up orphaned BPMN elements for a specific datapoint
    """
    # Find tasks related to this datapoint
    datapoint_filter = Q(id__contains=combination_id)
    
    # Delete UserTasks related to this datapoint
    user_tasks = UserTask.objects.filter(datapoint_filter)
    user_task_count = user_tasks.count()
    user_tasks.delete()
    print(f"Deleted {user_task_count} UserTasks")
    
    # Delete ServiceTasks related to this datapoint
    service_tasks = ServiceTask.objects.filter(datapoint_filter)
    service_task_count = service_tasks.count()
    service_tasks.delete()
    print(f"Deleted {service_task_count} ServiceTasks")
    
    # Delete SequenceFlows related to this datapoint
    sequence_flows = SequenceFlow.objects.filter(datapoint_filter)
    sequence_flow_count = sequence_flows.count()
    sequence_flows.delete()
    print(f"Deleted {sequence_flow_count} SequenceFlows")
    
    # Delete WorkflowModule
    try:
        module = WorkflowModule.objects.get(module_id=f"module_{combination_id}")
        module.delete()
        print(f"Deleted WorkflowModule for {combination_id}")
    except WorkflowModule.DoesNotExist:
        pass


def _get_datapoint_bpmn_lineage_data(datapoint):
    """
    Retrieve BPMN metadata lineage data for a specific datapoint
    """
    combination_id = datapoint.combination_id
    
    print(f"Getting BPMN lineage data for {combination_id}")
    
    # Get the main subprocess for this datapoint
    subprocess_id = f"workflow_{combination_id}"
    try:
        subprocess = SubProcess.objects.get(id=subprocess_id)
    except SubProcess.DoesNotExist:
        print(f"No SubProcess found for {subprocess_id}")
        return {
            'user_tasks': [],
            'service_tasks': [],
            'sequence_flows': [],
            'subprocesses': [],
            'workflow_modules': []
        }
    
    # Get all flow elements in the subprocess
    flow_elements = SubProcessFlowElement.objects.filter(sub_process=subprocess)
    
    # Collect element IDs by type
    user_task_ids = []
    service_task_ids = []
    sequence_flow_ids = []
    
    for element in flow_elements:
        if element.flow_element_type == 'UserTask':
            user_task_ids.append(element.flow_element_id)
        elif element.flow_element_type == 'ServiceTask':
            service_task_ids.append(element.flow_element_id)
        elif element.flow_element_type == 'SequenceFlow':
            sequence_flow_ids.append(element.flow_element_id)
    
    # Get the actual objects
    user_tasks = UserTask.objects.filter(id__in=user_task_ids)
    service_tasks = ServiceTask.objects.filter(id__in=service_task_ids)
    sequence_flows = SequenceFlow.objects.filter(id__in=sequence_flow_ids)
    
    # Get workflow module
    workflow_modules = WorkflowModule.objects.filter(module_id=f"module_{combination_id}")
    
    print(f"BPMN lineage summary:")
    print(f"  UserTasks: {user_tasks.count()}")
    print(f"  ServiceTasks: {service_tasks.count()}")
    print(f"  SequenceFlows: {sequence_flows.count()}")
    print(f"  WorkflowModules: {workflow_modules.count()}")
    
    # Build the response
    return {
        'user_tasks': [
            {
                'id': task.id,
                'name': task.name,
                'type': 'UserTask',
                'description': task.description,
                'entity_reference': task.entity_reference
            }
            for task in user_tasks
        ],
        'service_tasks': [
            {
                'id': task.id,
                'name': task.name,
                'type': 'ServiceTask',
                'description': task.description,
                'enriched_attribute_reference': task.enriched_attribute_reference,
                'entity_creation_task': getattr(task, 'entity_creation_task', False)
            }
            for task in service_tasks
        ],
        'sequence_flows': [
            {
                'id': flow.id,
                'name': flow.name,
                'description': flow.description,
                'source_ref': flow.source_ref.id if flow.source_ref else None,
                'target_ref': flow.target_ref.id if flow.target_ref else None,
                'source_type': flow.source_ref.__class__.__name__ if flow.source_ref else None,
                'target_type': flow.target_ref.__class__.__name__ if flow.target_ref else None,
            }
            for flow in sequence_flows
        ],
        'subprocesses': [
            {
                'id': subprocess.id,
                'name': subprocess.name,
                'description': subprocess.description
            }
        ],
        'workflow_modules': [
            {
                'module_id': module.module_id,
                'module_name': module.module_name
            }
            for module in workflow_modules
        ]
    }