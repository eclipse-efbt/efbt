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
# This script creates output layers (CUBE, CUBE_STRUCTURE, COMBINATION, etc.) 
# from DPM table data for any framework

import django
import os
from django.apps import AppConfig
from pybirdai.context.sdd_context_django import SDDContext
from django.conf import settings
import logging

class RunDPMOutputLayerCreation(AppConfig):
    """
    Django AppConfig for running the DPM output layer creation process.
    
    This class creates output layers (CUBE, CUBE_STRUCTURE, COMBINATION, etc.)
    from DPM table data, supporting any framework (FINREP, COREP, AE, etc.).
    """
    
    path = os.path.join(settings.BASE_DIR, 'birds_nest')
    
    @staticmethod
    def run_creation(framework=None, table_code=None):
        """
        Run the output layer creation process.
        
        Args:
            framework: Optional framework name (e.g., 'FINREP', 'COREP', 'AE')
            table_code: Optional specific table code to process
        
        Returns:
            dict: Results of the creation process
        """
        from pybirdai.process_steps.report_filters.create_non_reference_output_layers import CreateNROutputLayers
        from pybirdai.context.context import Context
        from django.conf import settings
        
        # Set up context
        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'results')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        
        context = Context()
        context.file_directory = sdd_context.output_directory
        context.output_directory = sdd_context.output_directory
        
        # Create output layer creator instance
        creator = CreateNROutputLayers()
        
        results = {
            'status': 'success',
            'processed': [],
            'errors': []
        }
        
        try:
            if table_code:
                # Process specific table
                logging.info(f"Creating output layers for table: {table_code}")
                cube, cube_structure = creator.process_table_by_code(table_code, save_to_db=True)
                results['processed'].append({
                    'table_code': table_code,
                    'cube': cube.cube_id,
                    'cube_structure': cube_structure.cube_structure_id
                })
                logging.info(f"Successfully created output layers for table: {table_code}")
                
            elif framework:
                # Process all tables for a framework
                logging.info(f"Creating output layers for framework: {framework}")
                framework_results = creator.process_framework_tables(framework, save_to_db=True)
                
                for result in framework_results:
                    if result['status'] == 'success':
                        results['processed'].append({
                            'table': result['table'].table_id,
                            'cube': result['cube'].cube_id,
                            'cube_structure': result['cube_structure'].cube_structure_id
                        })
                    else:
                        results['errors'].append({
                            'table': result['table'].table_id,
                            'error': result['error']
                        })
                
                logging.info(f"Processed {len(results['processed'])} tables for framework: {framework}")
                
            else:
                # No specific framework or table specified
                results['status'] = 'error'
                results['message'] = 'Please specify either a framework or table_code parameter'
                logging.warning("No framework or table_code specified for output layer creation")
                
        except Exception as e:
            logging.error(f"Error during output layer creation: {str(e)}")
            results['status'] = 'error'
            results['message'] = str(e)
            
        return results
    
    def ready(self):
        # This method is still needed for Django's AppConfig
        pass