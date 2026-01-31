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

import django
import os
from django.apps import AppConfig
from django.conf import settings

class RunMetadataLineageProcessor(AppConfig):
    """
    Django AppConfig for processing metadata lineage.
    
    This class sets up the necessary context and runs the metadata
    lineage processing workflow to create DataItem, Process, and
    Relationship instances from BIRD metadata.
    """
    
    path = os.path.join(settings.BASE_DIR, 'birds_nest')
    
    def ready(self):
        """
        Prepare and execute the metadata lineage processing workflow.
        """
        from pybirdai.context.context import Context
        from pybirdai.context.sdd_context_django import SDDContext
        from pybirdai.process_steps.metadata_lineage.metadata_lineage_processor import MetadataLineageProcessor
        
        print("Starting metadata lineage processing...")
        
        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        
        # Initialize the metadata lineage processor
        processor = MetadataLineageProcessor(sdd_context)
        
        # Process different types of metadata lineage
        try:
            # Process input tables
            processor.process_input_tables()
            print("✓ Input tables processed")
            
            # Process output tables
            processor.process_output_tables()
            print("✓ Output tables processed")
            
            # Process product-specific joins
            processor.process_product_specific_joins()
            print("✓ Product-specific joins processed")
            
            # Process datapoints
            processor.process_datapoints()
            print("✓ Datapoints processed")
            
            # Export lineage graph to JSON
            output_path = os.path.join(base_dir, 'results', 'metadata_lineage.json')
            processor.export_lineage_to_json(output_path)
            print(f"✓ Metadata lineage exported to {output_path}")
            
            print("Metadata lineage processing completed successfully!")
            
        except Exception as e:
            print(f"Error during metadata lineage processing: {e}")
            raise