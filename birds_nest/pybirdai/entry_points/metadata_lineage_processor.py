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
#    Benjamin Arfa - multi-framework support
#

"""
Entry point for processing metadata lineage using BPMN_lite models.

This module creates BPMN workflows that represent metadata lineage from
input tables through product-specific joins to output tables.

Supports multiple frameworks:
- Template-based (FINREP, COREP): Uses BPMNMetadataLineageProcessor
- Dataset-based (ANACREDIT): Uses AncrdtMetadataLineageProcessor
"""

import os
from django.apps import AppConfig
from django.conf import settings


# Framework detection patterns
ANACREDIT_PREFIX = 'ANCRDT_'
TEMPLATE_PREFIXES = ('F_', 'C_', 'FINREP_', 'COREP_')


class RunMetadataLineageProcessor(AppConfig):
    """
    Django AppConfig for processing metadata lineage.

    This class sets up the necessary context and runs the appropriate
    metadata lineage processor based on framework type:
    - FINREP/COREP: Template-based BPMN processor
    - ANACREDIT: Dataset-based ANCRDT processor
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    def ready(self):
        """
        Prepare and execute the metadata lineage processing workflow.
        """
        print("Starting metadata lineage processing for all frameworks...")

        results = {
            'finrep_corep': {'processed': 0, 'failed': 0},
            'anacredit': {'processed': 0, 'failed': 0}
        }

        # Process template-based frameworks (FINREP/COREP)
        print("\n=== Processing FINREP/COREP (template-based) ===")
        results['finrep_corep'] = self._process_template_based_frameworks()

        # Process dataset-based frameworks (ANACREDIT)
        print("\n=== Processing ANACREDIT (dataset-based) ===")
        results['anacredit'] = self._process_anacredit()

        # Summary
        print("\n=== Metadata Lineage Processing Summary ===")
        print(f"FINREP/COREP: {results['finrep_corep']['processed']} processed, "
              f"{results['finrep_corep']['failed']} failed")
        print(f"ANACREDIT: {results['anacredit']['processed']} processed, "
              f"{results['anacredit']['failed']} failed")
        print("Results stored in BPMN_lite models (UserTask, ServiceTask, SequenceFlow, SubProcess)")

    def _process_template_based_frameworks(self):
        """
        Process FINREP and COREP datapoints using BPMNMetadataLineageProcessor.

        Returns:
            dict: Processing results with 'processed' and 'failed' counts
        """
        from pybirdai.context.sdd_context_django import SDDContext
        from pybirdai.process_steps.metadata_lineage.bpmn_metadata_lineage_processor import (
            BPMNMetadataLineageProcessor
        )
        from pybirdai.models.bird_meta_data_model import COMBINATION

        results = {'processed': 0, 'failed': 0}

        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')

        # Initialize the BPMN metadata lineage processor
        processor = BPMNMetadataLineageProcessor(sdd_context)

        # Filter datapoints to template-based frameworks only (exclude ANACREDIT)
        datapoints = COMBINATION.objects.exclude(
            combination_id__startswith=ANACREDIT_PREFIX
        )
        total = datapoints.count()

        if total == 0:
            print("No FINREP/COREP datapoints found.")
            return results

        print(f"Processing {total} FINREP/COREP datapoints...")

        for i, datapoint in enumerate(datapoints, 1):
            try:
                processor.process_datapoint_metadata_lineage(datapoint)
                results['processed'] += 1
                if i % 100 == 0 or i == total:
                    print(f"✓ Processed {i}/{total} datapoints")
            except Exception as e:
                results['failed'] += 1
                print(f"Warning: Failed to process datapoint {datapoint.combination_id}: {e}")
                continue

        print(f"FINREP/COREP processing complete: {results['processed']} success, {results['failed']} failed")
        return results

    def _process_anacredit(self):
        """
        Process ANACREDIT output tables using AncrdtMetadataLineageProcessor.

        Returns:
            dict: Processing results with 'processed' and 'failed' counts
        """
        from pybirdai.process_steps.ancrdt_transformation.ancrdt_metadata_lineage_processor import (
            AncrdtMetadataLineageProcessor
        )
        from pybirdai.models.bird_meta_data_model import CUBE

        results = {'processed': 0, 'failed': 0}

        # Check if any ANACREDIT cubes exist
        ancrdt_cubes = CUBE.objects.filter(cube_id__startswith=ANACREDIT_PREFIX)
        if not ancrdt_cubes.exists():
            print("No ANACREDIT cubes found in database.")
            return results

        # Initialize the ANACREDIT metadata lineage processor
        processor = AncrdtMetadataLineageProcessor()

        # Process all ANACREDIT output tables
        table_results = processor.process_all_output_tables()

        for table_id, result in table_results.items():
            if result['success']:
                results['processed'] += 1
            else:
                results['failed'] += 1

        print(f"ANACREDIT processing complete: {results['processed']} success, {results['failed']} failed")
        return results


def run_metadata_lineage_for_framework(framework: str = None):
    """
    Standalone function to run metadata lineage processing.

    Args:
        framework: Optional framework to process ('FINREP', 'COREP', 'ANACREDIT', or None for all)

    Returns:
        dict: Processing results
    """
    from pybirdai.context.sdd_context_django import SDDContext
    from pybirdai.process_steps.metadata_lineage.bpmn_metadata_lineage_processor import (
        BPMNMetadataLineageProcessor
    )
    from pybirdai.process_steps.ancrdt_transformation.ancrdt_metadata_lineage_processor import (
        AncrdtMetadataLineageProcessor
    )
    from pybirdai.models.bird_meta_data_model import COMBINATION, CUBE

    results = {}

    if framework is None or framework.upper() in ('FINREP', 'COREP'):
        # Process template-based frameworks
        print(f"Processing template-based framework: {framework or 'ALL'}")
        sdd_context = SDDContext()

        # Pass framework_id for filtering if specified
        framework_id = framework.upper() if framework else None
        processor = BPMNMetadataLineageProcessor(sdd_context, framework_id=framework_id)

        # Filter datapoints
        datapoints = COMBINATION.objects.exclude(combination_id__startswith=ANACREDIT_PREFIX)
        if framework_id:
            # Further filter by combination_id prefix
            prefix = 'F_' if framework_id == 'FINREP' else 'C_'
            datapoints = datapoints.filter(combination_id__startswith=prefix)

        processed, failed = 0, 0
        for datapoint in datapoints:
            try:
                processor.process_datapoint_metadata_lineage(datapoint)
                processed += 1
            except Exception as e:
                failed += 1
                print(f"Failed: {datapoint.combination_id}: {e}")

        results['template_based'] = {'processed': processed, 'failed': failed}

    if framework is None or framework.upper() == 'ANACREDIT':
        # Process ANACREDIT
        print("Processing ANACREDIT...")
        processor = AncrdtMetadataLineageProcessor()
        table_results = processor.process_all_output_tables()

        processed = sum(1 for r in table_results.values() if r['success'])
        failed = sum(1 for r in table_results.values() if not r['success'])
        results['anacredit'] = {'processed': processed, 'failed': failed}

    return results
