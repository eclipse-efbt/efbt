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
from pybirdai.process_steps.filter_code.report_cells import *
import importlib
import os
from datetime import datetime
from django.conf import settings

class ExecuteDataPoint:
    def execute_data_point(data_point_id):
        ExecuteDataPoint.delete_lineage_data()
        print(f"Executing data point with ID: {data_point_id}")

        # Set up AORTA lineage tracking
        from pybirdai.process_steps.pybird.orchestration import Orchestration, OrchestrationWithLineage
        from pybirdai.annotations.decorators import set_lineage_orchestration
        from pybirdai.context.context import Context

        # Create orchestration based on configuration
        orchestration = Orchestration()
        execution_name = f"DataPoint_{data_point_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Only set up lineage if using the lineage-enhanced orchestrator
        if isinstance(orchestration, OrchestrationWithLineage):
            # Initialize the trail and metadata without dummy objects
            orchestration.trail = None
            orchestration.metadata_trail = None
            orchestration.current_populated_tables = {}
            orchestration.current_rows = {}
            orchestration.lineage_enabled = True

            # Create trail directly
            from pybirdai.aorta_model import MetaDataTrail, Trail
            orchestration.metadata_trail = MetaDataTrail.objects.create()
            orchestration.trail = Trail.objects.create(
                name=execution_name,
                metadata_trail=orchestration.metadata_trail
            )
            print(f"Created AORTA Trail: {orchestration.trail.name}")

            # Set the global lineage context
            set_lineage_orchestration(orchestration)
        else:
            print(f"Using original orchestrator - lineage tracking disabled")

        # Initialize with lineage tracking
        klass = globals()['Cell_' + str(data_point_id)]
        datapoint = klass()

        # Execute the datapoint
        datapoint.init()
        metric_value = str(datapoint.metric_value())

        # Print lineage summary
        if isinstance(orchestration, OrchestrationWithLineage):
            trail = orchestration.get_lineage_trail()
            if trail:
                print(f"AORTA Trail created: {trail.name} (ID: {trail.id})")
                from pybirdai.aorta_model import DatabaseTable, PopulatedDataBaseTable, DatabaseField, DatabaseRow
                print(f"  DatabaseTables: {DatabaseTable.objects.count()}")
                print(f"  PopulatedTables: {PopulatedDataBaseTable.objects.count()}")
                print(f"  DatabaseFields: {DatabaseField.objects.count()}")
                print(f"  DatabaseRows: {DatabaseRow.objects.count()}")

        del datapoint
        return metric_value

    def delete_lineage_data():
        base_dir = settings.BASE_DIR
        lineage_dir = os.path.join(base_dir, 'results', 'lineage')
        for file in os.listdir(lineage_dir):
            if file != "__init__.py":
                os.remove(os.path.join(lineage_dir, file))
