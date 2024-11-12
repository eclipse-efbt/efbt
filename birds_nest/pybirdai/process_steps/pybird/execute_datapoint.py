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
from django.conf import settings

class ExecuteDataPoint:
    def execute_data_point(data_point_id):
        ExecuteDataPoint.delete_lineage_data()
        print(f"Executing data point with ID: {data_point_id}")
        # Add your logic here
        klass = globals()['Cell_' +data_point_id]
        datapoint = klass()
        datapoint.init()
        return str(datapoint.metric_value())
    
    def delete_lineage_data():
        base_dir = settings.BASE_DIR
        lineage_dir = os.path.join(base_dir, 'results', 'lineage')
        for file in os.listdir(lineage_dir):
            os.remove(os.path.join(lineage_dir, file))

