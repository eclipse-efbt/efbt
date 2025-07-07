import os
import logging
os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'
from django.conf import settings
import pytest
from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()
from pybirdai.entry_points.execute_datapoint import RunExecuteDataPoint
from pybirdai.process_steps.pybird.execute_datapoint import ExecuteDataPoint
from pybirdai.process_steps.filter_code.report_cells import Cell_F_05_01_REF_FINREP_3_0_152589_REF

def test_execute_datapoint(value: int=83491250):
    data_point_id = 'F_05_01_REF_FINREP_3_0_152589_REF'
    result = RunExecuteDataPoint.run_execute_data_point(data_point_id)
    ExecuteDataPoint.delete_lineage_data()
    assert result == str(value)