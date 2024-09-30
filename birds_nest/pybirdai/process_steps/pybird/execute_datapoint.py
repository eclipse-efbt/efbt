from pybirdai.process_steps.filter_code.report_cells import *
import importlib

class ExecuteDataPoint:
    def execute_data_point(data_point_id):
        print(f"Executing data point with ID: {data_point_id}")
        # Add your logic here
        klass = globals()['Cell_' +data_point_id]
        datapoint = klass()
        datapoint.init()
        return str(datapoint.metric_value())

