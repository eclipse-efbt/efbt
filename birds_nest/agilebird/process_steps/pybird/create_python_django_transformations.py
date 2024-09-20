from agilebird.utils.utils import Utils
from agilebird.sdd_models import *


class CreatePythonTransformations:

    def create_python_transformations(self, context, sdd_context):
        '''
        Read in the tranforamtion meta daa and create python classes
        '''
        # get all the cube_links for  a report
        for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():
            self.create_python_class(join_for_report_id, cube_links, sdd_context)

    def create_python_class(self, join_for_report_id, cube_links, sdd_context):
        print(f"join_for_report_id: {join_for_report_id}")
        print(f"cube_links: {cube_links}")