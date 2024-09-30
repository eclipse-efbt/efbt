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

from pybirdai.utils.utils import Utils

import os


class CreateExecutableFilters:
    def create_executable_filters(self,context, sdd_context):

        file = open(sdd_context.output_directory + os.sep + 'generated_python' + os.sep +  'report_cells.py', "a",  encoding='utf-8') 
        report_html_file = open(sdd_context.output_directory + os.sep + 'generated_html' + os.sep +  'report_templates.html', "a",  encoding='utf-8') 
        report_html_file.write("<!DOCTYPE html>\n")
        report_html_file.write("<html>\n")
        report_html_file.write("<head>\n")
        report_html_file.write("<title>Report Templates</title>\n")
        report_html_file.write("</head>\n")
        report_html_file.write("<body>\n")
        report_html_file.write("<h1>Report Templates</h1>\n")
        report_html_file.write("<table border=\"1\">\n") 

       
   
        file.write("from pybirdai.ldm_models import *\n")
        file.write("from output_tables import *\n")
        file.write("from pybirdai.process_steps.pybird.orchestration import Orchestration\n")

        
        for cube_id, combination_list in sdd_context.combination_to_rol_cube_map.items():
            report_html_file.write("<tr><td><a href=\"{% url 'pybirdai:show_report' '" + cube_id +'.html' + "'%}\">" +cube_id + "</a></td></tr>\n")   
            filter_html_file = open(sdd_context.output_directory + os.sep + 'generated_html' + os.sep +  cube_id +'.html', "a",  encoding='utf-8') 
            filter_html_file.write("<!DOCTYPE html>\n")
            filter_html_file.write("<html>\n")
            filter_html_file.write("<head>\n")
            filter_html_file.write("<title>Execute Datapoints</title>\n")
            filter_html_file.write("</head>\n")
            filter_html_file.write("<body>\n")
            filter_html_file.write("<h1>" + cube_id + "</h1>\n")
            filter_html_file.write("<table border=\"1\">\n")
            

            for combination in combination_list:
                if combination.combination_id.metric:
                    filter_html_file.write("<tr><td><a href=\"{% url 'pybirdai:execute_data_point' '" + cube_id + "_" + combination.combination_id.combination_id + "'%}\">" + cube_id + "_" + combination.combination_id.combination_id + "</a></td></tr>\n")
                    
                    
                    file.write("class Cell_" +  cube_id + "_" + combination.combination_id.combination_id + ":\n")
                    file.write("\t" + cube_id + "_Table = None\n")
                    file.write("\t" + cube_id + "s = []\n")
                    file.write("\tdef metric_value(self):\n"  )
                    file.write("\t\ttotal = 0\n")
                    file.write("\t\tfor item in self." + cube_id + "s:\n")
                    file.write("\t\t\ttotal += item." + combination.combination_id.metric.name + "()\n")
                    file.write("\t\treturn total\n")
                    file.write("\tdef calc_referenced_items(self):\n")
                    file.write("\t\titems = self." + cube_id + "_Table." + cube_id + "s\n")
                    file.write("\t\tfor item in items:\n")
                    file.write("\t\t\tfilter_passsed = False\n")
                    combination_item_list = []
                    try:
                        combination_item_list =  sdd_context.combination_item_dictionary[combination.combination_id.combination_id]
                    except:
                        pass
                    for combination_item in combination_item_list:
                        if combination_item.member_id.code is not None:
                            file.write("\t\t\tif item." + combination_item.variable_id.name + "() == '" + combination_item.member_id.code + "':\n")
                            file.write("\t\t\t\tfilter_passed = True\n")
                            file.write("\t\t\tif filter_passed:\n")
                            file.write("\t\t\t\tself." + cube_id + "s.add(item)\n")
                    file.write("\tdef init(self):\n")
                    file.write("\t\tOrchestration.init(self)\n")
                    file.write("\t\tself.calc_referenced_items()\n")
                    file.write("\t\treturn None\n")
            
            
            filter_html_file.write("</table>\n")
            filter_html_file.write("</body>\n")
            filter_html_file.write("</html>\n")
            filter_html_file.write("</table>\n")
            filter_html_file.write("</body>\n")
            filter_html_file.write("</html>\n")
        
        report_html_file.write("</table>\n")
        report_html_file.write("</body>\n")
        report_html_file.write("</html>\n")
        report_html_file.write("</table>\n")
        report_html_file.write("</body>\n")
        report_html_file.write("</html>\n")


