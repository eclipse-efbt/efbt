# coding=UTF-8
# Copyright (c) 2020 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

from birdai.utils.utils import Utils

import os


class CreateExecutableFilters:
    def create_executable_filters(self,context, sdd_context):

        file = open(sdd_context.output_directory + os.sep + 'generated_python' + os.sep +  'report_cells.py', "a",  encoding='utf-8') 
        file.write("from birdai.ldm_models import *\n")
        file.write("from output_tables import *\n")
        file.write("from birdai.process_steps.pybird.orchestration import Orchestration\n")

        for cube_id, combination_list in sdd_context.combination_to_rol_cube_map.items():
            for combination in combination_list:
                if combination.combination_id.metric:
                    file.write("class Cell_" +  cube_id + "_" + combination.combination_id.combination_id + ":\n")
                    file.write("\t" + cube_id + "_Table = None\n")
                    file.write("\t" + cube_id + "s = []\n")
                    file.write("\tdef metric_value():\n"  )
                    file.write("\t\ttotal = 0\n")
                    file.write("\t\tfor item in " + cube_id + "s:\n")

                    file.write("\t\t\ttotal += item." + combination.combination_id.metric.name + "()\n")
                    file.write("\t\treturn total\n")
                    file.write("\tdef calc_referenced_items():\n")
                    file.write("\t\titems = " + cube_id + "_Table." + cube_id + "s\n")
                    file.write("\t\tfor item in items:\n")
                    file.write("\t\t\tfilter_passsed = False\n")
                    combination_item_list = []
                    try:
                        combination_item_list =  sdd_context.combination_item_dictionary[combination.combination_id.combination_id]
                    except:
                        pass
                    for combination_item in combination_item_list:
                        if combination_item.member_id.code is not None:
                            file.write("\t\t\tif item." + combination_item.variable_id.name + "() == " + combination_item.member_id.code + ":\n")
                            file.write("\t\t\t\tfilter_passed = True\n")
                            file.write("\t\t\tif filter_passed:\n")
                            file.write("\t\t\t\t" + cube_id + "s.add(item)\n")
                            file.write("\tdef init():\n")
                            file.write("\t\tOrchestration.init(self)\n")
                            file.write("\t\tcalc_referenced_items()\n")
                            file.write("\t\treturn None\n")


