# coding=UTF-8#
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
#
import os
from pyecore.resources import ResourceSet, URI
from pyecore.resources.json import JsonResource
from utils.utils import Utils



class PersistSDD:
    '''
    Documentation for PersistSDD
    '''
    def save_analysis_model_as_xmi_files(self, sdd_context):
        rset = ResourceSet()
        extension = 'xmi'
        sdd_model_resource = rset.create_resource(URI(
            sdd_context.output_directory + os.sep + extension +
            os.sep + "sdd.xmi"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        sdd_model_resource.append(sdd_context.sdd_model)
        sdd_model_resource.save()
        
    def save_analysis_model_as_csv(self, sdd_context):
        ''' For each report cell, show its report, row and column
        and each of its combination items
        '''
        f = open(sdd_context.output_directory + os.sep + "csv" +
                 os.sep + "report_cells" +
                 '.' + "csv", "a",  encoding='utf-8')
        for cell_position in sdd_context.cell_positions.cellPositions:
            x_axis = "None"
            y_axis = "None"
            cell = cell_position.cell_id
            if not(cell is None):
                template = cell.table_id
                template_name = template.table_id
                for axis_ordinate in cell_position.axis_ordinate_id:
                    axis=axis_ordinate.axis_id
                    orientation = axis.orientation
                    if orientation == 'X':
                        x_axis = axis_ordinate.code
                    if orientation == 'Y':
                        y_axis = axis_ordinate.code
                    axis_ordinate_id = axis_ordinate.axis_ordinate_id
                    index_of_ref = axis_ordinate_id.index('_REF')
                   
                f.write(template_name+"," +x_axis +"," + y_axis +",")
                comb = cell_position.cell_id.combination_id
                # if there is no combination, this is because the combination is not
                # current, according to its valid_to date.
                if not(comb is None):
                    items = comb.combination_items
                    for item in items:
                        variable_id = item.variable_id.variable_id
                        if variable_id == "MTRCS":
                            # todo make sure we can get the metric name from
                            # the associate varaible set
                            variable_set = item.variable_set_id
                            if not(variable_set is None):
                                for item in variable_set.variable_set_items:
                                    f.write("Metric=" + item.variable_id.variable_id + ":")
                            else:
                                print("combination" + comb.combination_id + "has no metrics for metrics variable")
                        elif variable_id == "SUBA_CD" or variable_id == "VALUE_DECIMAL" or variable_id == "DT_RFRNC" or variable_id == "ENTTY_RIAD_CD_RPRTNG_AGNT" :
                            # these entity and refernce dates are not useful filters , we dont diplay them,
                            pass
                        else:
    
                            member = item.member_id
                            if(member is not None):
                                member_id = member.member_id
                                f.write(variable_id+"=" +member_id +":")
                    f.write("\n")
                    
        f = open(sdd_context.output_directory + os.sep + "csv" +
                 os.sep + "eba_dimensions" +
                 '.' + "csv", "a",  encoding='utf-8')
        
        f.write("dpm_dimension,dpm_dimension_member,choices\n")
        
        for member_mapping in sdd_context.member_mappings.memberMappings:
            member_mapping_name = member_mapping.name
            # we find mappings related to  just one non-reference variable
            # for ecample DPM_MCY, but not DPM_MCY_CPS
            if member_mapping_name.startswith("DPM_"):
                dpm_dimension = member_mapping_name[member_mapping_name.index('_')+1:len(member_mapping_name)]
                if dpm_dimension.find('_') == -1:
                    target_mapping_item_dict = {}
                    source_mapping_item_dict = {}
                    items = member_mapping.memberMappingItems
                    
                    for item in items:
                        row = item.row
                        is_source = item.isSource
                        variable_name = item.variable.name
                        member_name = item.member.name
                        choice = variable_name + ":" + member_name
                        if is_source == "TRUE":
                            source_mapping_item_dict[row] = member_name
                        else:
                            target_row_details = None
                            try:
                                target_row_details = target_mapping_item_dict[row]
                                target_mapping_item_dict[row] = target_row_details + ";" + choice
                            except KeyError:
                                target_mapping_item_dict[row] = choice
                                
                    for key,value in source_mapping_item_dict.items():
                        f.write(dpm_dimension + "," + source_mapping_item_dict[key] + "," + target_mapping_item_dict[key])
                        f.write("\n")
