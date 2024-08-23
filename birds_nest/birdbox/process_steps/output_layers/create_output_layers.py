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

from birdbox.sdd_models import *

import os
import csv

class CreateOutputLayers(object):
    '''
    Documentation for CombinationsToReportFilters
    '''
    def create_output_layers(self, context,sdd_context,framework,version):
        '''
        for each cube mappng, look at eh varaible mappings and expanded 
        variable set mappings, and create a n output layer with those variables. 
        '''
        missing_domains = []
        # looka t cube mappings
        # get destination
        # checkif it is in scope
        # get the mappings
        # look at the variables

        file_location = context.file_directory + os.sep + "in_scope_reports_" + framework + ".csv"
        in_scope_reports = []
        header_skipped = False
        # Loop through the list of in scope reports
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not header_skipped):
                    header_skipped = True
                else:
                    report_template = row[0]
                    if framework == 'FINREP_REF':
                        in_scope_reports.append('M_' + report_template + '_REF_FINREP' + ' ' + version.replace('.','_'))
                    elif framework == 'AE_REF': 
                        in_scope_reports.append('M_' + report_template + '_REF_AE' + framework + ' ' + version.replace('.','_'))

        
        rol_variables = {}
        
        for destination_cube in sdd_context.mapping_to_cube_dictionary.keys():

            if destination_cube.replace('.','_') in in_scope_reports:
                
                for mapping_to_cube in sdd_context.mapping_to_cube_dictionary[destination_cube]:
                    mapping_def = mapping_to_cube.mapping
                    variable_mapping = mapping_def.variableMapping
                    variable_mapping_items = sdd_context.variable_mapping_item_dictionary[variable_mapping.variable_mapping_id]
                    for item in variable_mapping_items:
                        if item.isSource == 'false':
                            variable = item.variable
                            try:
                                if not (variable in rol_variables[destination_cube]):
                                    rol_variables[destination_cube].append(variable)
                            except KeyError:    
                                rol_variables[destination_cube] = [variable]
                    
        for destination_cube,variables in rol_variables.items():
            self.create_output_layer_for_cube_mapping(context,sdd_context,destination_cube,variables,framework,missing_domains)

        for name in missing_domains:
            print("Missing domain: " + name)

    def create_output_layer_for_cube_mapping(self,context,sdd_context,destination_cube,rol_variables,framework,missing_domains):
        ''' 
        for each cube mapping, create an output layer
        '''

        output_layer_cube = CUBE()
        output_layer_cube_cube_structure = CUBE_STRUCTURE()
        output_layer_cube.cube_id = destination_cube.replace('.','_').replace(' ','_')[2:len(destination_cube)]
        output_layer_cube.name = destination_cube.replace('.','_').replace(' ','_')[2:len(destination_cube)]
        output_layer_cube_cube_structure.cube_structure_id = destination_cube.replace('.','_').replace(' ','_')[2:len(destination_cube)] + '_cube_structure'
        output_layer_cube_cube_structure.name = destination_cube.replace('.','_').replace(' ','_')[2:len(destination_cube)] + '_cube_structure'
        output_layer_cube.cube_structure_id = output_layer_cube_cube_structure

        sdd_context.rol_cube_structure_dictionary[output_layer_cube_cube_structure.name] = output_layer_cube_cube_structure
        sdd_context.rol_cube_dictionary[output_layer_cube.name] = output_layer_cube
        if context.save_derived_sdd_items:
            output_layer_cube_cube_structure.save()
            output_layer_cube.save()
        
        
        if framework == 'FINREP_REF':
            sdd_context.finrep_output_cubes[output_layer_cube.name] = output_layer_cube
        elif framework == 'AE_REF': 
            sdd_context.ae_output_cubes[output_layer_cube.name] = output_layer_cube

        for variable in rol_variables:
            csi = CUBE_STRUCTURE_ITEM()
            csi.cube_structure_id = output_layer_cube_cube_structure
            csi.variable_id = variable
            try:
                sdd_context.rol_cube_structure_item_dictionary[csi.cube_structure_id.cube_structure_id].append(csi)
            except KeyError:
                sdd_context.rol_cube_structure_item_dictionary[csi.cube_structure_id.cube_structure_id] = [csi]
            if context.save_derived_sdd_items:
                csi.save()

           
        

