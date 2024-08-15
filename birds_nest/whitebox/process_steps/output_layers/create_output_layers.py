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
from whitebox.utils.utils import Utils

from whitebox.regdna import ELClass,ELOperation,ELAnnotation,ELStringToStringMapEntry
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

        output_layer = ELClass()
        
        output_layer.name = destination_cube.replace('.','_').replace(' ','_')[2:len(destination_cube)] + "_OutputItem"
        if framework == 'FINREP_REF':
            context.finrep_output_tables_package.eClassifiers.append(output_layer)
        elif framework == 'AE_REF': 
            context.ae_output_tables_package.eClassifiers.append(output_layer)

        for variable in rol_variables:
            domain = variable.domain_id
            domain_name = domain.domain_id
            #find the domain in existing domains, if i does not exist then reaise an error.
            enum  = None
            for domain in context.ldm_domains_package.eClassifiers:
                if domain.name == domain_name + '_domain':
                    enum = domain
            name = domain_name + '_domain'       
            if not (enum is None):
                name = enum.name

            the_attribute_name = variable.variable_id
            operation = ELOperation()
            operation.lowerBound=0
            operation.upperBound=1
            if(name == "String"):
                operation.name = the_attribute_name
                operation.eType = context.types.e_string
            elif(name == "Date"):
                operation.name = the_attribute_name
                operation.eType = context.types.e_date
            elif(name.startswith("String_")):
                operation.name = the_attribute_name
                operation.eType = context.types.e_string
            elif(name == "STRNG_domain"):
                    operation.name = the_attribute_name
                    operation.eType = context.types.e_string
            elif(name == "EBA_String_domain"):
                    operation.name = the_attribute_name
                    operation.eType = context.types.e_string
            elif(name == "DT_domain"):
                    operation.name = the_attribute_name
                    operation.eType = context.types.e_date
            elif(name == "Number"):
                operation.name = the_attribute_name
                operation.eType = context.types.e_double
            elif(name == "RL_domain"):
                operation.name = the_attribute_name
                operation.eType = context.types.e_double
            
            elif(name.startswith("Real_")):
                operation.name = the_attribute_name
                operation.eType = context.types.e_double
            elif(name.startswith("Monetary")): 
                operation.name = the_attribute_name
                operation.eType = context.types.e_int
            elif(name.startswith("INTGR_domain")): 
                operation.name = the_attribute_name
                operation.eType = context.types.e_int
            elif(name.startswith("MNTRY")): 
                operation.name = the_attribute_name
                operation.eType = context.types.e_int
            elif(name.startswith("Monetary_domain")): 
                operation.name = the_attribute_name
                operation.eType = context.types.e_int
            elif(name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                operation.name = the_attribute_name
                operation.eType = context.types.e_int
            elif(name.startswith("Non_negative_integers")): 
                operation.name = the_attribute_name
                operation.eType = context.types.e_int
            elif(name.startswith("All_possible_dates")):   
                operation.name = the_attribute_name
                operation.eType = context.types.e_date  
            else:
                operation.name = the_attribute_name
                operation.eType = enum  
            
            if operation.eType is None:
                if not (name in missing_domains):
                    missing_domains.append(name)    
            else:
                the_long_name_annotation = ELAnnotation()
                the_long_name_directive = Utils.get_annotation_directive(output_layer.eContainer(), "long_name")
                the_long_name_annotation.source = the_long_name_directive
                details = the_long_name_annotation.details
                mapentry  = ELStringToStringMapEntry()
                mapentry.key = "long_name"
                mapentry.value = Utils.make_valid_id(variable.displayName)
                details.append(mapentry)

                operation.name = variable.variable_id
                output_layer.eOperations.append(operation)

        if framework == 'FINREP_REF':
            context.finrep_output_tables_package.eClassifiers.append(output_layer)
        elif framework == 'AE_REF': 
            context.ae_output_tables_package.eClassifiers.append(output_layer)
        

