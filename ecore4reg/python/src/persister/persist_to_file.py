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


from ecore4reg import ELAttribute, ELClass, ELEnum, ELPublicOperation, ELReference,SelectColumnAttributeAs


class PersistToFile:
    '''
    Documentation for PersistToFile
    '''

    def save_model_as_ecore4reg_file(self, context):
        '''
        Save resources as Ecore4Reg files
        '''
        if context.load_eil_from_website:
            PersistToFile.persist_entity_model(
                self, context, context.input_tables_package,
                "ecore4reg", context.sdd_domains_package)
        else:        
            PersistToFile.persist_entity_model(
                self, context, context.input_tables_package,
                "ecore4reg", context.il_domains_package)

        PersistToFile.persist_entity_model(
            self, context, context.output_tables_package,
            "ecore4reg", context.sdd_domains_package)
        PersistToFile.persist_enum_model(
            self, context, context.il_domains_package, "ecore4reg")
        PersistToFile.persist_enum_model(
            self, context, context.sdd_domains_package, "ecore4reg")
        PersistToFile.persist_types_model(
            self, context, context.types_package, "ecore4reg")
        

        PersistToFile.persist_enum_model(
            self, context, context.il_domains_package, "ecore4reg")
        PersistToFile.persist_enum_model(
            self, context, context.sdd_domains_package, "ecore4reg")
        PersistToFile.persist_types_model(
            self, context, context.types_package, "ecore4reg")
        PersistToFile.persist_generation_transformations_to_csv(self, context)
        PersistToFile.persist_generation_transformations(self, context)
        
        
        
    def save_model_as_xcore_file(self, context):
        '''
        Save resources as XCore files
        '''
        if context.load_eil_from_website:
            PersistToFile.persist_entity_model(
                self, context, context.input_tables_package,
                "xcore", context.sdd_domains_package)
        else:
            PersistToFile.persist_entity_model(
                self, context, context.input_tables_package,
                "xcore", context.il_domains_package)
        PersistToFile.persist_entity_model(
            self, context, context.output_tables_package,
            "xcore", context.sdd_domains_package)
        PersistToFile.persist_enum_model(
            self, context, context.il_domains_package, "xcore")
        PersistToFile.persist_enum_model(
            self, context, context.sdd_domains_package, "xcore")
        

    def persist_entity_model(self, context, the_package, extension, imported_package):
        '''
        Documentation for persist_entity_model
        '''

        f = open(context.output_directory + os.sep + extension +
                 os.sep + the_package.name + '.' + extension,
                 "a",  encoding='utf-8')
        f.write("\t\t package " + the_package.name + "\r")
        f.write("\t\t import " + imported_package.name + ".*\r")
        if the_package == context.output_tables_package:
            for import_string in context.import_logic_strings:
                f.write("\t\t import " + import_string + ".*\r")
        if extension == "ecore4reg":
            f.write("\t\t import types.*\r")
        for classifier in the_package.eClassifiers:
            if isinstance(classifier, ELClass):
                f.write("\t\t\t")
                if classifier.eAbstract:
                    f.write("abstract ")
                f.write("class " + classifier.name)
                if (hasattr(classifier, "eSuperTypes") and len(classifier.eSuperTypes) > 0):
                    f.write(" extends " + classifier.eSuperTypes[0].name)
                f.write(" {\r")
                for member in classifier.eStructuralFeatures:

                    if isinstance(member, ELReference):
                        if member.containment:
                            f.write("\t\t\t\tcontains ")
                        else:
                            f.write("\t\t\t\trefers ")

                        f.write(member.eType.name + " ")
                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            upperBoundNumber = member.upperBound
                            upperBoundString = str(member.upperBound)
                            if (extension == "xcore") and (upperBoundNumber == -1):
                                upperBoundString = "*"
                            
                            f.write("[" + str(member.lowerBound) +
                                    ".." + upperBoundString + "] ")

                        f.write(member.name)
                        f.write(" \r")
                    elif isinstance(member, ELAttribute):
                        f.write("\t\t\t\t")
                        if member.iD:
                            f.write("id ")

                        if member.eType.name == "String":
                            f.write("String  ")
                        elif member.eType.name == "Double":
                            f.write("double  ")
                        elif member.eType.name == "Integer":
                            f.write("int  ")
                        elif member.eType.name == "Date":
                            f.write("Date  ")
                        else:
                            f.write(member.eType.name + " ")

                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            upperBoundNumber = member.upperBound
                            upperBoundString = str(member.upperBound)
                            if (extension == "xcore") and (upperBoundNumber == -1):
                                upperBoundString = "*"
                            
                            f.write("[" + str(member.lowerBound) +
                                    ".." + upperBoundString + "] ")

                        f.write(member.name)
                        f.write(" \r")

                for member in classifier.eOperations:
                    if isinstance(member, ELPublicOperation):
                        f.write("\t\t\t\top ")

                        f.write(member.eType.name + " ")
                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            upperBoundNumber = member.upperBound
                            upperBoundString = str(member.upperBound)
                            if (extension == "xcore") and (upperBoundNumber == -1):
                                upperBoundString = "*"
                            
                            f.write("[" + str(member.lowerBound) +
                                    ".." + upperBoundString + "] ")
                            
                        f.write(member.name)
                        if (extension == "xcore") and (member.eType.name == "int"):
                            f.write("() { return 0}")
                        else:
                            f.write("() {}")
                            
                        f.write(" \r")

                f.write("\t\t\t}\r")

        f.close()

    def persist_types_model(self, context, the_package, extension):
        '''
        Documentation for persist_types_model
        '''
        f = open(context.output_directory + os.sep + extension +
                 os.sep + the_package.name +
                 '.' + extension, "a",  encoding='utf-8')
        f.write("\t\t package " + the_package.name + "\r")
        f.write("\t\t\ttype double wraps double\r")
        f.write("\t\t\ttype String wraps String\r")
        f.write("\t\t\ttype int wraps int\r")
        if extension == "ecore4reg":
            f.write("\t\t\ttype Date wraps Date\r")
        else:
            f.write("\t\t\ttype Date wraps java.util.Date\r")
        f.write("\t\t\ttype boolean wraps boolean\r")
        f.close()

    def persist_enum_model(self, context, the_package, extension):
        '''
        Documentation for persist_enum_model
        '''
        f = open(context.output_directory + os.sep + extension +
                 os.sep + the_package.name +
                 '.' + extension, "a",  encoding='utf-8')
        f.write("\t\t package " + the_package.name + "\r")
        for classifier in the_package.eClassifiers:

            if isinstance(classifier, ELEnum):
                f.write("\t\t\tenum " + classifier.name)

                f.write(" { ")
                counter = 0
                splitcount = 1
                for the_literal in classifier.eLiterals:
                    counter = counter+1

                    if counter < 100:
                        f.write(" " + the_literal.name + " as \"" +
                                the_literal.literal + "\" = " + str(the_literal.value))
                    else:
                        counter = 0
                        splitcount = splitcount+1
                        f.write(" }\r")
                        f.write("\t\t\tenum " + classifier.name +
                                "_" + str(splitcount))
                        f.write(" { ")

                f.write(" }\r")

        f.close()

    
    def count_non_none_layers(self, element):
        '''
        Documentation for count_non_none_layers
        '''
        counter = 0
        for layer in element.selectionLayers:
            if not layer.name is None:
                counter = counter + 1
        return counter

    
    def save_model_as_json_files(self, context):
        '''
         save model as an json file representing an object tree.
        '''
        extension = 'json'
        rset2 = ResourceSet()  # We have a resource set
        # we register the factory for '.json' extensions
        rset2.resource_factory['json'] = JsonResource

        il_domains_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "types.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        il_domains_resource2.append(context.types_package)
        il_domains_resource2.save()
        il_domains_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "il_domains.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        il_domains_resource2.append(context.il_domains_package)
        il_domains_resource2.save()
        sdd_domains_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "sdd_domains.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        sdd_domains_resource2.append(context.sdd_domains_package)
        sdd_domains_resource2.save()
        input_tables_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "input_tables.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        input_tables_resource2.append(
            context.input_tables_package)
        input_tables_resource2.save()
        output_tables_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "output_tables.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        output_tables_resource2.append(
            context.output_tables_package)
        output_tables_resource2.save()

   
    
    def persist_generation_transformations_to_csv(self, context):
        '''
        Documentation for persist_generation_transformations
        '''
        rules_for_reports = context.generation_rules_module.rulesForReport
        report_to_table_parts_file = open(context.output_directory + os.sep + 'generations_transformations_csv' +
                         os.sep + 
                         'report_to_table_parts.csv', "a",  encoding='utf-8')
        report_to_table_parts_file.write("Report,Table Part,Notes\n")

        for rules_for_report in rules_for_reports:
            if not(rules_for_report.outputLayerCube is None): #column.attribute.eContainer().name + "." + column.attribute.name
                
                template = rules_for_report.outputLayerCube.name
                amended_template_name =  template[0:len(template) - 11]
                f = open(context.output_directory + os.sep + 'generations_transformations_csv' +
                         os.sep + 
                         amended_template_name + '.csv', "a",  encoding='utf-8')
                f.write("Template,Table Part,Main Table,Filter,Lineage type,Source Table,Source Column,Missing,Relevant, Derived,Domain,Member,Value,ROL Cube Item,Notes\n")

                for layer in rules_for_report.rulesForTable:
                    table = layer.inputLayerTable.name
                    for table_part in layer.rulesForTablePart:
                        main_catagory = table_part.main_catagory
                        main_catagory_name = context.main_catogory_to_name_map[main_catagory]
                        
                        table_and_part = table_part.table_and_part_tuple
                        report_to_table_parts_file.write(amended_template_name + "," + table_part.name + ",\n")
                        filter = context.table_parts_to_to_filter_map[table_and_part]
                        for column in table_part.columns:
                            if isinstance(column, SelectColumnAttributeAs) and not(column.attribute is None):
                                entity  = column.attribute.eContainer().name
                                attribute = column.attribute.name
                                lineage_type = "attribute"
                                missing = "Not Missing"
                            else:  
                                entity  = ""
                                attribute = ""
                                missing = "Missing"
                                lineage_type = "tbd"
                                
                            variable_id = column.asAttribute.name

                            
                            f.write(amended_template_name +"," + table_part.name +","  +table+"," + filter + ","  +lineage_type+"," +entity+"," +attribute+"," +missing+",,,,,," +variable_id + ",\n")

                f.close()
        report_to_table_parts_file.close

    def persist_generation_transformations(self, context):
        '''
        Documentation for persist_generation_transformations
        '''
        rules_for_reports = context.generation_rules_module.rulesForReport
        for rules_for_report in rules_for_reports:
            if not(rules_for_report.outputLayerCube is None):
                f = open(context.output_directory + os.sep + 'ecore4reg' +
                         os.sep + rules_for_report.outputLayerCube.name +
                         '.ecore4reg', "a",  encoding='utf-8')
                f.write("generationRuleModule " + rules_for_report.outputLayerCube.name + "_generationModule\r{\r")
                f.write("\tgenerationRules " + "{\r")
                f.write("\t\treport output_tables." + rules_for_report.outputLayerCube.name + "{\r")
                for layer in rules_for_report.rulesForTable:
                    if layer.inputLayerTable is None:
                        f.write("\t\t\tILTable None {\r")
                    else:
                        f.write("\t\t\tILTable input_tables." + layer.inputLayerTable.name + "{\r")
                    for table_part in layer.rulesForTablePart:
                        if table_part.name is None:
                            f.write("\t\t\t\ttablePart None { \r")
                        else:
                            f.write("\t\t\t\ttablePart " + Utils.make_valid_id(table_part.name) + " { \r")
                        for column in table_part.columns:
                            if isinstance(column, SelectColumnAttributeAs) and not(column.attribute is None):
                                f.write("\t\t\t\t\tselectAttribute input_tables." + column.attribute.eContainer().name + "." + column.attribute.name)
                            else:  
                                f.write("\t\t\t\t\tselectValue \"TODO\"")
                            f.write(" as output_tables." +
                                    rules_for_report.outputLayerCube.name + "." + column.asAttribute.name + "\r")
                        f.write("\t\t\t\t}\r")
                        
                    f.write("\t\t\t}\r")
                f.write("\t\t}\r")
                f.write("\t}\r")
                f.write("}\r")
               
                f.close()
            
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
  
                    
    def save_model_as_ecore_file(self, context):
        '''
         save model as an xmi file representing an object tree.
        '''
        rset2 = ResourceSet()
        extension = 'ecore'
        il_domains_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "il_domains.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        il_domains_resource2.append(context.il_domains_ecore_package)
        il_domains_resource2.save()
        PersistToFile.hot_fix(self, context.output_directory +
                              os.sep + extension +
                              os.sep + "il_domains.ecore")
        
        sdd_domains_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "sdd_domains.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        sdd_domains_resource2.append(context.sdd_domains_ecore_package)
        sdd_domains_resource2.save()
        PersistToFile.hot_fix(self, context.output_directory +
                              os.sep + extension +
                              os.sep + "sdd_domains.ecore")
        
        input_tables_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "input_tables.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        input_tables_resource2.append(
            context.input_tables_ecore_package)
        input_tables_resource2.save()
        PersistToFile.hot_fix(self, context.output_directory +
                              os.sep + extension +
                              os.sep + "input_tables.ecore")
        output_tables_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "output_tables.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        output_tables_resource2.append(
            context.output_tables_ecore_package)
        output_tables_resource2.save()
        PersistToFile.hot_fix(self, context.output_directory +
                              os.sep + extension +
                              os.sep + "output_tables.ecore")
    

    def hot_fix(self, file_name):
        '''
        Documentation for hot_fix, PYEcore seems to want to use the last
        model it used when persisting Enums
        '''
        f = open(file_name, "r", encoding='utf-8')
        text = f.read()
        amended_text = text.replace('ecore4reg:EEnum', 'ecore:EEnum').replace('sdd_model:EEnum', 'ecore:EEnum').replace('sdd_model:EDataType', 'ecore:EDataType')
        f.close()
        f1 = open(file_name, "w", encoding='utf-8')
        f1.write(amended_text)
        f.close()

    def remove_comment_chars(self, the_string):
        '''
        Documentation for remove_comment_chars
        '''
        return the_string.replace("/**", "").replace("/*", "").replace("*/", "")
