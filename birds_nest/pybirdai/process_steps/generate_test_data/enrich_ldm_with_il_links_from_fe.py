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
import csv
#from pybirdai.process_steps.generate_test_data.ldm_utils import Utils
from pybirdai.utils.utils import Utils
import os

from pybirdai.regdna import ELAttribute, ELClass, ELEnum, ELEnumLiteral, ELOperation, ELReference, ELAnnotation, ELStringToStringMapEntry

class InputLayerLinkEnricher(object):
    '''
    After the Forward Engineering process has been run on the LDM, 
    SQLDevelepor stores information about how whicj column in the Input
    Layer was created by forward engineering an attribute in the LDM.
    In SQLdeveloper these are accessed via the 'Impacty analysis'
    Feature..so we can see what is the equivelent Input Layer column
    for an LDM attribute.
    This class is responsable for adding an Annotation to the LDM attribute
    to show the name of the linked Input Layer column. The name
    is represented in a 'TableName.ColumnName' format.
    '''

    def enrich_with_links_to_input_layer_columns(self, context):
        '''
        Enrich the attributes of classes of our LDM package with an annotation
        To show what input layer column is related to LDM attribute.
        '''
        
        InputLayerLinkEnricher.create_attribute_to_column_links(self, context)
        
    def create_attribute_to_column_links(self, context):
        file_location = context.file_directory + os.sep + "ldm" + os.sep + "DM_Mappings.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    logical_object_name = row[5]
                    relational_model_name = row[8]
                    relational_object_Name = row[11]
                    entity_name = row[12]
                    table_name = row[13]
                    print(relational_model_name)
                    print(context.input_layer_name) 
                    print(entity_name)
                    print(table_name)
                    if (relational_model_name == context.input_layer_name) and (table_name is not None) and (entity_name is not None) and not (table_name.strip() == "") and not (entity_name.strip() == ""):
                        print(table_name)
                        # annotate entites
                        if logical_object_name == entity_name:
                            ldm_entity = InputLayerLinkEnricher.get_ldm_entity(
                                self, 
                                context,
                                Utils.make_valid_id(entity_name))
                            print(table_name)
                            the_entity_annotation = Utils.get_annotation_with_source(ldm_entity, "il_mapping")
                            print(table_name)
                            if the_entity_annotation is None: 
                                the_entity_annotation = ELAnnotation()
                                the_entity_annotation_directive = Utils.get_annotation_directive(ldm_entity.eContainer(), "il_mapping")
                                the_entity_annotation.source = the_entity_annotation_directive
                                ldm_entity.eAnnotations.append(the_entity_annotation)
                            print(table_name)
                            details = the_entity_annotation.details
                            print(table_name)
                            il_tables_count = 0
                            
                            for detail in details:
                                if detail.key.startswith("il_table"):
                                    il_tables_count = il_tables_count + 1
                            print(table_name)    
                            detail1 = ELStringToStringMapEntry()
                            if il_tables_count ==0:
                                detail1.key = "il_table"
                            else:
                                detail1.key = "il_table" + str(il_tables_count)
                            print(table_name)    
                            detail1.value = table_name
                            details.append(detail1)
                        else:
                            print('2')
                            # annotate attributes
                            ldm_attribute = InputLayerLinkEnricher.get_ldm_attribute(
                                self, 
                                context,
                                Utils.make_valid_id(entity_name),
                                Utils.make_valid_id(logical_object_name))
                            print('2')
                            # logical_attribute_to_relational_name[ldm_attribute] =  table_name + "." + relational_object_Name
                            if not(ldm_attribute is None):
                                if isinstance(ldm_attribute,ELAttribute):
                                    the_attribute_annotation = Utils.get_annotation_with_source(ldm_attribute, "il_mapping")
                                    if the_attribute_annotation is None: 
                                        the_attribute_annotation = ELAnnotation()
                                        the_attribute_annotation_directive = Utils.get_annotation_directive(ldm_attribute.eContainer().eContainer(), "il_mapping")
                                        the_attribute_annotation.source = the_attribute_annotation_directive
                                        ldm_attribute.eAnnotations.append(the_attribute_annotation)

                                    details = the_attribute_annotation.details
                                    detail1 = ELStringToStringMapEntry()
                                    detail1.key = "il_column"
                                    detail1.value = table_name + "." + relational_object_Name
                                    details.append(detail1)

                                if isinstance(ldm_attribute,ELReference):
                                    the_reference_annotation = Utils.get_annotation_with_source(ldm_attribute, "il_mapping")
                                    if the_reference_annotation is None: 
                                        the_reference_annotation = ELAnnotation()
                                        the_reference_annotation_directive = Utils.get_annotation_directive(ldm_attribute.eContainer().eContainer(), "il_mapping")
                                        the_reference_annotation.source = the_reference_annotation_directive
                                        ldm_attribute.eAnnotations.append(the_reference_annotation)
                                    
                                    details = the_reference_annotation.details
                                
                                    detail1 = ELStringToStringMapEntry()
                                    detail1.key = "il_column"
                                    detail1.value = relational_object_Name
                                    details.append(detail1)
                                    
                            

    def get_ldm_attribute(self, context,entity_name,attribute_name):
        for eClassifier in context.ldm_entities_package.eClassifiers:
            if isinstance(eClassifier,ELClass):
                for feature in eClassifier.eStructuralFeatures: 
                    
                    if isinstance(feature,ELAttribute) :                
                        the_entity_annotation = Utils.get_annotation_with_source(eClassifier, "long_name")
                        if the_entity_annotation is not None:
                            if the_entity_annotation.details is not None:
                                for detail in the_entity_annotation.details:
                                    if detail.key == "long_name":
                                        if detail.value == entity_name:                                        
                                            the_attribute_annotation = Utils.get_annotation_with_source(feature, "long_name")
                                            if the_attribute_annotation is not None:
                                                if the_attribute_annotation.details is not None:
                                                    for detail in the_attribute_annotation.details:
                                                        if detail.key == "long_name":
                                                            if detail.value == attribute_name:
                                                                return feature 
                    if isinstance(feature,ELReference):
                        the_reference_annotation = Utils.get_annotation_with_source(feature, "long_name")   
                        if the_reference_annotation is not None:
                            if the_reference_annotation.details is not None:
                                for detail in the_reference_annotation.details:
                                    if detail.key == "long_name":
                                        if detail.value == attribute_name:
                                            return feature 


    def get_ldm_entity(self, context,entity_name):
        for eClassifier in context.ldm_entities_package.eClassifiers:
            if isinstance(eClassifier,ELClass):
                the_entity_annotation = Utils.get_annotation_with_source(eClassifier, "long_name")
                if the_entity_annotation is not None:
                    if the_entity_annotation.details is not None:
                        for detail in the_entity_annotation.details:
                            if detail.key == "long_name":
                                if detail.value == entity_name:
                                    return eClassifier

               
                        