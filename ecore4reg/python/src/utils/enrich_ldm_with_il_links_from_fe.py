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
from utils.utils import Utils
import os

from ecore4reg import ELAttribute, ELClass, ELEnum, ELEnumLiteral, ELPublicOperation, ELReference, ELAnnotation, ELStringToStringMapEntry

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
        file_location = context.file_directory + os.sep + "DM_Mappings.csv"
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
                    if (relational_model_name == context.input_layer_name):
                        ldm_attribute = InputLayerLinkEnricher.get_ldm_attribute(
                            self, 
                            context,
                            Utils.make_valid_id(entity_name),
                            Utils.make_valid_id(logical_object_name))
                        # logical_attribute_to_relational_name[ldm_attribute] =  table_name + "." + relational_object_Name
                        if not(ldm_attribute is None):
                            if isinstance(ldm_attribute,ELAttribute):
                                eAnnotation = ELAnnotation()
                                eAnnotation.source = "impact_analysis"
                                detail1 = ELStringToStringMapEntry()
                                detail1.key = "il_column"
                                detail1.value = table_name + "." + relational_object_Name
                                eAnnotation.details.append(detail1)
                                ldm_attribute.eAnnotations = eAnnotation
                            if isinstance(ldm_attribute,ELReference):
                                eAnnotation = ELAnnotation()
                                eAnnotation.source = "impact_analysis"
                                detail1 = ELStringToStringMapEntry()
                                detail1.key = "il_column"
                                detail1.value = relational_object_Name
                                eAnnotation.details.append(detail1)
                                ldm_attribute.eAnnotations = eAnnotation
                        

    def get_ldm_attribute(self, context,entity_name,attribute_name):
        for eClassifier in context.input_tables_package.eClassifiers:
            if isinstance(eClassifier,ELClass):
                for feature in eClassifier.eStructuralFeatures:
                    if isinstance(feature,ELAttribute) and eClassifier.name == entity_name:
                        if feature.name == attribute_name:
                            return feature 
                    if isinstance(feature,ELReference):
                        if feature.name == attribute_name:
                            return feature 
                        