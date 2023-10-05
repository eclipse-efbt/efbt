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
from ecore4reg import ELAttribute, ELClass, ELReference,ELAnnotation,ELStringToStringMapEntry
import csv
from utils.utils import Utils
import os

class RelationshipEnricher(object):
    '''
    Enrich a model which can from the SDD website,
    Create references for each relationship, mark the references
    with the separated list of target keys and separated list of source keys
    For each foreign key, annotate that attribute as FK.
    For each primary key, annotate that as private key.
    foreign cubes should have the relationship to primary cube
    '''
    def enrich(self, context):
        
        file_location = context.file_directory + os.sep + "cube_relationship.csv"
        header_skipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    typ_relationship = row[5]
                    primary_cube_id = row[9]
                    primary_cube_variable_code = row[10]
                    foreign_cube_id = row[11]
                    foreign_cube_variable_code = row[12]
                    primary_cube_cardinality = row[13]
                    foreign_cube_cardinality = row[14]
                    primary_cube_mandatoriness = row[15]
                    foreign_cube_mandatoriness =row[16]
                    
                    if typ_relationship == 'ASS':
                        foreign_class = None 
                        try:                            
                            foreign_class = context.classes_map[foreign_cube_id]
                        except KeyError:
                            pass
                   
                        if not (foreign_class is None):
                            relationship_name = 'the_' + primary_cube_id
                            
                            primary_class = context.classes_map[primary_cube_id]                    # add the relationship to the class if it does not exist
                            # and set its mandatoriness and its cardinality
                            the_reference = None 
                            for reference in foreign_class.eStructuralFeatures:
                                if reference.name == relationship_name:
                                    the_reference = reference
                                    
                            if the_reference is None:
                                the_reference = ELReference()
                                the_reference.name = relationship_name
                                if primary_cube_mandatoriness == 'TRUE':
                                    the_reference.lowerBound = 1
                                else:
                                    the_reference.lowerBound = 0
                                    
                                if primary_cube_cardinality == '1':
                                    the_reference.upperBound = 1
                                else:
                                    the_reference.upperBound = -1
                                    
                                the_reference.eType = primary_class
                                
                                foreign_class.eStructuralFeatures.append(the_reference)
        
                            # add the foreign_key field list if it does not exist
                            # append to the foreign key list
                            the_reference_annotation = the_reference.eAnnotations
                            if the_reference_annotation is None:
                                the_reference_annotation = ELAnnotation()
                                the_reference.eAnnotations = the_reference_annotation
                                
                            details = the_reference_annotation.details
                            
                            foreign_key_field_list = None 
                            
                            for key_value_pair in details.items:
                                if key_value_pair.key == 'foreign_key_field_list':
                                    foreign_key_field_list = key_value_pair
                                    foreign_key_field_list.value=foreign_key_field_list.value + ":" + foreign_cube_variable_code
                                    
                            if foreign_key_field_list is None:
                                foreign_key_field_list = ELStringToStringMapEntry()
                                foreign_key_field_list.key = "foreign_key_field_list"
                                foreign_key_field_list.value = foreign_cube_variable_code
                                details.append(foreign_key_field_list)
        
                                
                            primary_key_field_list = None 
                            
                            for key_value_pair in details.items:
                                if key_value_pair.key == 'primary_key_field_list':
                                    primary_key_field_list = key_value_pair
                                    primary_key_field_list.value=primary_key_field_list.value + ":" + primary_cube_variable_code
                                    
                            if primary_key_field_list is None:
                                primary_key_field_list = ELStringToStringMapEntry()
                                primary_key_field_list.key = "primary_key_field_list"
                                primary_key_field_list.value = primary_cube_variable_code
                                details.append(primary_key_field_list)
        
                            # find the related attributes and set their FK or PK annotation
                            for attribute in foreign_class.eStructuralFeatures:
                                if attribute.name == foreign_cube_variable_code:
                                    the_attribute_annotation = attribute.eAnnotations
                                    if the_attribute_annotation is None:
                                        the_attribute_annotation = ELAnnotation()
                                        attribute.eAnnotations = the_attribute_annotation
                                    foreign_key = ELStringToStringMapEntry()
                                    foreign_key.key = "foreign_key"
                                    foreign_key.value = "foreign_key"
                                    the_attribute_annotation.details.append(foreign_key) 
                                    
                            for attribute in primary_class.eStructuralFeatures:
                                if attribute.name == primary_cube_variable_code:
                                    the_attribute_annotation = attribute.eAnnotations
                                    if the_attribute_annotation is None:
                                        the_attribute_annotation = ELAnnotation()
                                        attribute.eAnnotations = the_attribute_annotation
                                    primary_key = ELStringToStringMapEntry()
                                    primary_key.key = "primary_key"
                                    primary_key.value = "primary_key"
                                    the_attribute_annotation.details.append(primary_key)     
          
                