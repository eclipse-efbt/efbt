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
from regdna import ELAttribute, ELClass, ELReference,ELAnnotation,ELStringToStringMapEntry
import csv
from utils.utils import Utils
import os
from pickle import TRUE

class LDMRelationshipEnricher(object):
    '''
    Enrich a model which can from the SDD website,
    Create references for each relationship, mark the references
    with the separated list of target keys and separated list of source keys
    For each foreign key, annotate that attribute as FK.
    For each primary key, annotate that as private key.
    foreign cubes should have the relationship to primary cube
    '''
    def enrich(self, context):
        
        LDMRelationshipEnricher.add_ldm_relationships_between_classes(self,context)
        LDMRelationshipEnricher.import_disjoint_subtyping_information(self,context)
        LDMRelationshipEnricher.set_ldm_super_classes(self,context)
        LDMRelationshipEnricher.set_unique_ids(self,context)
        LDMRelationshipEnricher.remove_duplicate_attributes_in_subclasses(self,context)
    
    def set_unique_ids(self, context):
        '''
        for each entity in the LDM, create a class and add it to the package
        '''
        for classifier in context.ldm_entities_package.eClassifiers:
            if isinstance(classifier, ELClass): 
                if len(classifier.eSuperTypes) == 0:
                    pk_name = classifier.name + "_uniqueID"
                    attribute = ELAttribute()
                    attribute.name = pk_name
                    attribute.eType = context.types.e_string
                    attribute.eAttributeType = context.types.e_string
                    attribute.iD = True
                    attribute.lowerBound = 0
                    attribute.upperBound = 1
                    classifier.eStructuralFeatures.append(attribute)
            
        
    def import_disjoint_subtyping_information(self, context):
        '''
        we first find out which arcs are 'single arcs' , single arcs
        are ones where an entity has only only one arc and not more than 1.
        for each single arc, create a class.
        for each arc store its source in a dictionary
        for each arc target store a link from target to the arcs class
        later we will set supertypes of the targets to be the arcs class
        later we will set the arc to be a contained class of the source
        '''
        file_location = context.file_directory + os.sep + "arcs.csv"
        header_skipped = False
        # A dictionary from entity to its arcs
        entity_to_arc_dictionary = LDMRelationshipEnricher.get_entity_to_arc_dictionary(self,file_location)

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    entity_name = row[0]    
                    arc_name = row[1]
                    relation_name = row[2]
                    target_entity_name = row[3]
                    
                    altered_arc_name = Utils.make_valid_id(arc_name)
                    num_of_siblings_arcs = 0
                    try:
                        num_of_siblings_arcs = len(entity_to_arc_dictionary[entity_name]) -1
                    except KeyError:
                        pass
                    
                    if num_of_siblings_arcs > 0:
                        arc_class = None
                        try:
                            arc_class = context.arc_name_to_arc_class_map[altered_arc_name]
                        except KeyError:
                            # if the arc /source entry has not yet been added to the dictionary
                            # then we add it here, and we add the arc name
                            # and we create class for the arc
                            
                            
                            source_class = LDMRelationshipEnricher.find_class_with_long_name(self, context, Utils.make_valid_id(entity_name))
                            if (source_class is None):
                                print("could not find source class " + entity_name  + " of arc : " + altered_arc_name)
                            else:
                                arc_class = ELClass(name=altered_arc_name)
                                arc_class.eAbstract = True
                                context.arc_name_to_arc_class_map[altered_arc_name] = arc_class
                                context.arc_to_source_map[altered_arc_name] = source_class
                                context.ldm_entities_package.eClassifiers.extend([arc_class])
                                non_containment_reference = ELReference()
                                non_containment_reference.name = altered_arc_name + "_delegate"
                                non_containment_reference.eType = arc_class
                                non_containment_reference.upperBound = 1
                                non_containment_reference.lowerBound = 0
                                non_containment_reference.containment = False
                                
                                parent_reference = ELReference()
                                parent_reference.name = "parent_" + source_class.name 
                                parent_reference.eType = source_class
                                parent_reference.upperBound = 1
                                parent_reference.lowerBound = 1
                                parent_reference.containment = False
                                parent_reference.eOpposite = non_containment_reference
                                
                                non_containment_reference.eOpposite = parent_reference

                                arc_class.eStructuralFeatures.append(
                                    parent_reference)
    
                                source_class.eStructuralFeatures.append(
                                    non_containment_reference)
                            
                        if not(arc_class is None):
                            target_class = LDMRelationshipEnricher.find_class_with_long_name(self, context,Utils.make_valid_id(target_entity_name))
                            context.arc_target_to_arc_map[Utils.make_valid_id(target_entity_name)] = target_class                            
                            target_class.eSuperTypes.extend([arc_class])

    def get_entity_to_arc_dictionary(self,file_location) :
        entity_to_arc_dictionary = {}
        header_skipped = False
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    entity_name = row[0]    
                    arc_name = row[1]
                    relation_name = row[2]
                    target_entity_name = row[3]
                    
                    altered_arc_name = Utils.make_valid_id(arc_name)
                    arc_class = None
                    try:
                        arc_list = entity_to_arc_dictionary[entity_name]
                        if not (altered_arc_name in arc_list):
                            arc_list.append(altered_arc_name)
                            
                    except KeyError:
                        arc_list = [altered_arc_name]
                        entity_to_arc_dictionary[entity_name] = arc_list

            return entity_to_arc_dictionary
        
    def find_class_with_long_name(self, context, name):
        '''
        get the class with this name from the input tables package
        '''
        print("find_class_with_long_name")
        print(name)
        for eclassifier in context.ldm_entities_package.eClassifiers:
            if isinstance(eclassifier, ELClass):
                for annotation in  eclassifier.eAnnotations:
                    for detail in annotation.details: 
                        if detail.key == 'long_name':
                            if detail.value == name:
                                return eclassifier
        return None
                
    def set_ldm_super_classes(self, context):
        '''
        for each entity in the LDM, set the superclass of the class,
        but not if it already has a super class set by the disjoint subtyping
        processing
        '''
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
                    foreign_cube_id = row[11]

                    
                    if typ_relationship == 'GEN':
                        super_class = None 
                        try:                            
                            super_class = context.classes_map[primary_cube_id]
                        except KeyError:
                            pass
                        
                        sub_class = None 
                        try:                            
                            sub_class = context.classes_map[foreign_cube_id]
                        except KeyError:
                            pass
                        
                        if not(sub_class is None):
                            if len(sub_class.eSuperTypes) == 0:
                                sub_class.eSuperTypes.extend([super_class])

    def remove_duplicate_attributes_in_subclasses(self, context):
        
        for classifier in context.ldm_entities_package.eClassifiers:
            if isinstance(classifier, ELClass):
                feaures_to_remove = []
                for feature in  classifier.eStructuralFeatures:
                    if isinstance(feature, ELAttribute):
                        if LDMRelationshipEnricher.attribute_exists_in_any_superclass(self, classifier,feature, context):
                            feaures_to_remove.append(feature)
                            
                for feature_to_remove in feaures_to_remove:
                    classifier.eStructuralFeatures.remove(feature_to_remove)                                    

    def attribute_exists_in_any_superclass(self, el_class,attribute, context):
        return_value = False
        if len(el_class.eSuperTypes) > 0:
            super_class = el_class.eSuperTypes[0]
            for feature in super_class.eStructuralFeatures:
                if isinstance(feature, ELAttribute):
                    if feature.name == attribute.name:
                        return_value = True 
        
            if return_value == False:
                return LDMRelationshipEnricher.attribute_exists_in_any_superclass(self, super_class,attribute, context)
            else:
                return True
                         
        else:
            return False               
                
            
        
    def add_ldm_relationships_between_classes(self, context):
        '''
        For each relationship in the LDM, add a reference between the relevant classes
        '''
        file_location = context.file_directory + os.sep + "DM_Relations.csv"
        header_skipped = False
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:

                    source_to_target_cardinality = row[10]
                    source_class_name = row[6]
                    target_class_name = row[7]
                    # not that source optional actually means that the target is optional
                    # which is confusing, we can see that in the SQL developer diagrams
                    # where we have the circle (meaning options) on the target side
                    # of the realtionship when source_optional is true
                    source_optional = row[12]
                    target_optional = row[13]
                    relation_name = row[0]
                    #identifying = row[15]

                    reference_name = ""
                    if target_optional == "N":
                        reference_name = Utils.make_valid_id(relation_name)+"_2wayassociation"
                    else:
                        reference_name = Utils.make_valid_id(relation_name)+"_association"


                    the_class =  LDMRelationshipEnricher.find_class_with_long_name(self, context, Utils.make_valid_id(source_class_name))

                    target_class = LDMRelationshipEnricher.find_class_with_long_name(self, context, Utils.make_valid_id(target_class_name))

                    if (not (the_class is None) ) and (not (target_class is None)):
                        ereference = ELReference()
                        ereference.name = reference_name
                        ereference.eType = target_class
    
                        if target_optional == "N":
                            ereference.containment = False
                            # the_reference_annotation = ELAnnotation()
                            # the_reference_annotation_directive = Utils.get_annotation_directive(the_class.eContainer(), "relationship_type")
                            # the_reference_annotation.source = the_reference_annotation_directive
                            # details = the_reference_annotation.details
                            # mapentry  = ELStringToStringMapEntry()
                            # mapentry.key = "is_identifying_relationship"
                            # mapentry.value = "true"
                            # details.append(mapentry)
                            # ereference.eAnnotations.append(the_reference_annotation)
                            linked_reference = ELReference()
                            linked_reference.name = "linked_" + the_class.name 
                            linked_reference.eType = the_class
                            linked_reference.upperBound = 1
                            linked_reference.lowerBound = 1
                            linked_reference.containment = False
                            linked_reference.eOpposite = ereference
                            
                            ereference.eOpposite = linked_reference

                            target_class.eStructuralFeatures.append(
                                linked_reference)
                                                        
    
                        else:
                            ereference.containment = False
                            #the_reference_annotation = ELAnnotation()
                            #the_reference_annotation_directive = Utils.get_annotation_directive(the_class.eContainer(), "relationship_type")
                            #the_reference_annotation.source = the_reference_annotation_directive
                            #details = the_reference_annotation.details
                            #mapentry  = ELStringToStringMapEntry()
                            #mapentry.key = "is_association_relationship"
                            #mapentry.value = "true"
                            #details.append(mapentry)
                            #ereference.eAnnotations.append(the_reference_annotation)
                            
                            the_associated_class_annotation = ELAnnotation()
                            the_associated_class_directive = Utils.get_annotation_directive(the_class.eContainer(), "relationship_type")
                            the_associated_class_annotation.source = the_associated_class_directive
                            details = the_associated_class_annotation.details
                            mapentry  = ELStringToStringMapEntry()
                            mapentry.key = "is_associated_with"
                            mapentry.value = the_class.name + "." + ereference.name
                            details.append(mapentry)
                            target_class.eAnnotations.append(the_associated_class_annotation)
    
                        if source_optional.strip() == "Y":
                            if source_to_target_cardinality.strip() == "*":                            
                                # upper bound of -1 means there is no upper bounds, 
                                # so represents an open list of reference
                                ereference.upperBound = -1
                                ereference.lowerBound = 0
    
                            else:
                                ereference.upperBound = 1
                                ereference.lowerBound = 0
                        else:
                            if source_to_target_cardinality.strip() == "*":
    
                                ereference.upperBound = -1
                                ereference.lowerBound = 1
                            else:
                                ereference.upperBound = 1
                                ereference.lowerBound = 1
    
                            
                        if not the_class is None:
                            the_class.eStructuralFeatures.append(ereference)
    
        