# coding=UTF-8#
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
#
import os
import csv
from pybirdai.utils.utils import Utils

from pybirdai.regdna import ELAttribute, ELClass, ELEnum
from pybirdai.regdna import ELEnumLiteral, ELReference
from pybirdai.regdna import ELAnnotation, ELStringToStringMapEntry

class SQLDevLDMImport(object):
    '''
    Documentation for SQLDevLDMImport
    '''

    def do_import(self, context):
        '''
        import the items from the BIRD LDM csv files
        '''
        SQLDevLDMImport.add_ldm_classes_to_package(self, context)
        SQLDevLDMImport.import_disjoint_subtyping_information(self, context)
        SQLDevLDMImport.set_ldm_super_classes(self, context)
        SQLDevLDMImport.add_ldm_enums_to_package(self, context)
        SQLDevLDMImport.add_ldm_literals_to_enums(self, context)
        SQLDevLDMImport.create_ldm_types_map(self, context)
        SQLDevLDMImport.add_ldm_attributes_to_classes(self, context)
        SQLDevLDMImport.remove_enums_not_used_by_attributes(self, context)
        SQLDevLDMImport.add_ldm_relationships_between_classes(self, context)
        SQLDevLDMImport.remove_duplicate_attributes_in_subclasses(self, context)
        SQLDevLDMImport.mark_root_class_as_entity_group_annotation(self, context)

    def add_ldm_classes_to_package(self, context):
        '''
        for each entity in the LDM, create a class and add it to the package
        '''
        file_location = context.file_directory + os.sep + 'ldm' + os.sep + "DM_Entities.csv"

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

                    entity_name = row[0] 
                    object_id = row[1]
                    engineering_type = row[27]
                    num_supertype_entity_id = row[26]
                    preferred_abbreviation = row[24]
                    class_name = preferred_abbreviation
                        
                    altered_class_name = Utils.make_valid_id(class_name)
                    eclass = ELClass(name=altered_class_name)  
                    eclass.original_name = entity_name
                       

                    context.ldm_entities_package.eClassifiers.extend([
                                                                          eclass])
                    the_long_name_annotation = ELAnnotation()
                    the_long_name_directive = Utils.get_annotation_directive(eclass.eContainer(), "long_name")
                    the_long_name_annotation.source = the_long_name_directive
                    details = the_long_name_annotation.details
                    mapentry  = ELStringToStringMapEntry()
                    mapentry.key = "long_name"
                    mapentry.value = Utils.make_valid_id(entity_name)
                    details.append(mapentry)
                    eclass.eAnnotations.append(the_long_name_annotation)

                    # if the class is a not a subtype, then we need to add a primary key
                    # subtypes will inherit the primary key from their supertype
                    if (num_supertype_entity_id == "") or (num_supertype_entity_id is None):
                        pk_name = altered_class_name + "_uniqueID"
                        attribute = ELAttribute()
                        attribute.name = pk_name
                        attribute.eType = context.types.e_string
                        attribute.eAttributeType = context.types.e_string
                        attribute.iD = True
                        attribute.lowerBound = 0
                        attribute.upperBound = 1
                        eclass.eStructuralFeatures.append(attribute)

                    # maintain a map a objectIDs to ELClasses
                    context.classes_map[object_id] = eclass

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
        file_location = context.file_directory + os.sep + "ldm" + os.sep + "arcs.csv"
        header_skipped = False
        # A dictionary from entity to its arcs
        entity_to_arc_dictionary = SQLDevLDMImport.get_entity_to_arc_dictionary(self,context,file_location)

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
                            
                            arc_class = ELClass(name=altered_arc_name)
                            arc_class.eAbstract = True
                            source_class = SQLDevLDMImport.find_class_with_long_name(self, context, Utils.make_valid_id(entity_name))
                            context.arc_name_to_arc_class_map[altered_arc_name] = arc_class
                            context.arc_to_source_map[altered_arc_name] = source_class
                            context.ldm_entities_package.eClassifiers.extend([arc_class])
                            non_containment_reference = ELReference()
                            non_containment_reference.name = altered_arc_name + "_delegate"
                            non_containment_reference.eType = arc_class
                            non_containment_reference.upperBound = 1
                            non_containment_reference.lowerBound = 0
                            non_containment_reference.containment = False
                            pk_name = altered_arc_name + "_uniqueID"
                            attribute = ELAttribute()
                            attribute.name = pk_name
                            attribute.eType = context.types.e_string
                            attribute.eAttributeType = context.types.e_string
                            attribute.iD = True
                            attribute.lowerBound = 0
                            attribute.upperBound = 1
                            the_reference_annotation = ELAnnotation()
                            the_reference_annotation_directive = Utils.get_annotation_directive(source_class.eContainer(), "relationship_type")
                            the_reference_annotation.source = the_reference_annotation_directive
                            details = the_reference_annotation.details
                            mapentry  = ELStringToStringMapEntry()
                            mapentry.key = "is_identifying_relationship"
                            mapentry.value = "true"
                            details.append(mapentry)
                            non_containment_reference.eAnnotations.append(the_reference_annotation)

                            arc_class.eStructuralFeatures.append(attribute)
                            the_identified_class_annotation = ELAnnotation()
                            the_identified_class_directive = Utils.get_annotation_directive(source_class.eContainer(), "relationship_type")
                            the_identified_class_annotation.source = the_identified_class_directive
                            details = the_identified_class_annotation.details
                            mapentry  = ELStringToStringMapEntry()
                            mapentry.key = "is_identified_by"
                            mapentry.value = source_class.name + "." + non_containment_reference.name
                            details.append(mapentry)
                            arc_class.eAnnotations.append(the_identified_class_annotation)

                            source_class.eStructuralFeatures.append(
                                non_containment_reference)
                            
                        
                        target_class = SQLDevLDMImport.find_class_with_long_name(self, context,Utils.make_valid_id(target_entity_name))
                        context.arc_target_to_arc_map[Utils.make_valid_id(target_entity_name)] = target_class
                        target_class.eSuperTypes.extend([arc_class])

    def get_entity_to_arc_dictionary(self,context,file_location) :
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

            context.entity_to_arc_dictionary = entity_to_arc_dictionary
            return entity_to_arc_dictionary
        
    def find_class_with_name(self, context, name):
        '''
        get the class with this name from the input tables package
        '''
        for eclassifier in context.ldm_entities_package.eClassifiers:
            if isinstance(eclassifier, ELClass):
                if eclassifier.name == name:
                    return eclassifier
                
    def find_class_with_long_name(self, context, name):
        '''
        get the class with this name from the input tables package
        '''

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
        file_location = context.file_directory + os.sep + 'ldm' + os.sep + "DM_Entities.csv"
        header_skipped = False

        # Where an entity has a superclass, set the superclass on the ELClass
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    class_id = row[1]
                    superclass_id = row[25]
                    if not (len(superclass_id.strip()) == 0):
                        theclass = context.classes_map[class_id]
                        superclass = context.classes_map[superclass_id]
                        if len(theclass.eSuperTypes) == 0:
                            theclass.eSuperTypes.extend([superclass])

    def add_ldm_enums_to_package(self, context):
        '''
        for each domain in the LDM add an enum to the package
        '''
        file_location = context.file_directory + os.sep + 'ldm' + os.sep + "DM_Domains.csv"
        header_skipped = False
        counter = 0
        # Create an ELEnum for each domain, and add it to the ELPackage
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    counter = counter+1
                    enum_id = row[0]
                    enum_name = row[1]
                    synonym = row[3]
                    adapted_enum_name = Utils.make_valid_id(synonym)
                    the_enum = ELEnum()
                    the_enum.name = adapted_enum_name + "_domain"
                    # maintain a map of enum IDS to ELEnum objects
                    context.enum_map[enum_id] = the_enum
                    context.ldm_domains_package.eClassifiers.extend([
                                                                        the_enum])

    def add_ldm_literals_to_enums(self, context):
        '''
        for each memebr of a domain the LDM, add a literal to the corresponding enum
        '''
        file_location = context.file_directory + os.sep + 'ldm' + os.sep + "DM_Domain_AVT.csv"
        header_skipped = False
        counter = 0
        # Add the members of a domain as literals of the related Enum
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    try:
                        counter = counter+1
                        enum_id = row[0]
                        adapted_enum_name = Utils.make_valid_id_for_literal( row[3])
                        value = row[4]
                        adapted_value = Utils.make_valid_id( value)
                        try:
                            the_enum = context.enum_map[enum_id]
                            new_adapted_value = Utils.unique_value(
                                 the_enum, adapted_value)
                            new_adapted_name = Utils.unique_name(
                                 the_enum, adapted_enum_name)
                            enum_literal = ELEnumLiteral()
                            enum_literal.name = new_adapted_value
                            enum_literal.literal = new_adapted_name
                            enum_literal.value = counter
                            the_enum.eLiterals.extend([enum_literal])
                            context.enum_literals_map[the_enum.name+":" + enum_literal.literal] = enum_literal
                        except KeyError:
                            print("missing domain: " + enum_id)

                    except IndexError:
                        print(
                            "row in DM_Domain_AVT.csv skipped  due to improper formatting at row number")
                        print(counter)

    def create_ldm_types_map(self, context):
        '''
        for each type in the LDM, create a type in the ELPackage
        '''
        # for each logicalDatatype for orcle 12c, make a Datatype if we have an
        # equivalent

        file_location = context.file_directory + os.sep + 'ldm' + os.sep + "DM_Logical_To_Native.csv"
        header_skipped = False
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    rdbms_type = row[3]
                    rdbms_version = row[4]
                    datatype_id = row[0]
                    if (rdbms_type.strip() == "Oracle Database") and (rdbms_version.strip() == "12cR2"):
                        native_type = row[2]

                        if native_type.strip() == "VARCHAR":

                            context.datatype_map[datatype_id] = context.types.e_string

                        if native_type.strip() == "VARCHAR2":

                            context.datatype_map[datatype_id] = context.types.e_string

                        if native_type.strip() == "INTEGER":

                            context.datatype_map[datatype_id] = context.types.e_int

                        if native_type.strip() == "DATE":

                            context.datatype_map[datatype_id] = context.types.e_date

                        if native_type.strip() == "NUMBER":

                            context.datatype_map[datatype_id] = context.types.e_double

                        if native_type.strip() == "UNKNOWN":

                            context.datatype_map[datatype_id] = context.types.e_string

    def add_ldm_attributes_to_classes(self, context):
        '''
        For each attribute on an entity of the LDM, add an attribute
        to the relevant class in the package
        '''

        file_location = context.file_directory + os.sep + 'ldm' + os.sep + "DM_Attributes.csv"
        header_skipped = False
        # For each attribute add an ELAttribute to the correct ELClass representing the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    attribute_name = row[0]
                    amended_attribute_name = Utils.make_valid_id(
                         attribute_name)
                    attribute_kind = row[7]

                    preferred_abbreviation = row[31]
                    class_id = row[4]

                    relation_id = row[32]
                    primary_key_or_not = row[35]
                    foreign_key_or_not = row[36]

                    if len(preferred_abbreviation) == 0:
                        preferred_abbreviation = attribute_name
                        print("no preferred abbreviation for attribute: " + attribute_name)
                        
                    the_attribute_name = Utils.make_valid_id(preferred_abbreviation)
                    
                        
                    the_class = context.classes_map[class_id]

                    
                   
                    if attribute_kind == "Domain":
                        enum_id = row[12]
                        the_enum = context.enum_map[enum_id]

                        attribute = ELAttribute()
                          
                                                          
                        attribute.lowerBound = 0
                        attribute.upperBound = 1
                        if the_enum.name == "String":
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_string
                            attribute.eAttributeType = context.types.e_string
                        elif the_enum.name.startswith("String_"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_string
                            attribute.eAttributeType = context.types.e_string
                        elif the_enum.name.startswith("STRNG_"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_string
                            attribute.eAttributeType = context.types.e_string
                        elif the_enum.name == "Number":
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_double
                            attribute.eAttributeType = context.types.e_double
                        elif the_enum.name == "RL_domain":
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_double
                            attribute.eAttributeType = context.types.e_double
                        elif the_enum.name.startswith("RL"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_double
                            attribute.eAttributeType = context.types.e_double
                        elif the_enum.name.startswith("Real_"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_double
                            attribute.eAttributeType = context.types.e_double
                        elif the_enum.name.startswith("Monetary"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_int
                            attribute.eAttributeType = context.types.e_int
                        elif the_enum.name.startswith("MNTRY_"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_int
                            attribute.eAttributeType = context.types.e_int
                        elif the_enum.name.startswith("Non_negative_monetary_amounts_with_2_decimals"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_int
                            attribute.eAttributeType = context.types.e_int
                        elif the_enum.name.startswith("INTGR"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_int
                            attribute.eAttributeType = context.types.e_int  
                        elif the_enum.name.startswith("YR"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_int
                            attribute.eAttributeType = context.types.e_int
                        elif the_enum.name.startswith("Non_negative_integers"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_int
                            attribute.eAttributeType = context.types.e_int
                        elif the_enum.name.startswith("All_possible_dates"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_date
                            attribute.eAttributeType = context.types.e_date
                        elif the_enum.name.startswith("DT_FLL"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_date
                            attribute.eAttributeType = context.types.e_date
                        elif the_enum.name.startswith("BLN"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.types.e_date
                            attribute.eAttributeType = context.types.e_boolean

                            

                        # This is a common domain used for String identifiers in BIRD
                        # in SQLDeveloper

                        else:
                            attribute.name = the_attribute_name
                            attribute.eType = the_enum
                            attribute.eAttributeType = the_enum

                    context.enums_used.append(attribute.eType)

                    if (attribute_kind == "Logical Type"):
                        datatype_id = row[14]
                        try:

                            attribute = ELAttribute()
                            attribute.lowerBound = 0
                            attribute.upperBound = 1
                            attribute.name = the_attribute_name
                            attribute.eType = Utils.get_ecore_datatype_for_datatype(
                                self)
                            attribute.eAttributeType = Utils.get_ecore_datatype_for_datatype(
                                self)

                        except KeyError:
                            print("missing datatype: ")
                            print(datatype_id)

                    try:
                        
                        
                        the_class = context.classes_map[class_id]
                        the_class.eStructuralFeatures.extend([attribute])
                        the_long_name_annotation = ELAnnotation()
                        the_long_name_directive = Utils.get_annotation_directive(the_class.eContainer(), "long_name")
                        the_long_name_annotation.source = the_long_name_directive
                        details = the_long_name_annotation.details
                        mapentry  = ELStringToStringMapEntry()
                        mapentry.key = "long_name"
                        mapentry.value = amended_attribute_name
                        details.append(mapentry)
                        attribute.eAnnotations.append(the_long_name_annotation)

                    except:
                        print("missing class2: ")
                        print(class_id)

    

    def add_ldm_relationships_between_classes(self, context):
        '''
        For each relationship in the LDM, add a reference between the relevant classes
        '''
        file_location = context.file_directory + os.sep + 'ldm' + os.sep + "DM_Relations.csv"
        header_skipped = False
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    source_id = row[16]
                    target_id = row[18]
                    source_to_target_cardinality = row[10]
                    target_class_name = row[7]
                    # not that source optional actually means that the target is optional
                    # which is confusing, we can see that in the SQL developer diagrams
                    # where we have the circle (meaning optional) on the target side
                    # of the relationship when source_optional is true
                    source_optional = row[12]
                    target_optional = row[13]
                    relation_name = row[0]
                    identifying = row[15]

                    if target_optional == "N":
                        reference_name = Utils.make_valid_id(relation_name)
                    else:
                        reference_name = Utils.make_valid_id(relation_name)

                    try:
                        the_class = context.classes_map[source_id]
                    except KeyError:
                        print("missing class1: " + source_id)

                    try:
                        target_class = context.classes_map[target_id]
                    except KeyError:
                        print("missing target class: " + target_id)

                    ereference = ELReference()
                    ereference.name = reference_name
                    ereference.eType = target_class

                    if (not (the_class is None) ) and (not (target_class is None)):
                        ereference = ELReference()
                        ereference.name = reference_name
                        ereference.eType = target_class
    
                        if target_optional == "N":
                            ereference.containment = False

                            linked_reference = ELReference()
                            linked_reference.name = reference_name 
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

    
    def mark_root_class_as_entity_group_annotation(self,context):
        for the_class in context.classes_map.values():
            ultimate_superclass = SQLDevLDMImport.get_ultimate_superclass(self,context,the_class)
            if not (ultimate_superclass == the_class) :
                the_entity_annotation = Utils.get_annotation_with_source(the_class, "entity_hierarchy")
                if the_entity_annotation is None: 
                    the_entity_annotation = ELAnnotation()
                    the_entity_annotation_directive = Utils.get_annotation_directive(the_class.eContainer(), "entity_hierarchy")
                    the_entity_annotation.source = the_entity_annotation_directive
                    the_class.eAnnotations.append(the_entity_annotation)
                
                details = the_entity_annotation.details
                mapentry  = ELStringToStringMapEntry()
                mapentry.key = "entity_hierarchy"
                mapentry.value = ultimate_superclass.name
                details.append(mapentry)

            if (ultimate_superclass == the_class) and ( SQLDevLDMImport.has_subclasses(self,context,the_class) or SQLDevLDMImport.has_delegate(self,context,the_class)):
                the_entity_annotation = Utils.get_annotation_with_source(the_class, "entity_hierarchy")
                if the_entity_annotation is None: 
                    the_entity_annotation = ELAnnotation() 
                    the_entity_annotation_directive = Utils.get_annotation_directive(the_class.eContainer(), "entity_hierarchy")
                    the_entity_annotation.source = the_entity_annotation_directive
                    the_class.eAnnotations.append(the_entity_annotation)
                
                details = the_entity_annotation.details
                mapentry  = ELStringToStringMapEntry()
                mapentry.key = "entity_hierarchy"
                mapentry.value = ultimate_superclass.name
                details.append(mapentry)
         
    def remove_enums_not_used_by_attributes(self,context):
        
        for enum in context.ldm_domains_package.eClassifiers:
            if not (enum in context.enums_used):
                context.ldm_domains_package.eClassifiers.remove(enum)
        
    def get_ultimate_superclass(self,context,the_class):
        
        return_class = None
        if len(the_class.eSuperTypes) > 0:
            return_class = SQLDevLDMImport.get_ultimate_superclass(self,context,the_class.eSuperTypes[0])
        elif SQLDevLDMImport.is_delegate_class(self,context,the_class): 
            return_class = SQLDevLDMImport.get_ultimate_superclass(self,context,
                                SQLDevLDMImport.get_delegate_class(self,context,the_class))
        else:
            return_class = the_class
        
        return return_class
            
    def has_subclasses(self,context,the_class):
        for a_class in context.classes_map.values():
            if len(a_class.eSuperTypes) > 0:
                superclass = a_class.eSuperTypes[0]
                if superclass == the_class:
                    return True

        return False 
    
    def has_delegate(self,context,the_class):
        for a_class in context.classes_map.values():
            for ref in a_class.eStructuralFeatures:
                if ref.name.endswith('_delegate'):
                    return True

        return False 
        
    def is_delegate_class(self,context,the_class):

        if not (SQLDevLDMImport.get_delegate_class(self,context,the_class) is None):
                return True 
        return False
        
    def get_delegate_class(self,context,the_class):
        # find the calss that has a containment reference to this class
        for a_class in context.classes_map.values():
            for reference in a_class.eStructuralFeatures:
                if (reference.name.endswith('_delegate')) and reference.eType == the_class:
                    return a_class
                
        return None
    
    def remove_duplicate_attributes_in_subclasses(self, context):
        
        for classifier in context.ldm_entities_package.eClassifiers:
            if isinstance(classifier, ELClass):
                feaures_to_remove = []
                for feature in  classifier.eStructuralFeatures:
                    if isinstance(feature, ELAttribute):
                        if SQLDevLDMImport.attribute_exists_in_any_superclass(self, classifier,feature, context):
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
                return SQLDevLDMImport.attribute_exists_in_any_superclass(self, super_class,attribute, context)
            else:
                return True
                         
        else:
            return False               
    

        
    
        