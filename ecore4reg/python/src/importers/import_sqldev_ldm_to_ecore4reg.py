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
import csv
from utils.utils import Utils

from ecore4reg import ELAttribute, ELClass, ELEnum
from ecore4reg import ELEnumLiteral, ELPublicOperation, ELReference
from ecore4reg import ELAnnotation, ELStringToStringMapEntry
from pickle import TRUE

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
        #SQLDevLDMImport.remove_ldm_attributes_already_in_superclass(self, context)
        SQLDevLDMImport.add_ldm_relationships_between_classes(self, context)
        SQLDevLDMImport.mark_root_class_as_entity_group_annotation(self, context)
        #SQLDevLDMImport.copy_primary_keys_to_subclasses(self, context)

    def add_ldm_classes_to_package(self, context):
        '''
        for each entity in the LDM, create a class and add it to the package
        '''

        file_location = context.file_directory + os.sep + "DM_Entities.csv"

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

                    class_name = row[0]
                    object_id = row[1]
                    engineering_type = row[27]
                    num_supertype_entity_id = row[26]

                    altered_class_name = Utils.make_valid_id(class_name)
                    eclass = ELClass(name=altered_class_name)
                    eclass_attributes = ELClass(name=altered_class_name+"_attributes") 
                    eclass_attributes_table = ELClass(name=altered_class_name+"_attributes_table")  
                    eclass_objects_table = ELClass(name=altered_class_name+"_objects_table")                
                    
                    attribute_reference = ELReference(name = "attributes")
                    attribute_reference.upperBound = 1
                    attribute_reference.lowerBound = 1
                    attribute_reference.eType = eclass_attributes
                    attribute_reference.containment = False
                    eclass.eStructuralFeatures.append(attribute_reference)
                    
                    attribute_reference_list = ELReference(name = "attribute_list")
                    attribute_reference_list.upperBound = -1
                    attribute_reference_list.lowerBound = 0
                    attribute_reference_list.eType = eclass_attributes
                    attribute_reference_list.containment = True
                    eclass_attributes_table.eStructuralFeatures.append(attribute_reference_list)
                    
                    object_reference_list = ELReference(name = "object_list")
                    object_reference_list.upperBound = -1
                    object_reference_list.lowerBound = 0
                    object_reference_list.eType = eclass
                    object_reference_list.containment = False
                    eclass_objects_table.eStructuralFeatures.append(object_reference_list)
                    
                    object_table_to_attribute_table_link = ELReference(name = "object_table_to_attribute_table_link")
                    object_table_to_attribute_table_link.upperBound = -1
                    object_table_to_attribute_table_link.lowerBound = 0
                    object_table_to_attribute_table_link.eType = eclass_attributes_table
                    object_table_to_attribute_table_link.containment = False
                    eclass_objects_table.eStructuralFeatures.append(object_table_to_attribute_table_link)
                    
                    context.input_tables_package.eClassifiers.extend([
                                                                          eclass])
                    context.input_tables_package.eClassifiers.extend([
                                                                          eclass_attributes])
                    context.input_tables_package.eClassifiers.extend([
                                                                          eclass_attributes_table])
                    context.input_tables_package.eClassifiers.extend([
                                                                          eclass_objects_table])

                    # maintain a map a objectIDs to ELClasses
                    context.classes_map[object_id] = eclass
                    context.attribute_classes_map[object_id] = eclass_attributes
                    context.object_classes_table_map[object_id] = eclass_objects_table
                    
                    init_operation = ELPublicOperation()
                    init_operation.name = 'init'
                    init_operation.eType = context.e_string
                    eclass_objects_table.eOperations.append(init_operation)
                   

    def import_disjoint_subtyping_information(self, context):
        '''
        for each disjoint substype arc, create a class.
        for each arc store its source in a dictionary
        for each arc target store a link from target to the arcs class
        later we will set supertypes of the targets to be the arcs class
        later we will set the arc to be a contained class of the source
        '''
        file_location = context.file_directory + os.sep + "arcs.csv"
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
                        arc_class = context.arc_name_to_arc_class_map[altered_arc_name]
                    except KeyError:
                        # if the arc /source entry has not yet been added to the dictionary
                        # then we add it here, and we add the arc name
                        # and we create class for the arc
                        
                        arc_class = ELClass(name=altered_arc_name)
                        arc_class.eAbstract = True
                        source_class = SQLDevLDMImport.find_class_with_name(self, context, Utils.make_valid_id(entity_name))
                        context.arc_name_to_arc_class_map[altered_arc_name] = arc_class
                        context.arc_to_source_map[altered_arc_name] = source_class
                        context.input_tables_package.eClassifiers.extend([arc_class])
                        containment_reference = ELReference()
                        containment_reference.name = altered_arc_name + "_delegate"
                        containment_reference.eType = arc_class
                        containment_reference.upperBound = 1
                        containment_reference.lowerBound = 0
                        containment_reference.containment = True
                        source_class.eStructuralFeatures.append(
                            containment_reference)
                        
                    
                    target_class = SQLDevLDMImport.find_class_with_name(self, context,Utils.make_valid_id(target_entity_name))
                    context.arc_target_to_arc_map[Utils.make_valid_id(target_entity_name)] = target_class
                    target_class.eSuperTypes.extend([arc_class])
         
    def find_class_with_name(self, context, name):
        '''
        get the class with this name from the input tables package
        '''
        for eclassifier in context.input_tables_package.eClassifiers:
            if isinstance(eclassifier, ELClass):
                if eclassifier.name == name:
                    return eclassifier
                
    def set_ldm_super_classes(self, context):
        '''
        for each entity in the LDM, set the superclass of the class,
        but not if it already has a super class set by the disjoint subtyping
        processing
        '''
        file_location = context.file_directory + os.sep + "DM_Entities.csv"
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
        file_location = context.file_directory + os.sep + "DM_Domains.csv"
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
                    adapted_enum_name = Utils.make_valid_id(enum_name)+"_domain"
                    if not Utils.in_enum_excluded_list( adapted_enum_name):
                        the_enum = ELEnum()
                        the_enum.name = adapted_enum_name
                        # maintain a map of enum IDS to ELEnum objects
                        context.enum_map[enum_id] = the_enum
                        context.il_domains_package.eClassifiers.extend([
                                                                           the_enum])

    def add_ldm_literals_to_enums(self, context):
        '''
        for each memebr of a domain the LDM, add a literal to the corresponding enum
        '''
        file_location = context.file_directory + os.sep + "DM_Domain_AVT.csv"
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
                        enum_used_name = Utils.make_valid_id( row[3])
                        # enumName = row[5]
                        adapted_enum_name = Utils.make_valid_id( enum_used_name)
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

        file_location = context.file_directory + os.sep + "DM_Logical_To_Native.csv"
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

                            context.datatype_map[datatype_id] = context.e_string

                        if native_type.strip() == "VARCHAR2":

                            context.datatype_map[datatype_id] = context.e_string

                        if native_type.strip() == "INTEGER":

                            context.datatype_map[datatype_id] = context.e_int

                        if native_type.strip() == "DATE":

                            context.datatype_map[datatype_id] = context.e_date

                        if native_type.strip() == "NUMBER":

                            context.datatype_map[datatype_id] = context.e_double

                        if native_type.strip() == "UNKNOWN":

                            context.datatype_map[datatype_id] = context.e_string

    def add_ldm_attributes_to_classes(self, context):
        '''
        For each attribute on an entity of the LDM, add an attribute
        to the relevant class in the package
        '''

        file_location = context.file_directory + os.sep + "DM_Attributes.csv"
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

                    class_id = row[4]
                    relation_id = row[32]
                    primary_key_or_not = row[35]
                    foreign_key_or_not = row[36]
                    the_class = context.attribute_classes_map[class_id]

                    the_attribute_name = amended_attribute_name
                   
                    if attribute_kind == "Domain":
                        enum_id = row[12]
                        the_enum = context.enum_map[enum_id]

                        attribute = ELAttribute()
                        if primary_key_or_not == "P":
                            attribute.iD = True
                            annotation = ELAnnotation()
                            mapentry  = ELStringToStringMapEntry()
                            mapentry.key = "key_type"
                            mapentry.value = "Primary"
                            annotation.details.append(mapentry)
                            attribute.eAnnotations = annotation
                        
                        if foreign_key_or_not == "F":
                            annotation2 = attribute.eAnnotations
                            if annotation2 is None:
                                annotation2 = ELAnnotation()
                            mapentry2  = ELStringToStringMapEntry()
                            mapentry2.key = "key_type"
                            mapentry2.value = "Foreign"
                            annotation2.details.append(mapentry2)
                            attribute.eAnnotations = annotation2      
                                                          
                        attribute.lowerBound = 0
                        attribute.upperBound = 1
                        if the_enum.name == "String":
                            attribute.name = the_attribute_name
                            attribute.eType = context.e_string
                            attribute.eAttributeType = context.e_string
                        elif the_enum.name.startswith("String_"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.e_string
                            attribute.eAttributeType = context.e_string
                        elif the_enum.name == "Number":
                            attribute.name = the_attribute_name
                            attribute.eType = context.e_double
                            attribute.eAttributeType = context.e_double
                        elif the_enum.name == "RL_domain":
                            attribute.name = the_attribute_name
                            attribute.eType = context.e_double
                            attribute.eAttributeType = context.e_double
                        elif the_enum.name.startswith("Real_"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.e_double
                            attribute.eAttributeType = context.e_double
                        elif the_enum.name.startswith("Monetary"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.e_int
                            attribute.eAttributeType = context.e_int
                        elif the_enum.name.startswith("Non_negative_monetary_amounts_with_2_decimals"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.e_int
                            attribute.eAttributeType = context.e_int
                        elif the_enum.name.startswith("Non_negative_integers"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.e_int
                            attribute.eAttributeType = context.e_int
                        elif the_enum.name.startswith("All_possible_dates"):
                            attribute.name = the_attribute_name
                            attribute.eType = context.e_date
                            attribute.eAttributeType = context.e_date

                        # This is a common domain used for String identifiers in BIRD
                        # in SQLDeveloper

                        else:
                            attribute.name = the_attribute_name
                            attribute.eType = the_enum
                            attribute.eAttributeType = the_enum

                        

                    if (attribute_kind == "Logical Type"):
                        datatype_id = row[14]
                        try:

                            attribute = ELAttribute()
                            attribute.lowerBound = 0
                            attribute.upperBound = 1
                            attribute.name = amended_attribute_name
                            attribute.eType = Utils.get_ecore_datatype_for_datatype(
                                self)
                            attribute.eAttributeType = Utils.get_ecore_datatype_for_datatype(
                                self)

                        except KeyError:
                            print("missing datatype: ")
                            print(datatype_id)

                    try:

                        the_class = context.attribute_classes_map[class_id]
                        the_class.eStructuralFeatures.extend([attribute])

                    except:
                        print("missing class2: ")
                        print(class_id)

    def remove_ldm_attributes_already_in_superclass(self, context):
        '''
        if we already have created an attribute in both a subclass and a superclass
        then we delete it in the subclass
        '''
        for the_class in context.classes_map.values():
            if len(the_class.eSuperTypes) > 0:
                superclass = the_class.eSuperTypes[0]
                if superclass:

                    attributes = the_class.eStructuralFeatures
                    attributes_to_delete = []
                    for the_attribute in attributes:
                        if Utils.superclass_contains_feature( superclass, the_attribute):
                            attributes_to_delete.append(the_attribute)

                    for the_attribute in attributes_to_delete:
                        the_class.eStructuralFeatures.remove(the_attribute)


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
                    source_id = row[16]
                    target_id = row[18]
                    source_to_target_cardinality = row[10]
                    target_class_name = row[7]
                    target_optional = row[13]
                    relation_name = row[0]
                    identifying = row[15]

                    reference_name = Utils.make_valid_id(relation_name)

                    try:
                        the_class = context.classes_map[source_id]
                        object_table  = context.object_classes_table_map[source_id]
                    except KeyError:
                        print("missing class1: " + source_id)

                    try:
                        target_class = context.classes_map[target_id]
                        target_object_table  = context.object_classes_table_map[target_id]
                    except KeyError:
                        print("missing target class: " + target_id)

                    # num_of_relations = Utils.number_of_relationships_to_this_class(
                    #     the_class, target_class)
                    # if num_of_relations > 0:
                    #     reference_name = reference_name + str(num_of_relations)

                    ereference = ELReference()
                    object_table_ereference = ELReference()
                    
                    ereference.name = reference_name
                    object_table_ereference.name = reference_name + "_object_table"
                    
                    ereference.eType = target_class
                    object_table_ereference.eType = target_object_table
                    
                    if identifying == "Y":
                        ereference.containment = True
                    else:
                        ereference.containment = False
                        
                    object_table_ereference.containment = False
                    
                    if target_optional.strip() == "Y":
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

                    object_table_ereference.upperBound = 1    
                    object_table_ereference.lowerBound = 1
                        
                    if not the_class is None:
                        the_class.eStructuralFeatures.append(ereference)
                        object_table.eStructuralFeatures.append(object_table_ereference)
    
    def mark_root_class_as_entity_group_annotation(self,context):
        for the_class in context.classes_map.values():
            ultimate_superclass = SQLDevLDMImport.get_ultimate_superclass(self,context,the_class)
            if not (ultimate_superclass == the_class) :
                annotation = the_class.eAnnotations
                if annotation is None:
                    annotation = ELAnnotation()
                mapentry  = ELStringToStringMapEntry()
                mapentry.key = "entity_hierarchy"
                mapentry.value = ultimate_superclass.name
                annotation.details.append(mapentry)
                the_class.eAnnotations = annotation   
            if (ultimate_superclass == the_class) and ( SQLDevLDMImport.has_subclasses(self,context,the_class) or SQLDevLDMImport.has_delegate(self,context,the_class)):
                annotation = the_class.eAnnotations
                if annotation is None:
                    annotation = ELAnnotation()
                mapentry  = ELStringToStringMapEntry()
                mapentry.key = "entity_hierarchy"
                mapentry.value = ultimate_superclass.name
                annotation.details.append(mapentry)
                the_class.eAnnotations = annotation
         
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
    
    def create_init_function_text(self,object_table):
        ''' 
        The init function for an object table should look at the
        associated attribute list table, nd its associated attributes,
        and any referenced object tables, and their associated attributes.
        Looking at the annotations of the attributes we can find 
        what is a primary key, and a foreign key, and what links
        are annotated between tables. Then we can create an object
        for each attribute list in the attribute list table we create an object
        and with the annotated information of the attributes we try 
        to set the rferences (relationships) correctly on the object.
        We assume that attribute lists  have already been populated
        at the tim that the init function is run. It will be run 
        by an orchestrator.
        '''
        return ""
        
    
        