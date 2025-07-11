# coding=UTF-8#
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDE-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#
'''
Created on 22 Jan 2022

@author: Neil
'''

import os
import csv
from pybirdai.entry_points.utils_processor import Utils

from pybirdai.regdna import ELAttribute, ELClass, ELEnum
from pybirdai.regdna import ELEnumLiteral, ELReference



class SQLDeveloperILImport(object):
    '''
    Documentation for SQLDeveloperILImport
    '''

    def do_import(self, context):
        '''
        import the items from the BIRD LDM csv files
        '''
        SQLDeveloperILImport.add_il_classes_to_package(self, context)
        SQLDeveloperILImport.add_il_enums_to_package(self, context)
        SQLDeveloperILImport.add_il_literals_to_enums(self, context)
        SQLDeveloperILImport.create_il_types_map(self, context)
        SQLDeveloperILImport.add_il_pk_attributes_to_classes(self, context)
        SQLDeveloperILImport.add_il_attributes_to_classes(self, context)
        SQLDeveloperILImport.create_fk_to_column_map(self, context)
        SQLDeveloperILImport.add_il_relationships_between_classes(self, context)

    def add_il_classes_to_package(self, context):
        '''
        for each entity in the IL, create a class and add it to the package
        '''

        file_location = context.file_directory + os.sep + "il" + os.sep + "DM_Tables.csv"

        header_skipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not header_skipped):
                    header_skipped = True
                else:

                    class_name = row[0]
                    object_id = row[1]

                    # assume that SQLDve gives valid IDS fro table names according
                    # to the validity rules of ECoreL
                    altered_class_name = Utils.make_valid_id(class_name)

                    eclass = ELClass(name=altered_class_name)
                    # of engineering type is single table, as i should be
                    # for all members of a type
                    # heirarchy, and num_suptype is blanck, then
                    # this means that this class is a root
                    # of a type heirarchy....we will set such classes
                    # to be abstract.

                    containment_reference = ELReference()
                    containment_reference.name = eclass.name+"s"
                    containment_reference.eType = eclass
                    containment_reference.upperBound = -1
                    containment_reference.lowerBound = 0
                    containment_reference.containment = True



                    context.il_tables_package.eClassifiers.extend([
                                                                        eclass])


                    # maintain a map a objectIDs to ELClasses, we add to
                    # the map even in input layters come from the websiute,
                    # this is because we want to find the table rrealtionship
                    # info that is not currently on the website, and use that to
                    # enrich the website content
                    context.classes_map[object_id] = eclass


    def add_il_enums_to_package(self, context):
        '''
        for each domain in the IL add an enum to the package
        '''
        file_location = context.file_directory + os.sep + "il" + os.sep + "DM_Domains.csv"
        header_skipped = False
        counter = 0
        # Create an ELEnum for each domain, and add it to the ELPackage
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not header_skipped):
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
                    context.il_domains_package.eClassifiers.extend([
                                                                        the_enum])

    def add_il_literals_to_enums(self, context):
        '''
        for each memebr of a domain the IL, add a literal to the corresponding enum
        '''
        file_location = context.file_directory + os.sep + "il" + os.sep + "DM_Domain_AVT.csv"
        header_skipped = False
        counter = 0
        # Add the members of a domain as literals of the related Enum
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not header_skipped):
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
                            #new_adapted_value = Utils.special_cases(
                            #    new_adapted_value, counter)
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

    def create_il_types_map(self, context):
        '''
        for each type in the IL, create a map of typeID to type name
        '''
        # for each logicalDatatype for orcle 12c, make a Datatype if we have an
        # equivalent

        file_location = context.file_directory + os.sep + "il" + os.sep + "DM_Logical_To_Native.csv"
        header_skipped = False
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not header_skipped):
                    header_skipped = True
                else:
                    rdbms_type = row[3]
                    rdbms_version = row[4]
                    data_type_id = row[0]
                    if ((rdbms_type.strip() == "Oracle Database")
                        and (rdbms_version.strip() == "12cR2")):
                        native_type = row[2]

                        if (native_type.strip() == "VARCHAR"):

                            context.datatype_map[data_type_id] = context.types.e_string

                        if (native_type.strip() == "VARCHAR2"):

                            context.datatype_map[data_type_id] = context.types.e_string

                        if (native_type.strip() == "INTEGER"):

                            context.datatype_map[data_type_id] = context.types.e_int

                        if (native_type.strip() == "DATE"):

                            context.datatype_map[data_type_id] = context.types.e_date

                        if (native_type.strip() == "NUMBER"):

                            context.datatype_map[data_type_id] = context.types.e_double

                        if (native_type.strip() == "UNKNOWN"):

                            context.datatype_map[data_type_id] = context.types.e_string

    def add_il_pk_attributes_to_classes(self, context):
        file_location = context.file_directory + os.sep + "il" + os.sep + "DM_Columns.csv"
        header_skipped = False
        # For each attribute add an ELAttribute to the correct ELClass representing the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not header_skipped):
                    header_skipped = True
                else:
                    attribute_name = row[0]
                    attribute_id = row[1]
                    amended_attribute_name = Utils.make_valid_id(attribute_name)
                    mandatory = row[6]
                    attribute_kind = row[7]

                    class_id = row[4]
                    relationID = row[35]
                    primary_key_or_not = row[34]
                    the_class = context.classes_map[class_id]

                    class_is_derived = False
                    if (the_class.name.endswith("_derived")):
                        class_is_derived = True

                    the_attribute_name = amended_attribute_name

                    # we only add attributes here if they are not representing a relationship
                    if primary_key_or_not == "P":

                        if (attribute_kind == "Domain"):
                            enum_id = row[13]
                            the_enum = context.enum_map[enum_id]

                            attribute = ELAttribute()

                            attribute.lowerBound = 0
                            attribute.upperBound = 1

                            if (the_enum.name == "String"):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_string
                                attribute.eAttributeType = context.types.e_string
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name.startswith("String_")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_string
                                attribute.eAttributeType = context.types.e_string
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name.startswith("STRNG_")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_string
                                attribute.eAttributeType = context.types.e_string
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name == "Number"):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_double
                                attribute.eAttributeType = context.types.e_double
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name == "RL_domain"):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_double
                                attribute.eAttributeType = context.types.e_double
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif the_enum.name.startswith("RL"):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_double
                                attribute.eAttributeType = context.types.e_double
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name.startswith("Real_")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_double
                                attribute.eAttributeType = context.types.e_double
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name.startswith("Monetary")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_int
                                attribute.eAttributeType = context.types.e_int
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name.startswith("MNTRY_")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_int
                                attribute.eAttributeType = context.types.e_int
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name.startswith("INTGR_")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_int
                                attribute.eAttributeType = context.types.e_int
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name.startswith("YR")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_int
                                attribute.eAttributeType = context.types.e_int
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_int
                                attribute.eAttributeType = context.types.e_int
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name.startswith("Non_negative_integers")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_int
                                attribute.eAttributeType = context.types.e_int
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name.startswith("Positive_and_negative_monetary_amounts_with_2_decimals_domain")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_int
                                attribute.eAttributeType = context.types.e_int
                                attribute.upperBound = 1
                                attribute.lowerBound = 1
                            elif (the_enum.name.startswith("Positive_and_negative_monetary_amounts_with_2_decimals")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_int
                                attribute.eAttributeType = context.types.e_int
                                attribute.upperBound = 1
                                attribute.lowerBound = 1

                            elif (the_enum.name.startswith("All_possible_dates")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_date
                                attribute.eAttributeType = context.types.e_date
                                attribute.upperBound = 1
                                attribute.lowerBound = 1

                            elif (the_enum.name.startswith("DT_FLL")):
                                attribute.name = the_attribute_name
                                attribute.eType = context.types.e_date
                                attribute.eAttributeType = context.types.e_date
                                attribute.upperBound = 1
                                attribute.lowerBound = 1

                            # This is a common domain used for String identifiers in BIRD in SQLDeveloper

                            else:
                                attribute.name = the_attribute_name
                                attribute.eType = the_enum
                                attribute.eAttributeType = the_enum
                                attribute.upperBound = 1
                                attribute.lowerBound = 1



                        if (attribute_kind == "Logical Type"):
                            data_type_id = row[14]
                            try:
                                datatype = context.datatype_map[data_type_id]
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
                                print(data_type_id)

                        try:

                            the_class = context.classes_map[class_id]
                            SQLDeveloperILImport.add_composite_pk_if_missing(
                                self, context, the_class)
                            the_class.eStructuralFeatures.extend([attribute])


                        except:
                            print("missing class2: ")
                            print(class_id)
                    else:
                        if mandatory == "Y":
                            context.fk_to_mandatory_map[attribute_id] = "M"

    def add_composite_pk_if_missing(self, context, the_class):
        '''
        If the class does not have a composite PK, add one
        '''

        pk_name = the_class.name + "_uniqueID"
        pk_exists = False
        for member in the_class.eStructuralFeatures:
            if member.name == pk_name:
                pk_exists = True

        if not pk_exists:
            attribute = ELAttribute()
            attribute.name = pk_name
            attribute.eType = context.types.e_string
            attribute.eAttributeType = context.types.e_string
            attribute.iD = True
            attribute.lowerBound = 0
            attribute.upperBound = 1
            the_class.eStructuralFeatures.append(attribute)

    def add_il_attributes_to_classes(self, context):
        '''
        For each attribute on an entity of the IL, add an attribute
        to the relevant class in the package
        '''

        file_location = context.file_directory + os.sep +  "il" + os.sep + "DM_Columns.csv"
        header_skipped = False
        # For each attribute add an ELAttribute to the correct ELClass representing the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not header_skipped):
                    header_skipped = True
                else:
                    attribute_name = row[0]
                    attribute_id = row[1]
                    amended_attribute_name = Utils.make_valid_id(attribute_name)
                    mandatory = row[6]
                    attribute_kind = row[7]

                    class_id = row[4]
                    preferred_abbreviation = row[0]
                    relation_id = row[35]
                    primary_key_or_not = row[34]
                    the_class = context.classes_map[class_id]



                    if len(preferred_abbreviation) == 0:
                        preferred_abbreviation = attribute_name
                        print("no preferred abbreviation for attribute: " + attribute_name)

                    the_attribute_name = Utils.make_valid_id(preferred_abbreviation)

                    # we only add attributes here if they are not representing a relationship
                    if (relation_id == "") and not (primary_key_or_not == "P"):

                        if (attribute_kind == "Domain"):
                            enum_id = row[13]
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

                            # This is a common domain used for String identifiers in BIRD in SQLDeveloper

                        else:
                            attribute.name = the_attribute_name
                            attribute.eType = the_enum
                            attribute.eAttributeType = the_enum


                        if attribute_kind == "Logical Type":
                            data_type_id = row[14]
                            try:
                                datatype = context.datatype_map[data_type_id]
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
                                print(data_type_id)

                        try:

                            the_class = context.classes_map[class_id]
                            the_class.eStructuralFeatures.extend([attribute])

                        except:
                            print("missing class2: ")
                            print(class_id)
                    else:
                        if mandatory == "Y":
                            context.fk_to_mandatory_map[attribute_id] = "M"

    def create_fk_to_column_map(self, context):
        '''
        Create a map of FK to column ID
        '''
        file_location = context.file_directory + os.sep +  "il" + os.sep + "DM_Constr_Index_Columns.csv"
        header_skipped = False
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not header_skipped):
                    header_skipped = True
                else:
                    fk_name = row[6]
                    columnID = row[2]
                    context.fk_to_column_map[fk_name] = columnID

    def add_il_relationships_between_classes(self, context):
        '''
        For each relationship in the IL, add a reference between the relevant classes
        '''
        file_location = context.file_directory + os.sep +  "il" + os.sep + "DM_ForeignKeys.csv"
        header_skipped = False
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not header_skipped):
                    header_skipped = True
                else:
                    fk_id = row[0]
                    source_id = row[10]
                    target_id = row[12]

                    target_class_name = row[7]

                    fk_columnn_id = context.fk_to_column_map[fk_id]

                    if fk_columnn_id in context.fk_to_mandatory_map:
                        target_optional = "N"
                    else:
                        target_optional = "Y"

                    source_to_target_cardinality = "1"

                    reference_name = "the" + Utils.make_valid_id(target_class_name)

                    try:
                        the_class = context.classes_map[source_id]
                    except KeyError:
                        print("missing class1: " + source_id)

                    try:
                        target_class = context.classes_map[target_id]
                    except KeyError:
                        print("missing target class: " + target_id)

                    num_of_relations = Utils.number_of_relationships_to_this_class(
                        the_class, target_class)
                    if (num_of_relations > 0):
                        reference_name = reference_name + str(num_of_relations)
                    relational_attribute = None
                    if (target_optional.strip() == "Y"):
                        if (source_to_target_cardinality.strip() == "*"):
                            reference_name = reference_name + "s"
                            e_reference = ELReference()
                            e_reference.name = reference_name
                            e_reference.eType = target_class
                            # upper bound of -1 means there is no upper bounds, so represents an open list of reference
                            e_reference.upperBound = -1
                            e_reference.lowerBound = 0
                            e_reference.containment = False

                        else:
                            e_reference = ELReference()
                            e_reference.name = reference_name
                            e_reference.eType = target_class
                            e_reference.upperBound = 1
                            e_reference.lowerBound = 0
                            e_reference.containment = False

                    else:
                        if (source_to_target_cardinality.strip() == "*"):
                            reference_name = reference_name + "s"
                            e_reference = ELReference()
                            e_reference.name = reference_name
                            e_reference.eType = target_class
                            e_reference.upperBound = -1
                            e_reference.lowerBound = 1
                            e_reference.containment = False

                        else:
                            e_reference = ELReference()
                            e_reference.name = reference_name
                            e_reference.eType = target_class
                            e_reference.upperBound = 1
                            e_reference.lowerBound = 1
                            e_reference.containment = False

                    if not (the_class is None):
                        the_class.eStructuralFeatures.append(e_reference)
                        # reference_tuple = (the_class.name,e_reference.name,e_reference.eType.name,e_reference.upperBound,e_reference.lowerBound)
                        # context.foreign_key_tuple.append(reference_tuple)
