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
from importers.utils import Utils

from ecore4reg import ELAttribute, ELClass, ELEnum
from ecore4reg import ELEnumLiteral, ELOperation, ELReference

class SQLDevLDMImport(object):
    '''
    Documentation for SQLDevLDMImport
    '''

    def do_import(self, context):
        '''
        import the items from the BIRD LDM csv files
        '''
        SQLDevLDMImport.add_ldm_classes_to_package(self, context)
        SQLDevLDMImport.set_ldm_super_classes(self, context)
        SQLDevLDMImport.add_ldm_enums_to_package(self, context)
        SQLDevLDMImport.add_ldm_literals_to_enums(self, context)
        SQLDevLDMImport.create_ldm_types_map(self, context)
        SQLDevLDMImport.add_ldm_attributes_to_classes(self, context)
        SQLDevLDMImport.remove_ldm_attributes_already_in_superclass(self, context)
        SQLDevLDMImport.add_ldm_relationships_between_classes(self, context)

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
                    if altered_class_name.endswith("_derived"):
                        eclass = ELClass(name=altered_class_name)
                        eclass_table = ELClass(
                            name=altered_class_name+"_DerivedTable")
                        eclass_table.containedEntityType = eclass
                        containment_reference = ELReference()
                        containment_reference.name = eclass.name+"s"
                        containment_reference.eType = eclass
                        containment_reference.upperBound = -1
                        containment_reference.lowerBound = 0
                        containment_reference.containment = True
                        eclass_table.eStructuralFeatures.append(
                            containment_reference)
                        eclass_table_operation = ELOperation()
                        eclass_table_operation.name = eclass.name+"s"
                        eclass_table_operation.eType = eclass
                        eclass_table_operation.upperBound = -1
                        eclass_table_operation.lowerBound = 0
                        eclass_table.eOperations.append(eclass_table_operation)
                        context.input_tables_package.eClassifiers.extend([
                                                                              eclass])
                        context.input_tables_package.eClassifiers.extend([
                                                                              eclass_table])
                    elif (class_name.startswith("OUTPUT_LAYER_")):
                        eclass = ELClass(name=altered_class_name)

                        context.input_tables_package.eClassifiers.extend([
                                                                              eclass])

                    else:
                        eclass = ELClass(name=altered_class_name)
                        # of engineering type is single table, as it should be
                        # for all members of a type
                        # heirarchy, and num_suptype is blank,
                        # then this means that this class is a root
                        # of a type heirarchy....we will set such classes
                        #  to be abstract.
                        if (engineering_type == "Single Table") and (num_supertype_entity_id == ""):
                            eclass.abstract = True
                        eclass_table = ELClass(
                            name=altered_class_name+"_Table")
                        containment_reference = ELReference()
                        containment_reference.name = eclass.name+"s"
                        containment_reference.eType = eclass
                        containment_reference.upperBound = -1
                        containment_reference.lowerBound = 0
                        containment_reference.containment = True
                        eclass_table.eStructuralFeatures.append(
                            containment_reference)
                        context.input_tables_package.eClassifiers.extend([
                                                                              eclass])
                        context.input_tables_package.eClassifiers.extend([
                                                                              eclass_table])

                    # maintain a map a objectIDs to ELClasses
                    context.classes_map[object_id] = eclass
                    context.table_map[eclass] = eclass_table

    def set_ldm_super_classes(self, context):
        '''
        for each entity in the LDM, set the superclass of the class
        '''
        file_location = context.file_directory + os.sep + "DM_Entities.csv"
        header_skipped = False

        # Where an nxtity has a superclass, set the superclass on the ELClass
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
                    the_class = context.classes_map[class_id]

                    class_is_derived = False
                    if the_class.name.endswith("_derived"):
                        class_is_derived = True

                    the_attribute_name = amended_attribute_name

                    # we only add attributes here if they are not representing a relationship
                    if relation_id == "":

                        if attribute_kind == "Domain":
                            enum_id = row[12]
                            the_enum = context.enum_map[enum_id]

                            attribute = ELAttribute()
                            if primary_key_or_not == "P":
                                attribute.iD = True

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

                            if class_is_derived:
                                operation = ELOperation()
                                operation.lowerBound = 0
                                operation.upperBound = 1
                                if the_enum.name == "String":
                                    operation.name = the_attribute_name
                                    operation.eType = context.e_string
                                elif the_enum.name.startswith("String_"):
                                    operation.name = the_attribute_name
                                    operation.eType = context.e_string
                                elif the_enum.name == "Number":
                                    operation.name = the_attribute_name
                                    operation.eType = context.e_double

                                elif the_enum.name.startswith("Real_"):
                                    operation.name = the_attribute_name
                                    operation.eType = context.e_double
                                elif the_enum.name.startswith("Monetary"):
                                    operation.name = the_attribute_name
                                    operation.eType = context.e_int
                                elif the_enum.name.startswith("Non_negative_monetary_amounts_with_2_decimals"):
                                    operation.name = the_attribute_name
                                    operation.eType = context.e_int
                                elif the_enum.name.startswith("Non_negative_integers"):
                                    operation.name = the_attribute_name
                                    operation.eType = context.e_int
                                elif the_enum.name.startswith("All_possible_dates"):
                                    operation.name = the_attribute_name
                                    operation.eType = context.e_date
                                else:
                                    operation.name = the_attribute_name
                                    operation.eType = the_enum

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

                                if class_is_derived:
                                    operation = ELOperation()
                                    operation.lowerBound = 0
                                    operation.upperBound = 1
                                    operation.name = amended_attribute_name
                                    operation.eType = Utils.get_ecore_datatype_for_datatype(
                                        self)

                            except KeyError:
                                print("missing datatype: ")
                                print(datatype_id)

                        try:

                            the_class = context.classes_map[class_id]
                            the_class.eStructuralFeatures.extend([attribute])
                            if class_is_derived:
                                the_class.eOperations.extend([operation])

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

                    reference_name = "the" + \
                        Utils.make_valid_id(target_class_name)

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
                    if num_of_relations > 0:
                        reference_name = reference_name + str(num_of_relations)

                    if target_optional.strip() == "Y":
                        if source_to_target_cardinality.strip() == "*":
                            reference_name = reference_name + "s"
                            ereference = ELReference()
                            ereference.name = reference_name
                            ereference.eType = target_class
                            # upper bound of -1 means there is no upper bounds, 
                            # so represents an open list of reference
                            ereference.upperBound = -1
                            ereference.lowerBound = 0
                            ereference.containment = False
                            if the_class.name.endswith("_derived"):
                                the_source_table = context.table_map[the_class]
                                the_target_table = context.table_map[target_class]
                                if not Utils.has_member_called( the_source_table, "sourceTable1"):

                                    source_tables_reference = ELReference()
                                    source_tables_reference.name = "sourceTable1"
                                    source_tables_reference.eType = the_target_table
                                    source_tables_reference.upperBound = -1
                                    source_tables_reference.lowerBound = 0
                                    source_tables_reference.containment = False
                                    the_source_table.eStructuralFeatures.append(
                                        source_tables_reference)
                                else:

                                    source_tables_reference = ELReference()
                                    source_tables_reference.name = "sourceTable2"
                                    source_tables_reference.eType = the_target_table
                                    source_tables_reference.upperBound = -1
                                    source_tables_reference.lowerBound = 0
                                    source_tables_reference.containment = False
                                    the_source_table.eStructuralFeatures.append(
                                        source_tables_reference)
                        else:
                            ereference = ELReference()
                            ereference.name = reference_name
                            ereference.eType = target_class
                            ereference.upperBound = 1
                            ereference.lowerBound = 0
                            ereference.containment = False
                            if the_class.name.endswith("_derived"):
                                the_source_table = context.table_map[the_class]
                                the_target_table = context.table_map[target_class]
                                if not Utils.has_member_called(the_source_table, "sourceTable1"):

                                    source_tables_reference = ELReference()
                                    source_tables_reference.name = "sourceTable1"
                                    source_tables_reference.eType = the_target_table
                                    source_tables_reference.upperBound = -1
                                    source_tables_reference.lowerBound = 0
                                    source_tables_reference.containment = False
                                    the_source_table.eStructuralFeatures.append(
                                        source_tables_reference)
                                else:

                                    source_tables_reference = ELReference()
                                    source_tables_reference.name = "sourceTable2"
                                    source_tables_reference.eType = the_target_table
                                    source_tables_reference.upperBound = -1
                                    source_tables_reference.lowerBound = 0
                                    source_tables_reference.containment = False
                                    the_source_table.eStructuralFeatures.append(
                                        source_tables_reference)
                    else:
                        if source_to_target_cardinality.strip() == "*":
                            reference_name = reference_name + "s"
                            ereference = ELReference()
                            ereference.name = reference_name
                            ereference.eType = target_class
                            ereference.upperBound = -1
                            ereference.lowerBound = 1
                            ereference.containment = False
                            if the_class.name.endswith("_derived"):

                                the_source_table = context.table_map[the_class]
                                the_target_table = context.table_map[target_class]
                                if not Utils.has_member_called(the_source_table, "sourceTable1"):

                                    source_tables_reference = ELReference()
                                    source_tables_reference.name = "sourceTable1"
                                    source_tables_reference.eType = the_target_table
                                    source_tables_reference.upperBound = -1
                                    source_tables_reference.lowerBound = 0
                                    source_tables_reference.containment = False
                                    the_source_table.eStructuralFeatures.append(
                                        source_tables_reference)
                                else:
                                    source_tables_reference = ELReference(
                                        "sourceTable2", the_target_table, upper=-1, lower=0, containment=False)
                                    the_source_table.eStructuralFeatures.append(
                                        source_tables_reference)
                        else:
                            ereference = ELReference()
                            ereference.name = reference_name
                            ereference.eType = target_class
                            ereference.upperBound = 1
                            ereference.lowerBound = 1
                            ereference.containment = False
                            if the_class.name.endswith("_derived"):
                                the_source_table = context.table_map[the_class]
                                the_target_table = context.table_map[target_class]
                                if not (Utils.has_member_called(the_source_table, "sourceTable1")):

                                    source_tables_reference = ELReference()
                                    source_tables_reference.name = "sourceTable1"
                                    source_tables_reference.eType = the_target_table
                                    source_tables_reference.upperBound = -1
                                    source_tables_reference.lowerBound = 0
                                    source_tables_reference.containment = False
                                    the_source_table.eStructuralFeatures.append(
                                        source_tables_reference)
                                else:

                                    source_tables_reference = ELReference()
                                    source_tables_reference.name = "sourceTable2"
                                    source_tables_reference.eType = the_target_table
                                    source_tables_reference.upperBound = -1
                                    source_tables_reference.lowerBound = 0
                                    source_tables_reference.containment = False
                                    the_source_table.eStructuralFeatures.append(
                                        source_tables_reference)
                    if not the_class is None:
                        the_class.eStructuralFeatures.append(ereference)
