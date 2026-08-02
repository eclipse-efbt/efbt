# coding=UTF-8
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
from pybirdai.process_steps.utils import Utils
import os

from pybirdai.model_blueprint import Annotation, AnnotationDetail, Field, Relationship

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

                    if (relational_model_name == context.input_layer_name) and (table_name is not None) and (entity_name is not None) and not (table_name.strip() == "") and not (entity_name.strip() == ""):


                        # annotate entites
                        if logical_object_name == entity_name:
                            ldm_entity = InputLayerLinkEnricher.get_ldm_entity(
                                self,
                                context,
                                Utils.make_valid_id(entity_name))

                            the_entity_annotation = ldm_entity.annotation_with_source("il_mapping")

                            if the_entity_annotation is None:
                                the_entity_annotation = ldm_entity.add_annotation(Annotation(
                                    source=ldm_entity.package.annotation_directive("il_mapping")))

                            details = the_entity_annotation.details

                            # an entity can map onto more than one input layer table,
                            # so later tables are numbered il_table1, il_table2, ...
                            il_tables_count = sum(
                                1 for detail in details if detail.key.startswith("il_table"))

                            if il_tables_count == 0:
                                detail_key = "il_table"
                            else:
                                detail_key = "il_table" + str(il_tables_count)

                            details.append(AnnotationDetail(key=detail_key, value=table_name))
                        else:

                            # annotate attributes
                            ldm_attribute = InputLayerLinkEnricher.get_ldm_attribute(
                                self,
                                context,
                                Utils.make_valid_id(entity_name),
                                Utils.make_valid_id(logical_object_name))

                            # logical_attribute_to_relational_name[ldm_attribute] =  table_name + "." + relational_object_Name
                            if not(ldm_attribute is None):
                                # a field keeps the qualified table.column, a
                                # relationship only the column it was mapped onto
                                if isinstance(ldm_attribute, Field):
                                    il_column = table_name + "." + relational_object_Name
                                elif isinstance(ldm_attribute, Relationship):
                                    il_column = relational_object_Name
                                else:
                                    il_column = None

                                if il_column is not None:
                                    the_member_annotation = ldm_attribute.annotation_with_source("il_mapping")
                                    if the_member_annotation is None:
                                        owning_package = ldm_attribute.owner.package
                                        the_member_annotation = ldm_attribute.add_annotation(Annotation(
                                            source=owning_package.annotation_directive("il_mapping")))

                                    the_member_annotation.add_detail("il_column", il_column)



    def get_ldm_attribute(self, context,entity_name,attribute_name):
        for model_class in context.ldm_entities_package.model_classes:
            for member in model_class.members:
                # a field only matches when its class matches too, a relationship
                # matches on its own long name wherever it is declared
                if isinstance(member, Field):
                    if model_class.annotation_detail("long_name", "long_name") != entity_name:
                        continue
                if member.annotation_detail("long_name", "long_name") == attribute_name:
                    return member


    def get_ldm_entity(self, context,entity_name):
        for model_class in context.ldm_entities_package.model_classes:
            if model_class.annotation_detail("long_name", "long_name") == entity_name:
                return model_class
