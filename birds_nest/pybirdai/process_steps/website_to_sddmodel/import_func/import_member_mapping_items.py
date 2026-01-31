# coding=UTF-8
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

"""Import member mapping items from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import MEMBER_MAPPING_ITEM
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .lookups import find_member_with_id, find_variable_with_id, find_member_mapping_with_id
from .warning_writers import save_missing_mapping_variables_to_csv, save_missing_mapping_members_to_csv
from .config import get_csv_file_path
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_member_mapping_items(context):
    """
    Import all member mapping items from the rendering package CSV file using bulk create.

    Args:
        context: SDDContext containing file paths and dictionaries
    """
    file_location = get_csv_file_path(context, "member_mapping_item.csv")
    header_skipped = False
    missing_members = []
    missing_variables = []
    member_mapping_items_to_create = []
    id_increment = 0
    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
                if row[0].upper() == 'ID':  # sometimes exported data without a  primary key has an ID field added at the time of export, exported data is re-imported
                    id_increment = 1
            else:
                member_mapping_id = row[ColumnIndexes().member_mapping_id + id_increment]
                row_number = row[ColumnIndexes().member_mapping_row + id_increment]
                variable_id = row[ColumnIndexes().member_mapping_variable_id + id_increment]
                is_source = row[ColumnIndexes().member_mapping_is_source + id_increment]
                member_id = row[ColumnIndexes().member_mapping_member_id + id_increment]
                if not member_mapping_id.startswith("SHS_"):
                    member = find_member_with_id(
                                                        member_id, context)
                    variable = find_variable_with_id(
                                                        context, variable_id)

                    if member is None:
                        if member_id not in missing_members:
                            missing_members.append((member_id, member_mapping_id, row_number, variable_id))
                    if variable is None:
                        if variable_id not in missing_variables:
                            missing_variables.append((variable_id, '', ''))

                    if member is None or variable is None:
                        pass
                    else:
                        member_mapping_item = MEMBER_MAPPING_ITEM()
                        member_mapping_item.is_source = is_source
                        member_mapping_item.member_id = member
                        member_mapping_item.variable_id = variable
                        member_mapping_item.member_mapping_row = row_number
                        member_mapping_item.member_mapping_id = find_member_mapping_with_id(
                                            context, member_mapping_id)

                        if context.save_sdd_to_db:
                            member_mapping_items_to_create.append(member_mapping_item)
                        try:
                            member_mapping_items_list = context.member_mapping_items_dictionary[member_mapping_id]
                            member_mapping_items_list.append(member_mapping_item)
                        except KeyError:
                            context.member_mapping_items_dictionary[member_mapping_id] = [member_mapping_item]
    if context.save_sdd_to_db and member_mapping_items_to_create:
        MEMBER_MAPPING_ITEM.objects.bulk_create(member_mapping_items_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
    for missing_member in missing_members:
        print(f"Missing member {missing_member}")
    for missing_variable in missing_variables:
        print(f"Missing variable {missing_variable}")
    save_missing_mapping_variables_to_csv(context, missing_variables)
    save_missing_mapping_members_to_csv(context, missing_members)
