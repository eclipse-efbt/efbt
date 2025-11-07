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

"""Import member mappings from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import MEMBER_MAPPING
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .lookups import find_maintenance_agency_with_id


def import_member_mappings(context):
    """
    Import all member mappings from the rendering package CSV file.

    Args:
        context: SDDContext containing file paths and dictionaries
    """
    file_location = context.file_directory + os.sep + "technical_export" + os.sep + "member_mapping.csv"
    header_skipped = False
    member_mappings_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
            else:
                maintenance_agency_id = row[ColumnIndexes().member_map_maintenance_agency_id]
                member_mapping_id = row[ColumnIndexes().member_map_member_mapping_id]
                name = row[ColumnIndexes().member_map_name]
                code = row[ColumnIndexes().member_map_code]
                if not member_mapping_id.startswith("SHS_"):
                    member_mapping = MEMBER_MAPPING()
                    member_mapping.member_mapping_id = member_mapping_id
                    member_mapping.name = name
                    member_mapping.code = code
                    member_mapping.maintenance_agency_id = find_maintenance_agency_with_id(
                        context, maintenance_agency_id)

                    member_mappings_to_create.append(member_mapping)
                    context.member_mapping_dictionary[member_mapping_id] = member_mapping

    if context.save_sdd_to_db and member_mappings_to_create:
        MEMBER_MAPPING.objects.bulk_create(member_mappings_to_create, batch_size=1000, ignore_conflicts=True)
