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

"""Import members from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import MEMBER
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots
from .lookups import find_maintenance_agency_with_id, find_domain_with_id
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_members(context, ref, config=None):
    """
    Import all members from CSV file using bulk create.

    Args:
        context: SDDContext containing file paths and dictionaries
        ref: Boolean indicating if importing reference members (ECB) or others
        config: Optional DatasetConfig for dynamic file paths and filtering
    """
    # Determine file directory based on config
    file_dir = config.file_directory if config else "technical_export"
    file_location = context.file_directory + os.sep + file_dir + os.sep + "member.csv"
    header_skipped = False
    members_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
            else:
                code = row[ColumnIndexes().member_member_code_index]
                description = row[ColumnIndexes().member_member_descriptions]
                domain_id = row[ColumnIndexes().member_domain_id_index]
                member_id = row[ColumnIndexes().member_member_id_index]
                member_name = row[ColumnIndexes().member_member_name_index]
                maintenence_agency = row[ColumnIndexes().member_member_maintenence_agency]

                if (member_name is None) or (member_name == ""):
                    member_name = member_id

                # Determine if entity should be included
                if config and config.bypass_ecb_filter:
                    # For ANCRDT and other datasets that don't filter by ECB
                    include = True
                else:
                    # Original ECB filtering logic
                    include = False
                    if (ref) and (maintenence_agency == "ECB"):
                        include = True
                    if (not ref) and not (maintenence_agency == "ECB"):
                        include = True

                if include:
                    member = MEMBER(name=replace_dots(member_id))
                    member.member_id = replace_dots(member_id)
                    member.code = code
                    member.description = description
                    member.name = member_name
                    maintenance_agency = find_maintenance_agency_with_id(context, maintenence_agency)
                    member.maintenance_agency_id = maintenance_agency
                    domain = find_domain_with_id(context, domain_id)
                    member.domain_id = domain

                    members_to_create.append(member)
                    context.member_dictionary[member.member_id] = member

                    if not (domain_id is None) and not (domain_id == ""):
                        context.member_id_to_domain_map[member] = domain
                        context.member_id_to_member_code_map[member.member_id] = code

    if context.save_sdd_to_db and members_to_create:
        MEMBER.objects.bulk_create(members_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
