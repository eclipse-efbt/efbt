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

"""Import member hierarchies from CSV file."""

import csv
from pybirdai.models.bird_meta_data_model import MEMBER_HIERARCHY
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots
from .lookups import find_maintenance_agency_with_id, find_domain_with_id
from .warning_writers import save_missing_domains_to_csv
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_member_hierarchies(context):
    """
    Import all member hierarchies with batch processing.

    Args:
        context: SDDContext containing file paths and dictionaries
    """
    missing_domains = set()  # Using set for faster lookups
    hierarchies_to_create = []

    with open(f"{context.file_directory}/technical_export/member_hierarchy.csv", encoding='utf-8') as csvfile:
        next(csvfile)  # Skip header more efficiently
        for row in csv.reader(csvfile):
            maintenance_agency_id = row[ColumnIndexes().member_hierarchy_maintenance_agency]
            code = row[ColumnIndexes().member_hierarchy_code]
            id = row[ColumnIndexes().member_hierarchy_id]
            domain_id = row[ColumnIndexes().member_hierarchy_domain_id]
            description = row[ColumnIndexes().member_hierarchy_description]

            maintenance_agency = find_maintenance_agency_with_id(context, maintenance_agency_id)
            domain = find_domain_with_id(context, domain_id)

            if domain is None:
                missing_domains.add(domain_id)
                continue

            hierarchy = MEMBER_HIERARCHY(
                name=replace_dots(id),
                member_hierarchy_id=replace_dots(id),
                code=code,
                description=description,
                maintenance_agency_id=maintenance_agency,
                domain_id=domain
            )

            if hierarchy.member_hierarchy_id not in context.member_hierarchy_dictionary:
                hierarchies_to_create.append(hierarchy)
                context.member_hierarchy_dictionary[hierarchy.member_hierarchy_id] = hierarchy

    if context.save_sdd_to_db and hierarchies_to_create:
        MEMBER_HIERARCHY.objects.bulk_create(hierarchies_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)

    if missing_domains:
        save_missing_domains_to_csv(context, list(missing_domains))
