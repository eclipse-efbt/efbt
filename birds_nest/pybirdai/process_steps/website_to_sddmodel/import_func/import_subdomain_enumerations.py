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
#    Benjamin Arfa - refactoring into modular structure

"""Import subdomain enumerations from ANCRDT CSV files."""

import os
import csv
from pybirdai.models.bird_meta_data_model import SUBDOMAIN, SUBDOMAIN_ENUMERATION
from pybirdai.process_steps.ancrdt_transformation.csv_column_index_context_ancrdt import ColumnIndexes
from .utils import find_member_with_id
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_subdomain_enumerations(base_path, sdd_context):
    '''
    Import all subdomain enumerations from CSV file using bulk create
    '''
    file_location = base_path + os.sep + "subdomain_enumeration.csv"
    enumerations_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        rows = list(csv.reader(csvfile))[1:]  # Skip header

        for row in rows:
            subdomain_id = row[ColumnIndexes().sdd_subdomain_enumeration_subdomain_id_id]
            member_id = row[ColumnIndexes().sdd_subdomain_enumeration_member_id_id]
            order = row[ColumnIndexes().sdd_subdomain_enumeration_order]
            valid_from = row[ColumnIndexes().sdd_subdomain_enumeration_valid_from]
            valid_to = row[ColumnIndexes().sdd_subdomain_enumeration_valid_to]

            member = find_member_with_id(member_id, sdd_context)

            if member and not SUBDOMAIN_ENUMERATION.objects.filter(
                    subdomain_id=sdd_context.subdomain_dictionary.get(subdomain_id, SUBDOMAIN.objects.get(subdomain_id=subdomain_id)),
                    member_id=member
                ).exists():
                enumeration = SUBDOMAIN_ENUMERATION(
                    subdomain_id=sdd_context.subdomain_dictionary.get(subdomain_id, SUBDOMAIN.objects.get(subdomain_id=subdomain_id)),
                    member_id=member,
                    order=order,
                    valid_from=valid_from,
                    valid_to=valid_to
                )

                enumerations_to_create.append(enumeration)

                if subdomain_id not in sdd_context.subdomain_enumeration_dictionary:
                    sdd_context.subdomain_enumeration_dictionary[subdomain_id] = []
                sdd_context.subdomain_enumeration_dictionary[subdomain_id].append(enumeration)

    if sdd_context.save_sdd_to_db and enumerations_to_create:
        SUBDOMAIN_ENUMERATION.objects.bulk_create(enumerations_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
