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

"""Import subdomains from ANCRDT CSV files."""

import os
import csv
from pybirdai.models.bird_meta_data_model import SUBDOMAIN
from pybirdai.process_steps.ancrdt_transformation.csv_column_index_context_ancrdt import ColumnIndexes
from .utils import find_domain_with_id, find_maintenance_agency_with_id
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_subdomains(base_path, sdd_context):
    '''
    Import all subdomains from CSV file using bulk create
    '''
    file_location = base_path + os.sep + "subdomain.csv"
    subdomains_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        rows = list(csv.reader(csvfile))[1:]  # Skip header

        for row in rows:
            subdomain_id = row[ColumnIndexes().sdd_subdomain_subdomain_id]
            domain_id = row[ColumnIndexes().sdd_subdomain_domain_id_id]
            maintenance_agency = row[ColumnIndexes().sdd_subdomain_maintenance_agency_id_id]
            name = row[ColumnIndexes().sdd_subdomain_name]
            code = row[ColumnIndexes().sdd_subdomain_code]

            if not SUBDOMAIN.objects.filter(subdomain_id=subdomain_id).exists():
                subdomain = SUBDOMAIN(
                    subdomain_id=subdomain_id,
                    name=name,
                    domain_id=find_domain_with_id(sdd_context, domain_id),
                    maintenance_agency_id=find_maintenance_agency_with_id(sdd_context, maintenance_agency)
                )

                subdomains_to_create.append(subdomain)
                sdd_context.subdomain_dictionary[subdomain_id] = subdomain

    if sdd_context.save_sdd_to_db and subdomains_to_create:
        SUBDOMAIN.objects.bulk_create(subdomains_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
