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

"""Import domains from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import DOMAIN
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots
from .lookups import find_maintenance_agency_with_id
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_domains(context, ref, config=None):
    """
    Import all domains from CSV file using bulk create.

    Args:
        context: SDDContext containing file paths and dictionaries
        ref: Boolean indicating if importing reference domains (ECB) or others
        config: Optional DatasetConfig for dynamic file paths and filtering
    """
    # Determine file directory based on config
    file_dir = config.file_directory if config else "smcubes_artefacts"
    file_location = context.file_directory + os.sep + file_dir + os.sep + "domain.csv"
    header_skipped = False
    domains_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
            else:
                maintenence_agency = row[ColumnIndexes().domain_maintenence_agency]
                code = row[ColumnIndexes().domain_domain_id_index]
                data_type = row[ColumnIndexes().domain_domain_data_type]
                description = row[ColumnIndexes().domain_domain_description]
                domain_id = row[ColumnIndexes().domain_domain_true_id]
                is_enumerated = row[ColumnIndexes().domain_domain_is_enumerated]
                is_reference = row[ColumnIndexes().domain_domain_is_reference]
                domain_name = row[ColumnIndexes().domain_domain_name_index]

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
                    domain = DOMAIN(name=replace_dots(domain_id))

                    # Save original string value for is_reference logic
                    maintenance_agency_string = maintenence_agency

                    # Convert to object for database relation
                    if maintenence_agency == "":
                        maintenence_agency = find_maintenance_agency_with_id(context, "SDD_DOMAIN")
                    else:
                        maintenence_agency = find_maintenance_agency_with_id(context, maintenence_agency)
                    domain.maintenance_agency_id = maintenence_agency
                    domain.code = code
                    domain.description = description
                    domain.domain_id = replace_dots(domain_id)
                    domain.name = domain_name

                    # Temporarily set from CSV - will be corrected in post-processing based on members
                    domain.is_enumerated = is_enumerated.lower() == 'true' if isinstance(is_enumerated, str) else bool(is_enumerated)

                    # Business rule: is_reference based on maintenance agency (use original string)
                    # EBA → not reference, ECB (or contains ECB) → reference
                    if maintenance_agency_string == "EBA":
                        domain.is_reference = False
                    elif maintenance_agency_string == "ECB" or "ECB" in maintenance_agency_string:
                        domain.is_reference = True
                    else:
                        # Parse from CSV for other agencies
                        domain.is_reference = is_reference.lower() == 'true' if isinstance(is_reference, str) else bool(is_reference)

                    domains_to_create.append(domain)
                    if ref:
                        context.domain_dictionary[domain.domain_id] = domain
                    else:
                        context.domain_dictionary[domain.domain_id] = domain

    if context.save_sdd_to_db and domains_to_create:
        DOMAIN.objects.bulk_create(domains_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
        # Note: is_enumerated will be updated after members are imported (in import_members.py)
