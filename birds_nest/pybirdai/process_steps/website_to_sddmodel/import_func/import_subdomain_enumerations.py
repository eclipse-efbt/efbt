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
import logging
from pybirdai.models.bird_meta_data_model import SUBDOMAIN, SUBDOMAIN_ENUMERATION, MEMBER, DOMAIN
from pybirdai.process_steps.ancrdt_transformation.csv_column_index_context_ancrdt import ColumnIndexes
from .lookups import find_member_with_id
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT

logger = logging.getLogger(__name__)


def import_subdomain_enumerations(base_path, sdd_context):
    '''
    Import all subdomain enumerations from CSV file using bulk create.
    If the file doesn't exist, generate enumerations from domain/member data.
    '''
    file_location = base_path + os.sep + "subdomain_enumeration.csv"
    enumerations_to_create = []

    # Check if file exists
    if not os.path.exists(file_location):
        logger.warning(f"subdomain_enumeration.csv not found at {file_location}")
        logger.info("Generating subdomain enumerations from domain/member data...")
        _generate_subdomain_enumerations_from_domains(sdd_context)
        return

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


def _generate_subdomain_enumerations_from_domains(sdd_context):
    '''
    Generate subdomain enumerations by linking subdomains to their domain's members.

    For each subdomain:
    1. Find its associated domain
    2. Get all members of that domain
    3. Create enumeration entries linking members to subdomain
    '''
    enumerations_to_create = []

    # Get all subdomains from database or context
    subdomains = list(SUBDOMAIN.objects.select_related('domain_id').all())
    logger.info(f"Found {len(subdomains)} subdomains to process")

    for subdomain in subdomains:
        domain = subdomain.domain_id
        if not domain:
            logger.debug(f"Subdomain {subdomain.subdomain_id} has no domain, skipping")
            continue

        # Get all members for this domain
        members = MEMBER.objects.filter(domain_id=domain)

        for order, member in enumerate(members):
            # Check if enumeration already exists
            if not SUBDOMAIN_ENUMERATION.objects.filter(
                subdomain_id=subdomain,
                member_id=member
            ).exists():
                enumeration = SUBDOMAIN_ENUMERATION(
                    subdomain_id=subdomain,
                    member_id=member,
                    order=order
                )
                enumerations_to_create.append(enumeration)

                # Update context dictionary
                subdomain_id = subdomain.subdomain_id
                if subdomain_id not in sdd_context.subdomain_enumeration_dictionary:
                    sdd_context.subdomain_enumeration_dictionary[subdomain_id] = []
                sdd_context.subdomain_enumeration_dictionary[subdomain_id].append(enumeration)

    if sdd_context.save_sdd_to_db and enumerations_to_create:
        logger.info(f"Creating {len(enumerations_to_create)} subdomain enumerations from domain/member data")
        SUBDOMAIN_ENUMERATION.objects.bulk_create(
            enumerations_to_create,
            batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT,
            ignore_conflicts=True
        )
    else:
        logger.info("No subdomain enumerations to create")
