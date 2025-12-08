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
import logging
from pybirdai.models.bird_meta_data_model import SUBDOMAIN, FRAMEWORK, FRAMEWORK_SUBDOMAIN
from pybirdai.process_steps.ancrdt_transformation.csv_column_index_context_ancrdt import ColumnIndexes
from .utils import find_domain_with_id, find_maintenance_agency_with_id
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT

logger = logging.getLogger(__name__)


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

        # Create FRAMEWORK_SUBDOMAIN junction records if frameworks are specified
        # Note: For ANCRDT data, this typically uses 'ANCRDT' framework
        # For DPM data with subdomains, it would use selected_frameworks
        frameworks_to_link = []

        # Check for selected_frameworks in context (DPM workflow)
        if hasattr(sdd_context, 'selected_frameworks') and sdd_context.selected_frameworks:
            frameworks_to_link = sdd_context.selected_frameworks
        # Check for current_framework in context (ANCRDT/FINREP workflow)
        elif hasattr(sdd_context, 'current_framework') and sdd_context.current_framework:
            frameworks_to_link = [sdd_context.current_framework]
        # Default to ANCRDT if importing from ancrdt_csv directory
        elif 'ancrdt' in base_path.lower():
            frameworks_to_link = ['ANCRDT']

        if frameworks_to_link:
            logger.info(f"Creating FRAMEWORK_SUBDOMAIN junction records for {len(subdomains_to_create)} subdomains and {len(frameworks_to_link)} frameworks")
            framework_subdomain_links = []
            framework_cache = {}  # Cache to avoid repeated database queries

            for framework_code in frameworks_to_link:
                # Handle both 'EBA_ANCRDT' and 'ANCRDT' formats
                framework_id_str = framework_code if framework_code.startswith('EBA_') else f"EBA_{framework_code}"

                # Get or cache FRAMEWORK object
                if framework_id_str not in framework_cache:
                    try:
                        framework_obj = FRAMEWORK.objects.get(framework_id=framework_id_str)
                        framework_cache[framework_id_str] = framework_obj
                    except FRAMEWORK.DoesNotExist:
                        logger.warning(f"Framework {framework_id_str} not found, skipping junction records for this framework")
                        continue
                else:
                    framework_obj = framework_cache[framework_id_str]

                # Create junction records for all subdomains with this framework
                for subdomain in subdomains_to_create:
                    framework_subdomain_link = FRAMEWORK_SUBDOMAIN(
                        framework_id=framework_obj,
                        subdomain_id=subdomain
                    )
                    framework_subdomain_links.append(framework_subdomain_link)

            # Bulk create junction records
            if framework_subdomain_links:
                FRAMEWORK_SUBDOMAIN.objects.bulk_create(framework_subdomain_links, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
                logger.info(f"Created {len(framework_subdomain_links)} FRAMEWORK_SUBDOMAIN junction records")
            else:
                logger.warning("No FRAMEWORK_SUBDOMAIN junction records created")
