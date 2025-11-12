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

"""Import variables from CSV file."""

import os
import csv
from pybirdai.models.bird_meta_data_model import VARIABLE
from pybirdai.context.csv_column_index_context import ColumnIndexes
from .utilities import replace_dots
from .lookups import find_maintenance_agency_with_id, find_domain_with_id
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT


def import_variables(context, ref, config=None):
    """
    Import all variables from CSV file using bulk create.

    Args:
        context: SDDContext containing file paths and dictionaries
        ref: Boolean indicating if importing reference variables (ECB) or others
        config: Optional DatasetConfig for dynamic file paths and filtering
    """
    # Determine file directory based on config
    file_dir = config.file_directory if config else "technical_export"
    file_location = context.file_directory + os.sep + file_dir + os.sep + "variable.csv"
    header_skipped = False
    variables_to_create = []

    with open(file_location, encoding='utf-8') as csvfile:
        filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in filereader:
            if not header_skipped:
                header_skipped = True
            else:
                maintenence_agency = row[ColumnIndexes().variable_variable_maintenence_agency]
                code = row[ColumnIndexes().variable_code_index]
                description = row[ColumnIndexes().variable_variable_description]
                domain_id = row[ColumnIndexes().variable_domain_index]
                name = row[ColumnIndexes().variable_long_name_index]
                variable_id = row[ColumnIndexes().variable_variable_true_id]
                primary_concept = row[ColumnIndexes().variable_primary_concept]

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
                    variable = VARIABLE(name=replace_dots(variable_id))
                    maintenance_agency_id = find_maintenance_agency_with_id(context, maintenence_agency)
                    variable.code = code
                    variable.variable_id = replace_dots(variable_id)
                    variable.name = name
                    domain = find_domain_with_id(context, domain_id)
                    variable.domain_id = domain
                    variable.description = description
                    variable.maintenance_agency_id = maintenance_agency_id

                    variables_to_create.append(variable)
                    context.variable_dictionary[variable.variable_id] = variable
                    context.variable_to_domain_map[variable.variable_id] = domain
                    context.variable_to_long_names_map[variable.variable_id] = name
                    if not((primary_concept == "") or (primary_concept == None)):
                        context.variable_to_primary_concept_map[variable.variable_id] = primary_concept

    if context.save_sdd_to_db and variables_to_create:
        VARIABLE.objects.bulk_create(variables_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT, ignore_conflicts=True)
