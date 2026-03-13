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

import csv
import os
from django.conf import settings

class TypInstrmntMapper:
    """Maps conditions (e.g., TYP_INSTRMNT=TYP_INSTRMNT_114) to product-specific slice classes"""

    def __init__(self):
        self.condition_to_slices = {}
        self.load_mappings()

    def load_mappings(self):
        """Load condition to slice mappings from CSV file"""
        base_dir = settings.BASE_DIR
        mapping_file = os.path.join(base_dir, 'artefacts', 'joins_configuration',
                                   'join_for_product_to_reference_category_FINREP_REF.csv')

        if os.path.exists(mapping_file):
            with open(mapping_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    condition = row['Main Category']
                    slice_name = row['slice_name']

                    if condition not in self.condition_to_slices:
                        self.condition_to_slices[condition] = []

                    if slice_name not in self.condition_to_slices[condition]:
                        self.condition_to_slices[condition].append(slice_name)
        else:
            print(f"Warning: Mapping file not found: {mapping_file}")

        mapping_file = os.path.join(base_dir, 'artefacts', 'joins_configuration',
                                   'join_for_product_to_reference_category_COREP_REF.csv')

        if os.path.exists(mapping_file):
            with open(mapping_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    condition = row['Main Category']
                    slice_name = row['slice_name']

                    if condition not in self.condition_to_slices:
                        self.condition_to_slices[condition] = []

                    if slice_name not in self.condition_to_slices[condition]:
                        self.condition_to_slices[condition].append(slice_name)
        else:
            print(f"Warning: Mapping file not found: {mapping_file}")

    def get_slices_for_condition(self, condition):
        """Get list of slice names for a given condition (e.g., TYP_INSTRMNT=TYP_INSTRMNT_114)"""
        return self.condition_to_slices.get(condition, [])
    
    def get_all_mappings(self):
        """Return all condition to slice mappings"""
        return self.condition_to_slices.copy()
    
    def format_slice_name_for_class(self, slice_name, cube_id):
        """Convert slice name to class name format
        e.g., 'Other loans' -> 'F_01_01_REF_FINREP_3_0_Other_loans_Table'
        """
        # Replace spaces with underscores
        formatted_name = slice_name.replace(' ', '_')
        return f"{cube_id}_{formatted_name}_Table"
    
    def format_slice_name_for_import(self, slice_name):
        """Convert slice name to import format
        e.g., 'Other loans' -> 'Other_loans'
        """
        return slice_name.replace(' ', '_')