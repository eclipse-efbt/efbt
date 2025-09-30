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
    """Maps TYP_INSTRMNT values to product-specific slice classes"""
    
    def __init__(self):
        self.typ_to_slices = {}
        self.load_mappings()
    
    def load_mappings(self):
        """Load TYP_INSTRMNT to slice mappings from CSV file"""
        base_dir = settings.BASE_DIR
        mapping_file = os.path.join(base_dir, 'resources', 'joins_configuration', 
                                   'join_for_product_to_reference_category_FINREP_REF.csv')
        
        if os.path.exists(mapping_file):
            with open(mapping_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    typ_instrmnt = row['Main Category']
                    slice_name = row['slice_name']
                    
                    if typ_instrmnt not in self.typ_to_slices:
                        self.typ_to_slices[typ_instrmnt] = []
                    
                    if slice_name not in self.typ_to_slices[typ_instrmnt]:
                        self.typ_to_slices[typ_instrmnt].append(slice_name)
        else:
            print(f"Warning: Mapping file not found: {mapping_file}")
    
    def get_slices_for_typ_instrmnt(self, typ_instrmnt):
        """Get list of slice names for a given TYP_INSTRMNT value"""
        # Handle both full (e.g., TYP_INSTRMNT_114) and numeric (e.g., 114) formats
        if typ_instrmnt.startswith('TYP_INSTRMNT_'):
            key = typ_instrmnt
        else:
            key = f'TYP_INSTRMNT_{typ_instrmnt}'
        
        return self.typ_to_slices.get(key, [])
    
    def get_all_mappings(self):
        """Return all TYP_INSTRMNT to slice mappings"""
        return self.typ_to_slices.copy()
    
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