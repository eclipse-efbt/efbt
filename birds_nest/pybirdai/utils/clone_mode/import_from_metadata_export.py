import csv
import zipfile
import io
import os
import glob
import logging
import json
import subprocess
import tempfile
import itertools
from datetime import datetime
from pathlib import Path

# Allowed table name pattern: letters, digits, underscores only
import re
class DjangoSetup:
    @staticmethod
    def setup():
        import django
        from django.conf import settings
        if not settings.configured:
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')
            django.setup()

# Ensure Django is set up
DjangoSetup.setup()

from django.db import transaction
from django.db import models
from django.db import connection
from pybirdai import bird_meta_data_model
from pybirdai.utils.clone_mode.clone_mode_column_index import ColumnIndexes
import traceback

# Set up logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add console handler if not already present
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

class CSVDataImporter:
    def __init__(self, results_dir="import_results"):
        self.model_map = {}
        self.column_mappings = {}
        self.results_dir = results_dir
        self.id_mappings = {}  # Track ID mappings for models with auto-generated IDs
        self._build_model_map()
        self._build_column_mappings()
        self._ensure_results_directory()
        logger.info("CSVDataImporter initialized")

    def _ensure_results_directory(self):
        """Ensure the results directory exists"""
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)
            logger.info(f"Created results directory: {self.results_dir}")

    def _save_results(self, results, operation_type="import"):
        """Save import results to a JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{operation_type}_results_{timestamp}.json"
        filepath = os.path.join(self.results_dir, filename)

        # Prepare results for JSON serialization (remove non-serializable objects)
        serializable_results = {}
        for key, value in results.items():
            if isinstance(value, dict):
                serializable_value = {
                    'success': value.get('success', False),
                    'imported_count': value.get('imported_count', 0)
                }
                if 'error' in value:
                    serializable_value['error'] = value['error']
                serializable_results[key] = serializable_value
            else:
                serializable_results[key] = value

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(serializable_results, f, indent=2, ensure_ascii=False)
            logger.info(f"Results saved to: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to save results to {filepath}: {e}")
            return None

    def _build_model_map(self):
        """Build mapping of table names to model classes"""
        import inspect
        from django.db import models

        logger.debug("Building model map from bird_meta_data_model")
        model_count = 0

        for name, obj in inspect.getmembers(bird_meta_data_model):
            if inspect.isclass(obj) and issubclass(obj, models.Model) and obj != models.Model:
                self.model_map[obj._meta.db_table] = obj
                model_count += 1
                logger.debug(f"Added model {name} -> table {obj._meta.db_table}")

        logger.info(f"Built model map with {model_count} models")

    def _build_column_mappings(self):
        """Build column index mappings for each model type"""
        col_idx = ColumnIndexes()

        # Note: Some tables may have an ID column at index 0 if they use Django's auto-generated primary key
        # This is handled dynamically in import_csv_file method

        # Maintenance Agency mappings
        self.column_mappings['pybirdai_maintenance_agency'] = {
            col_idx.maintenance_agency_id: 'maintenance_agency_id',
            col_idx.maintenance_agency_code: 'code',
            col_idx.maintenance_agency_name: 'name',
            col_idx.maintenance_agency_description: 'description'
        }

        # Framework mappings
        self.column_mappings['pybirdai_framework'] = {
            col_idx.framework_maintenance_agency_id: 'maintenance_agency_id',
            col_idx.framework_id: 'framework_id',
            col_idx.framework_name: 'name',
            col_idx.framework_code: 'code',
            col_idx.framework_description: 'description',
            col_idx.framework_type: 'framework_type',
            col_idx.framework_reporting_population: 'reporting_population',
            col_idx.framework_other_links: 'other_links',
            col_idx.framework_order: 'order',
            col_idx.framework_status: 'status'
        }

        # Domain mappings
        self.column_mappings['pybirdai_domain'] = {
            col_idx.domain_maintenance_agency: 'maintenance_agency_id',
            col_idx.domain_domain_true_id: 'domain_id',
            col_idx.domain_domain_name_index: 'name',
            col_idx.domain_domain_is_enumerated: 'is_enumerated',
            col_idx.domain_domain_description: 'description',
            col_idx.domain_domain_data_type: 'data_type',
            col_idx.domain_code: 'code',
            col_idx.domain_facet_id: 'facet_id',
            col_idx.domain_domain_is_reference: 'is_reference'
        }

        # Variable mappings
        self.column_mappings['pybirdai_variable'] = {
            col_idx.variable_maintenance_agency: 'maintenance_agency_id',
            col_idx.variable_variable_true_id: 'variable_id',
            col_idx.variable_variable_name_index: 'name',
            col_idx.variable_code_index: 'code',
            col_idx.variable_domain_index: 'domain_id',
            col_idx.variable_variable_description: 'description',
            col_idx.variable_primary_concept: 'primary_concept',
            col_idx.variable_is_decomposed: 'is_decomposed'
        }

        # Member mappings
        self.column_mappings['pybirdai_member'] = {
            col_idx.member_maintenance_agency: 'maintenance_agency_id',
            col_idx.member_member_id_index: 'member_id',
            col_idx.member_member_code_index: 'code',
            col_idx.member_member_name_index: 'name',
            col_idx.member_domain_id_index: 'domain_id',
            col_idx.member_member_descriptions: 'description'
        }

        # Variable Set mappings
        self.column_mappings['pybirdai_variable_set'] = {
            col_idx.variable_set_maintenance_agency_id: 'maintenance_agency_id',
            col_idx.variable_set_variable_set_id: 'variable_set_id',
            col_idx.variable_set_name: 'name',
            col_idx.variable_set_code: 'code',
            col_idx.variable_set_description: 'description'
        }

        # Variable Set Enumeration mappings
        self.column_mappings['pybirdai_variable_set_enumeration'] = {
            col_idx.variable_set_enumeration_valid_set: 'variable_set_id',
            col_idx.variable_set_enumeration_variable_id: 'variable_id',
            col_idx.variable_set_enumeration_valid_from: 'valid_from',
            col_idx.variable_set_enumeration_valid_to: 'valid_to',
            col_idx.variable_set_enumeration_subdomain_id: 'subdomain_id',
            col_idx.variable_set_enumeration_is_flow: 'is_flow',
            col_idx.variable_set_enumeration_order: 'order'
        }

        # Subdomain mappings
        self.column_mappings['pybirdai_subdomain'] = {
            col_idx.subdomain_maintenance_agency_id: 'maintenance_agency_id',
            col_idx.subdomain_subdomain_id_index: 'subdomain_id',
            col_idx.subdomain_subdomain_name: 'name',
            col_idx.subdomain_domain_id_index: 'domain_id',
            col_idx.subdomain_is_listed: 'is_listed',
            col_idx.subdomain_subdomain_code: 'code',
            col_idx.subdomain_facet_id: 'facet_id',
            col_idx.subdomain_subdomain_description: 'description',
            col_idx.subdomain_is_natural: 'is_natural'
        }

        # Subdomain Enumeration mappings
        self.column_mappings['pybirdai_subdomain_enumeration'] = {
            col_idx.subdomain_enumeration_member_id_index: 'member_id',
            col_idx.subdomain_enumeration_subdomain_id_index: 'subdomain_id',
            col_idx.subdomain_enumeration_valid_from: 'valid_from',
            col_idx.subdomain_enumeration_valid_to_index: 'valid_to',
            col_idx.subdomain_enumeration_order: 'order'
        }

        # Member Hierarchy mappings
        self.column_mappings['pybirdai_member_hierarchy'] = {
            col_idx.member_hierarchy_maintenance_agency: 'maintenance_agency_id',
            col_idx.member_hierarchy_id: 'member_hierarchy_id',
            col_idx.member_hierarchy_code: 'code',
            col_idx.member_hierarchy_domain_id: 'domain_id',
            col_idx.member_hierarchy_name: 'name',
            col_idx.member_hierarchy_description: 'description',
            col_idx.member_hierarchy_is_main_hierarchy: 'is_main_hierarchy'
        }

        # Member Hierarchy Node mappings
        self.column_mappings['pybirdai_member_hierarchy_node'] = {
            col_idx.member_hierarchy_node_hierarchy_id: 'member_hierarchy_id',
            col_idx.member_hierarchy_node_member_id: 'member_id',
            col_idx.member_hierarchy_node_level: 'level',
            col_idx.member_hierarchy_node_parent_member_id: 'parent_member_id',
            col_idx.member_hierarchy_node_comparator: 'comparator',
            col_idx.member_hierarchy_node_operator: 'operator',
            col_idx.member_hierarchy_node_valid_from: 'valid_from',
            col_idx.member_hierarchy_node_valid_to: 'valid_to'
        }

        # Table mappings
        self.column_mappings['pybirdai_table'] = {
            col_idx.table_table_id: 'table_id',
            col_idx.table_table_name: 'name',
            col_idx.table_code: 'code',
            col_idx.table_description: 'description',
            col_idx.table_maintenance_agency_id: 'maintenance_agency_id',
            col_idx.table_version: 'version',
            col_idx.table_valid_from: 'valid_from',
            col_idx.table_valid_to: 'valid_to'
        }

        # Axis mappings
        self.column_mappings['pybirdai_axis'] = {
            col_idx.axis_id: 'axis_id',
            col_idx.axis_code: 'code',
            col_idx.axis_orientation: 'orientation',
            col_idx.axis_order: 'order',
            col_idx.axis_name: 'name',
            col_idx.axis_description: 'description',
            col_idx.axis_table_id: 'table_id',
            col_idx.axis_is_open_axis: 'is_open_axis'
        }

        # Axis Ordinate mappings
        self.column_mappings['pybirdai_axis_ordinate'] = {
            col_idx.axis_ordinate_axis_ordinate_id: 'axis_ordinate_id',
            col_idx.axis_ordinate_is_abstract_header: 'is_abstract_header',
            col_idx.axis_ordinate_code: 'code',
            col_idx.axis_ordinate_order: 'order',
            col_idx.axis_ordinate_level: 'level',
            col_idx.axis_ordinate_path: 'path',
            col_idx.axis_ordinate_axis_id: 'axis_id',
            col_idx.axis_ordinate_parent_axis_ordinate_id: 'parent_axis_ordinate_id',
            col_idx.axis_ordinate_name: 'name',
            col_idx.axis_ordinate_description: 'description'
        }

        # Ordinate Item mappings
        self.column_mappings['pybirdai_ordinate_item'] = {
            col_idx.ordinate_item_axis_ordinate_id: 'axis_ordinate_id',
            col_idx.ordinate_item_variable_id: 'variable_id',
            col_idx.ordinate_item_member_id: 'member_id',
            col_idx.ordinate_item_member_hierarchy_id: 'member_hierarchy_id',
            col_idx.ordinate_item_member_hierarchy_valid_from: 'member_hierarchy_valid_from',
            col_idx.ordinate_item_starting_member_id: 'starting_member_id',
            col_idx.ordinate_item_is_starting_member_included: 'is_starting_member_included'
        }

        # Table Cell mappings
        self.column_mappings['pybirdai_table_cell'] = {
            col_idx.table_cell_cell_id: 'cell_id',
            col_idx.table_cell_is_shaded: 'is_shaded',
            col_idx.table_cell_combination_id: 'table_cell_combination_id',
            col_idx.table_cell_table_id: 'table_id',
            col_idx.table_cell_system_data_code: 'system_data_code',
            col_idx.table_cell_name: 'name'
        }

        # Cell Position mappings
        self.column_mappings['pybirdai_cell_position'] = {
            col_idx.cell_positions_cell_id: 'cell_id',
            col_idx.cell_positions_axis_ordinate_id: 'axis_ordinate_id'
        }

        # Member Mapping mappings
        self.column_mappings['pybirdai_member_mapping'] = {
            col_idx.member_mapping_maintenance_agency_id: 'maintenance_agency_id',
            col_idx.member_mapping_member_mapping_id: 'member_mapping_id',
            col_idx.member_mapping_name: 'name',
            col_idx.member_mapping_code: 'code'
        }

        # Member Mapping Item mappings
        self.column_mappings['pybirdai_member_mapping_item'] = {
            col_idx.member_mapping_item_member_mapping_id: 'member_mapping_id',
            col_idx.member_mapping_row: 'member_mapping_row',
            col_idx.member_mapping_variable_id: 'variable_id',
            col_idx.member_mapping_is_source: 'is_source',
            col_idx.member_mapping_member_id: 'member_id',
            col_idx.member_mapping_item_valid_from: 'valid_from',
            col_idx.member_mapping_item_valid_to: 'valid_to',
            col_idx.member_mapping_item_member_hierarchy: 'member_hierarchy'
        }

        # Combination mappings
        self.column_mappings['pybirdai_combination'] = {
            col_idx.combination_combination_id: 'combination_id',
            col_idx.combination_combination_code: 'code',
            col_idx.combination_combination_name: 'name',
            col_idx.combination_maintenance_agency: 'maintenance_agency_id',
            col_idx.combination_version: 'version',
            col_idx.combination_valid_from: 'valid_from',
            col_idx.combination_combination_valid_to: 'valid_to',
            col_idx.combination_metric: 'metric'
        }

        # Combination Item mappings
        self.column_mappings['pybirdai_combination_item'] = {
            col_idx.combination_item_combination_id: 'combination_id',
            col_idx.combination_item_variable_id: 'variable_id',
            col_idx.combination_item_subdomain_id: 'subdomain_id',
            col_idx.combination_variable_set: 'variable_set_id',
            col_idx.combination_member_id: 'member_id',
            col_idx.combination_item_member_hierarchy: 'member_hierarchy'
        }

        # Mapping Definition mappings
        self.column_mappings['pybirdai_mapping_definition'] = {
            col_idx.mapping_definition_maintenance_agency_id: 'maintenance_agency_id',
            col_idx.mapping_definition_mapping_id: 'mapping_id',
            col_idx.mapping_definition_name: 'name',
            col_idx.mapping_definition_mapping_type: 'mapping_type',
            col_idx.mapping_definition_code: 'code',
            col_idx.mapping_definition_algorithm: 'algorithm',
            col_idx.mapping_definition_member_mapping_id: 'member_mapping_id',
            col_idx.mapping_definition_variable_mapping_id: 'variable_mapping_id'
        }

        # Mapping To Cube mappings
        self.column_mappings['pybirdai_mapping_to_cube'] = {
            col_idx.mapping_to_cube_cube_mapping_id: 'cube_mapping_id',
            col_idx.mapping_to_cube_mapping_id: 'mapping_id',
            col_idx.mapping_to_cube_valid_from: 'valid_from',
            col_idx.mapping_to_cube_valid_to: 'valid_to'
        }

        # Variable Mapping mappings
        self.column_mappings['pybirdai_variable_mapping'] = {
            col_idx.variable_mapping_variable_mapping_id: 'variable_mapping_id',
            col_idx.variable_mapping_maintenance_agency_id: 'maintenance_agency_id',
            col_idx.variable_mapping_code: 'code',
            col_idx.variable_mapping_name: 'name'
        }

        # Variable Mapping Item mappings
        self.column_mappings['pybirdai_variable_mapping_item'] = {
            col_idx.variable_mapping_item_variable_mapping_id: 'variable_mapping_id',
            col_idx.variable_mapping_item_variable_id: 'variable_id',
            col_idx.variable_mapping_item_is_source: 'is_source',
            col_idx.variable_mapping_item_valid_from: 'valid_from',
            col_idx.variable_mapping_item_valid_to: 'valid_to'
        }

        # Cube Structure mappings
        self.column_mappings['pybirdai_cube_structure'] = {
            col_idx.cube_structure_maintenance_agency: 'maintenance_agency_id',
            col_idx.cube_structure_id_index: 'cube_structure_id',
            col_idx.cube_structure_name_index: 'name',
            col_idx.cube_structure_code_index: 'code',
            col_idx.cube_structure_description_index: 'description',
            col_idx.cube_structure_valid_from: 'valid_from',
            col_idx.cube_structure_valid_to_index: 'valid_to',
            col_idx.cube_structure_version: 'version'
        }

        # Cube Structure Item mappings
        self.column_mappings['pybirdai_cube_structure_item'] = {
            col_idx.cube_structure_item_cube_structure_id: 'cube_structure_id',
            col_idx.cube_structure_item_variable_index: 'cube_variable_code',
            col_idx.cube_structure_item_variable_id: 'variable_id',
            col_idx.cube_structure_item_role_index: 'role',
            col_idx.cube_structure_item_order: 'order',
            col_idx.cube_structure_item_subdomain_index: 'subdomain_id',
            col_idx.cube_structure_item_variable_set: 'variable_set_id',
            col_idx.cube_structure_item_specific_member: 'member_id',
            col_idx.cube_structure_item_dimension_type: 'dimension_type',
            col_idx.cube_structure_item_attribute_associated_variable: 'attribute_associated_variable',
            col_idx.cube_structure_item_is_flow: 'is_flow',
            col_idx.cube_structure_item_is_mandatory: 'is_mandatory',
            col_idx.cube_structure_item_description: 'description',
            col_idx.cube_structure_item_is_implemented: 'is_implemented',
            col_idx.cube_structure_item_is_identifier: 'is_identifier'
        }

        # Cube mappings
        self.column_mappings['pybirdai_cube'] = {
            col_idx.cube_maintenance_agency_id: 'maintenance_agency_id',
            col_idx.cube_object_id_index: 'cube_id',
            col_idx.cube_class_name_index: 'name',
            col_idx.cube_class_code_index: 'code',
            col_idx.cube_framework_index: 'framework_id',
            col_idx.cube_cube_structure_id_index: 'cube_structure_id',
            col_idx.cube_cube_type_index: 'cube_type',
            col_idx.cube_is_allowed: 'is_allowed',
            col_idx.cube_valid_from: 'valid_from',
            col_idx.cube_valid_to_index: 'valid_to',
            col_idx.cube_version: 'version',
            col_idx.cube_description: 'description',
            col_idx.cube_published: 'published',
            col_idx.cube_dataset_url: 'dataset_url',
            col_idx.cube_filters: 'filters',
            col_idx.cube_di_export: 'di_export'
        }

        # Cube to Combination mappings
        self.column_mappings['pybirdai_cube_to_combination'] = {
            col_idx.cube_to_combination_cube_id: 'cube_id',
            col_idx.cube_to_combination_combination_id: 'combination_id'
        }

        # Cube Link mappings
        self.column_mappings['pybirdai_cube_link'] = {
            col_idx.cube_link_maintenance_agency_id: 'maintenance_agency_id',
            col_idx.cube_link_id: 'cube_link_id',
            col_idx.cube_link_code: 'code',
            col_idx.cube_link_name: 'name',
            col_idx.cube_link_description: 'description',
            col_idx.cube_link_valid_from: 'valid_from',
            col_idx.cube_link_valid_to: 'valid_to',
            col_idx.cube_link_version: 'version',
            col_idx.cube_link_order_relevance: 'order_relevance',
            col_idx.cube_link_primary_cube_id: 'primary_cube_id',
            col_idx.cube_link_foreign_cube_id: 'foreign_cube_id',
            col_idx.cube_link_type: 'link_type',
            col_idx.cube_link_join_identifier: 'join_identifier'
        }

        # Cube Structure Item Link mappings
        self.column_mappings['pybirdai_cube_structure_item_link'] = {
            col_idx.cube_structure_item_link_id: 'cube_structure_item_link_id',
            col_idx.cube_structure_item_link_cube_link_id: 'cube_link_id',
            col_idx.cube_structure_item_link_foreign_cube_variable_code: 'foreign_cube_variable_code',
            col_idx.cube_structure_item_link_primary_cube_variable_code: 'primary_cube_variable_code'
        }

        # Member Link mappings
        self.column_mappings['pybirdai_member_link'] = {
            col_idx.member_link_cube_structure_item_link_id: 'cube_structure_item_link_id',
            col_idx.member_link_primary_member_id: 'primary_member_id',
            col_idx.member_link_foreign_member_id: 'foreign_member_id',
            col_idx.member_link_is_linked: 'is_linked',
            col_idx.member_link_valid_from: 'valid_from',
            col_idx.member_link_valid_to: 'valid_to'
        }

        # Facet Collection mappings
        self.column_mappings['pybirdai_facet_collection'] = {
            col_idx.facet_collection_code: 'code',
            col_idx.facet_collection_facet_id: 'facet_id',
            col_idx.facet_collection_facet_value_type: 'facet_value_type',
            col_idx.facet_collection_maintenance_agency_id: 'maintenance_agency_id',
            col_idx.facet_collection_name: 'name'
        }

        logger.info(f"Built column mappings for {len(self.column_mappings)} model types")

    def _get_import_order(self):
        """Define the order in which tables should be imported to respect foreign key dependencies"""
        return [
            'pybirdai_maintenance_agency',  # No dependencies
            'pybirdai_facet_collection',    # No dependencies
            'pybirdai_domain',              # Depends on maintenance_agency
            'pybirdai_framework',           # Depends on maintenance_agency
            'pybirdai_variable',            # Depends on maintenance_agency, domain
            'pybirdai_member',              # Depends on maintenance_agency, domain
            'pybirdai_subdomain',           # Depends on domain
            'pybirdai_subdomain_enumeration', # Depends on member, subdomain
            'pybirdai_variable_set',        # Depends on maintenance_agency
            'pybirdai_variable_set_enumeration', # Depends on variable_set, variable, subdomain
            'pybirdai_member_hierarchy',    # Depends on maintenance_agency, domain
            'pybirdai_member_hierarchy_node', # Depends on member_hierarchy, member
            'pybirdai_cube_structure',      # Depends on maintenance_agency
            'pybirdai_cube_structure_item', # Depends on cube_structure, variable, subdomain, variable_set, member
            'pybirdai_cube',                # Depends on maintenance_agency, framework, cube_structure
            'pybirdai_combination',         # Depends on maintenance_agency
            'pybirdai_combination_item',    # Depends on combination, variable, subdomain, variable_set, member
            'pybirdai_cube_to_combination', # Depends on cube, combination
            'pybirdai_cube_link',           # Depends on maintenance_agency, cube
            'pybirdai_cube_structure_item_link', # Depends on cube_link, cube_structure_item
            'pybirdai_table',               # Depends on maintenance_agency
            'pybirdai_axis',                # Depends on table
            'pybirdai_axis_ordinate',       # Depends on axis
            'pybirdai_ordinate_item',       # Depends on axis_ordinate, variable, member, member_hierarchy
            'pybirdai_table_cell',          # Depends on table
            'pybirdai_cell_position',       # Depends on table_cell, axis_ordinate
            'pybirdai_member_mapping',      # Depends on maintenance_agency
            'pybirdai_member_mapping_item', # Depends on member_mapping, variable, member
            'pybirdai_variable_mapping',    # Depends on maintenance_agency
            'pybirdai_variable_mapping_item', # Depends on variable_mapping, variable
            'pybirdai_mapping_definition',  # Depends on maintenance_agency, member_mapping, variable_mapping
            'pybirdai_mapping_to_cube',     # Depends on mapping_definition
        ]

    def _get_table_name_from_csv_filename(self, filename):
        """Convert CSV filename back to table name"""
        base_name = filename.replace('.csv', '')
        if base_name.startswith('bird_'):
            table_name = f"pybirdai_{base_name.replace('bird_', '')}"
        elif base_name.startswith('auth_') or base_name.startswith('django_'):
            table_name = base_name
        else:
            table_name = f"pybirdai_{base_name}"

        logger.debug(f"Converted filename '{filename}' to table name '{table_name}'")
        return table_name

    def _parse_csv_content(self, csv_content):
        """Parse CSV content and return headers and rows"""
        logger.debug("Parsing CSV content")
        csv_reader = csv.reader(io.StringIO(csv_content))
        headers = next(csv_reader, [])  # First row is headers (if present)
        rows = list(csv_reader)
        logger.debug(f"Parsed CSV with {len(headers)} headers and {len(rows)} rows")
        return headers, rows

    def _convert_value(self, field, value, defer_foreign_keys=False):
        """Convert CSV string value to appropriate Python type for the field"""


        if not value or value == '' or value == 'None':
            return None

        if isinstance(field, models.CharField):
            return value
        if isinstance(field, models.IntegerField):
            return int(float(value))  # Handle cases where int comes as float string
        elif isinstance(field, models.FloatField):
            return float(value)
        elif isinstance(field, models.DecimalField):
            from decimal import Decimal
            return Decimal(str(value))
        elif isinstance(field, models.BooleanField):
            return value.lower() in ('true', '1', 'yes', 't')
        elif isinstance(field, models.DateField) or isinstance(field, models.DateTimeField):
            if value and value.strip():
                # Try various date formats
                from django.utils.dateparse import parse_date, parse_datetime
                parsed_date = parse_date(value) or parse_datetime(value)
                return parsed_date
            return None
        elif isinstance(field, models.ForeignKey):
            if defer_foreign_keys:
                # Return the raw ID value for later processing
                return value if value and str(value).strip() not in ('', 'None', 'NULL') else None
            else:
                # For foreign keys, we need to return the related object, not just the ID
                if value and str(value).strip() not in ('', 'None', 'NULL'):
                    try:
                        fk_id = value
                        # Get the related model
                        related_model = field.related_model
                        # Try to get the object by its primary key
                        return related_model.objects.get(pk=fk_id)
                    except (related_model.DoesNotExist, ValueError):
                        logger.warning(f"Foreign key object not found for {field.name} with value {value}")
                        return None
                return None
        else:
            return str(value) if value else None

    def _get_model_fields(self, model_class):
        """Get model fields as a dictionary"""
        return {field.name: field for field in model_class._meta.fields}
    
    def _calculate_optimal_batch_size(self, model_class, base_batch_size=250):
        """Calculate optimal batch size based on model field count and database constraints"""
        field_count = len(model_class._meta.fields)
        
        # SQLite has a limit of 999 variables per statement
        # Leave some margin for safety
        max_variables = 900 if connection.vendor == 'sqlite' else 10000
        
        # Calculate max records per batch
        max_records_per_batch = max_variables // max(field_count, 1)
        
        # Use the smaller of base batch size or calculated maximum
        optimal_batch_size = min(base_batch_size, max_records_per_batch)
        
        # Ensure minimum batch size of 10
        optimal_batch_size = max(10, optimal_batch_size)
        
        logger.debug(f"Model {model_class.__name__}: {field_count} fields, "
                    f"optimal batch size: {optimal_batch_size} (max variables: {max_variables})")
        
        return optimal_batch_size

    def _bulk_sqlite_import_with_index(self, csv_content, model_class, table_name):
        """
        High-performance bulk import for large tables using SQLite3 directly.
        Auto-generates sequential indices for models using Django's auto-generated primary keys.
        """
        logger.info(f"Starting bulk SQLite3 import for {table_name}")
        # Validate table name strictly before any SQL execution
        if not self._is_safe_table_name(table_name) or table_name not in self.model_map:
            logger.error(f"Unsafe or unknown table name detected: {table_name}")
            raise Exception(f"Unsafe or unknown table name detected: {table_name}")
        # Parse CSV content
        headers, rows = self._parse_csv_content(csv_content)
        
        if not rows:
            logger.warning(f"No data rows found in CSV for {table_name}")
            return []
        
        # Check if model uses auto-generated primary key
        pk_fields = [field for field in model_class._meta.fields if field.primary_key]
        has_auto_pk = len(pk_fields) == 1 and pk_fields[0].name == 'id'
        
        if not has_auto_pk:
            raise ValueError(f"Bulk import with auto-generated index only supports models with auto-generated 'id' primary key. {model_class.__name__} doesn't qualify.")
        
        # Get database file path
        db_file = Path(connection.settings_dict['NAME']).absolute()
        if not db_file.exists():
            raise FileNotFoundError(f"Database file not found: {db_file}")
        
        # Create temporary CSV file with auto-generated indices
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as temp_file:
            csv_writer = csv.writer(temp_file)
            
            # Write headers with 'id' column first
            # For foreign key fields, we need to use the Django field names (with _id suffix)
            django_headers = ['id']
            if table_name in self.column_mappings:
                column_mapping = self.column_mappings[table_name]
                for i, header in enumerate(headers):
                    if i in column_mapping:
                        field_name = column_mapping[i]
                        # Check if this is a foreign key field
                        model_fields = self._get_model_fields(model_class)
                        if field_name in model_fields and isinstance(model_fields[field_name], models.ForeignKey):
                            django_headers.append(f"{field_name}_id")
                        else:
                            django_headers.append(field_name)
                    else:
                        django_headers.append(header)
            else:
                django_headers.extend(headers)
            
            csv_writer.writerow(django_headers)
            
            # Write rows with auto-generated sequential IDs
            id_generator = itertools.count(1)  # Start from 1
            for row in rows:
                # Generate sequential ID
                row_id = next(id_generator)
                modified_row = [row_id] + list(row)
                csv_writer.writerow(modified_row)
            
            temp_csv_path = temp_file.name
        
        try:
            # Clear existing data first
            with connection.cursor() as cursor:
                if connection.vendor == 'sqlite':
                    cursor.execute("PRAGMA foreign_keys = 0;")
                
                cursor.execute(f"DELETE FROM {table_name};")
                
                if connection.vendor == 'sqlite':
                    cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table_name}';")
                    cursor.execute("PRAGMA foreign_keys = 1;")
            
            logger.info(f"Cleared existing data from {table_name}")
            
            # Prepare SQLite import commands
            sqlite_commands = [
                ".mode csv",
                f".separator ','",
                f".import --skip 1 '{temp_csv_path}' {table_name}"
            ]
            
            sqlite_script = '\n'.join(sqlite_commands)
            
            # Execute SQLite import
            logger.info(f"Executing bulk SQLite import for {table_name}")
            result = subprocess.run(
                ['sqlite3', str(db_file)],
                input=sqlite_script,
                text=True,
                capture_output=True,
                check=False
            )
            
            if result.returncode != 0:
                error_msg = f"SQLite bulk import failed: {result.stderr}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            # Verify import success
            with connection.cursor() as cursor:
                # Validate table name right before executing SQL
                if not self._is_safe_table_name(table_name) or table_name not in self.model_map:
                    logger.error(f"Unsafe or unknown table name detected (count): {table_name}")
                    raise Exception(f"Unsafe or unknown table name detected (count): {table_name}")
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                imported_count = cursor.fetchone()[0]
            
            logger.info(f"Bulk SQLite import completed: {imported_count} records imported to {table_name}")
            
            # Note: FK resolution is no longer needed since we now store values in correct _id columns during bulk import
            logger.info(f"Foreign keys stored directly in correct columns during bulk import")
            
            # Return mock objects list (limited for memory efficiency)  
            imported_objects = list(model_class.objects.all()[:min(100, imported_count)])
            return imported_objects
            
        except Exception as e:
            logger.error(f"Bulk SQLite import failed for {table_name}: {e}")
            raise
        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_csv_path)
            except Exception as cleanup_error:
                logger.warning(f"Failed to clean up temporary file {temp_csv_path}: {cleanup_error}")

    def _is_high_volume_table(self, table_name, row_count):
        """
        Determine if a table should use bulk SQLite import based on volume and table characteristics.
        """
        # Known high-volume tables that benefit from bulk import
        high_volume_tables = {
            'pybirdai_cell_position',
            'pybirdai_table_cell', 
            'pybirdai_axis_ordinate',
            'pybirdai_ordinate_item'
        }
        
        # Use bulk import for known high-volume tables or tables with >50,000 rows
        return table_name in high_volume_tables or row_count > 50000

    def _resolve_foreign_keys_post_bulk_import(self, model_class, table_name, csv_headers):
        """
        Resolve foreign key relationships after bulk SQLite import.
        The bulk import stores string values, we need to convert them to proper FK references.
        """
        logger.info(f"Resolving foreign keys for {table_name}")
        
        model_fields = self._get_model_fields(model_class)
        fk_fields = {name: field for name, field in model_fields.items() if isinstance(field, models.ForeignKey)}
        
        if not fk_fields:
            logger.info(f"No foreign keys to resolve for {table_name}")
            return
        
        # Get column mappings for this table
        if table_name not in self.column_mappings:
            logger.warning(f"No column mappings found for {table_name}, skipping FK resolution")
            return
            
        column_mapping = self.column_mappings[table_name]
        
        # Build mapping of CSV column index to FK field name
        csv_to_fk_mapping = {}
        for col_idx, field_name in column_mapping.items():
            if field_name in fk_fields and col_idx < len(csv_headers):
                csv_to_fk_mapping[col_idx] = field_name
        
        if not csv_to_fk_mapping:
            logger.info(f"No FK mappings found for {table_name}")
            return
        
        logger.info(f"Resolving {len(csv_to_fk_mapping)} foreign key fields: {list(csv_to_fk_mapping.values())}")
        
        # Process FK resolution in batches to avoid memory issues
        batch_size = 1000
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_records = cursor.fetchone()[0]
            
            for offset in range(0, total_records, batch_size):
                # Get batch of records with string FK values
                cursor.execute(f"SELECT id, {', '.join(csv_to_fk_mapping.values())} FROM {table_name} LIMIT {batch_size} OFFSET {offset}")
                records = cursor.fetchall()
                
                if not records:
                    break
                
                # Process each record in the batch
                updates = []
                for record in records:
                    record_id = record[0]
                    fk_updates = {}
                    
                    # Process each FK field
                    for i, (col_idx, field_name) in enumerate(csv_to_fk_mapping.items(), 1):
                        fk_string_value = record[i]
                        
                        if fk_string_value and fk_string_value.strip():
                            # Get related model and try to find the object
                            fk_field = fk_fields[field_name]
                            related_model = fk_field.related_model
                            
                            try:
                                # Try to find the related object by primary key
                                related_obj = related_model.objects.get(pk=fk_string_value.strip())
                                fk_updates[f"{field_name}_id"] = related_obj.pk
                            except related_model.DoesNotExist:
                                logger.warning(f"Foreign key object not found: {related_model.__name__} with pk '{fk_string_value}'")
                                # Create a minimal object if it doesn't exist
                                try:
                                    related_obj = related_model.objects.create(pk=fk_string_value.strip())
                                    fk_updates[f"{field_name}_id"] = related_obj.pk
                                    logger.info(f"Created missing {related_model.__name__} object with pk '{fk_string_value}'")
                                except Exception as create_error:
                                    logger.error(f"Failed to create missing FK object: {create_error}")
                            except Exception as lookup_error:
                                logger.error(f"Error looking up FK object: {lookup_error}")
                    
                    if fk_updates:
                        updates.append((record_id, fk_updates))
                
                # Execute batch updates
                for record_id, fk_updates in updates:
                    if fk_updates:
                        set_clause = ', '.join([f"{field} = ?" for field in fk_updates.keys()])
                        values = list(fk_updates.values()) + [record_id]
                        cursor.execute(f"UPDATE {table_name} SET {set_clause} WHERE id = ?", values)
                
                logger.debug(f"Processed FK resolution for batch {offset}-{offset + len(records)} of {total_records}")
        
        logger.info(f"Completed foreign key resolution for {table_name}")

    def _fallback_csv_import(self, csv_file, table_name, delimiter):
        """Fallback CSV import for databases that don't support native CSV import"""
        logger.info(f"Using fallback CSV import for {table_name}")
        
        # Read CSV and import row by row (existing bulk_create approach)
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f, delimiter=delimiter)
            headers = next(csv_reader)  # Skip header
            
            # Get model class
            if table_name not in self.model_map:
                raise ValueError(f"No model found for table: {table_name}")
            
            model_class = self.model_map[table_name]
            model_fields = self._get_model_fields(model_class)
            
            # Prepare objects for bulk create
            objects_to_create = []
            # Calculate optimal batch size based on model field count
            batch_size = self._calculate_optimal_batch_size(model_class)
            
            for row in csv_reader:
                if not any(row):  # Skip empty rows
                    continue
                
                obj_data = {}
                # Map CSV columns to model fields based on position
                for i, value in enumerate(row):
                    if i < len(headers) and headers[i] in model_fields:
                        field = model_fields[headers[i]]
                        converted_value = self._convert_value(field, value, defer_foreign_keys=True)
                        if converted_value is not None:
                            obj_data[headers[i]] = converted_value
                
                if obj_data:
                    obj = model_class(**obj_data)
                    objects_to_create.append(obj)
                    
                    if len(objects_to_create) >= batch_size:
                        model_class.objects.bulk_create(objects_to_create, batch_size=batch_size)
                        objects_to_create = []
                        logger.debug(f"Bulk created {batch_size} objects for {table_name}")
            
            # Create remaining objects
            if objects_to_create:
                model_class.objects.bulk_create(objects_to_create, batch_size=batch_size)
                logger.debug(f"Bulk created final {len(objects_to_create)} objects for {table_name}")

    def _create_instances_from_csv_copy(self, csv_file_path, model_class):
        """
        Fast CSV import using SQLite3 command line tool consistently for all databases.
        This approach uses the sqlite3 command to dump CSV files into the database.
        """
        table_name = model_class._meta.db_table
        csv_file = Path(csv_file_path).absolute()
        delimiter = ","

        # Check if CSV file exists
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        # Validate CSV file has content and proper headers
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                if not first_line:
                    raise ValueError(f"CSV file {csv_file} is empty")
                
                # Check if file has reasonable number of columns (basic validation)
                headers = first_line.split(delimiter)
                if len(headers) < 1:
                    raise ValueError(f"CSV file {csv_file} has no columns")
                
                logger.info(f"CSV validation passed: {len(headers)} columns found in {csv_file}")
                
        except Exception as e:
            logger.error(f"CSV validation failed for {csv_file}: {e}")
            raise

        logger.info(f"Starting fast CSV import for {table_name} from {csv_file}")

        try:
            # Clear the table first with proper foreign key handling
            with connection.cursor() as cursor:
                # Disable foreign key constraints for SQLite
                if connection.vendor == 'sqlite':
                    cursor.execute("PRAGMA foreign_keys = 0;")
                
                # Delete all records from the table
                cursor.execute(f"DELETE FROM {table_name};")
                
                # For SQLite, also reset the auto-increment counter if it exists
                if connection.vendor == 'sqlite':
                    cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table_name}';")
                    cursor.execute("PRAGMA foreign_keys = 1;")
                
                # Verify table is empty
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                count = cursor.fetchone()[0]
                if count > 0:
                    raise Exception(f"Failed to clear table {table_name}. Still has {count} records.")
                
                logger.info(f"Successfully cleared table {table_name}")

            # Get database file path (assumes SQLite database)
            db_file = Path(connection.settings_dict['NAME']).absolute()
            
            # Validate that we have a SQLite database file
            if not db_file.exists():
                raise FileNotFoundError(f"Database file not found: {db_file}")

            # Create the SQLite commands
            commands = [
                ".mode csv",
                f".separator '{delimiter}'",
                f".import --skip 1 '{csv_file}' {table_name}"
            ]

            # Join commands with newlines
            sqlite_script = '\n'.join(commands)

            # Execute the SQLite import
            logger.info(f"Executing SQLite import: sqlite3 {db_file}")
            result = subprocess.run(
                ['sqlite3', str(db_file)],
                input=sqlite_script,
                text=True,
                capture_output=True,
                check=False  # Don't raise exception immediately, handle errors manually
            )
            
            # Check for errors
            if result.returncode != 0:
                error_msg = f"SQLite import failed with return code {result.returncode}"
                if result.stderr:
                    error_msg += f": {result.stderr}"
                if result.stdout:
                    error_msg += f" (stdout: {result.stdout})"
                raise Exception(error_msg)
            
            if result.stderr and result.stderr.strip():
                logger.warning(f"SQLite import warnings: {result.stderr}")
                
            # Verify import success by checking record count
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                imported_count = cursor.fetchone()[0]
                logger.info(f"SQLite import completed successfully for {table_name}: {imported_count} records imported")
                
                if imported_count == 0:
                    logger.warning(f"No records were imported into {table_name}. Check CSV file format.")
            
            return result

        except Exception as e:
            logger.error(f"Error importing CSV for {table_name}: {str(e)}")
            # Provide more detailed error information
            if hasattr(e, 'stderr') and e.stderr:
                logger.error(f"SQLite stderr: {e.stderr}")
            if hasattr(e, 'stdout') and e.stdout:
                logger.error(f"SQLite stdout: {e.stdout}")
            raise

    def import_csv_file(self, csv_filename, csv_content, use_fast_import=False):
        """Import a single CSV file using column index mappings
        
        Args:
            csv_filename: Name of the CSV file
            csv_content: Content of the CSV file as string
            use_fast_import: If True, use fast SQL-based import method
        """
        if "bird" in csv_filename:
            return []
        logger.info(f"Starting import of CSV file: {csv_filename} (fast_import={use_fast_import})")

        # Write detailed debug info to a separate file
        debug_file = f"debug_import_{csv_filename.replace('.csv', '')}.txt"
        debug_path = os.path.normpath(os.path.join(self.results_dir, debug_file))
        # Verify that the debug_path is within the results_dir (no path traversal allowed)
        results_dir_norm = os.path.normpath(self.results_dir)
        if not debug_path.startswith(results_dir_norm):
            logger.error(f"Attempted debug file write outside results directory: {debug_path}")
            raise Exception("Unsafe debug file path detected!")
        
        table_name = self._get_table_name_from_csv_filename(csv_filename)
        logger.info(f"Mapped CSV file '{csv_filename}' to table '{table_name}'")

        if table_name not in self.model_map:
            logger.warning(f"No model found for table: {table_name}. Skipping file {csv_filename}")
            return []

        if table_name not in self.column_mappings:
            logger.warning(f"No column mapping found for table: {table_name}. Skipping file {csv_filename}")
            return []

        model_class = self.model_map[table_name]
        column_mapping = self.column_mappings[table_name]
        model_fields = self._get_model_fields(model_class)
        
        # Calculate optimal batch size for this model
        optimal_batch_size = self._calculate_optimal_batch_size(model_class)

        logger.info(f"Using model {model_class.__name__} for table {table_name}")
        logger.info(f"Optimal batch size for {model_class.__name__}: {optimal_batch_size}")
        
        # Parse CSV to get row count for high-volume detection
        headers, rows = self._parse_csv_content(csv_content)
        row_count = len(rows)
        
        # Check if this should use bulk SQLite import for high-volume data
        if self._is_high_volume_table(table_name, row_count):
            logger.info(f"High-volume table detected ({row_count} rows). Using bulk SQLite3 import for {table_name}")
            
            # Check if model is compatible with bulk import (has auto-generated PK)
            pk_fields = [field for field in model_fields.values() if field.primary_key]
            has_auto_pk = len(pk_fields) == 1 and pk_fields[0].name == 'id'
            
            if has_auto_pk:
                try:
                    return self._bulk_sqlite_import_with_index(csv_content, model_class, table_name)
                except Exception as bulk_error:
                    logger.warning(f"Bulk SQLite import failed for {table_name}: {bulk_error}")
                    logger.info(f"Falling back to Django ORM import for {table_name}")
                    
                    # Log the specific error type for debugging
                    error_str = str(bulk_error).lower()
                    if "no such table" in error_str:
                        logger.error(f"Database table {table_name} does not exist. Please run migrations first.")
                    elif "database is locked" in error_str:
                        logger.error(f"Database is locked. Ensure no other processes are using the database.")
                    elif "permission denied" in error_str:
                        logger.error(f"Permission denied accessing database file.")
                    
                    # Continue with normal Django ORM import below
            else:
                logger.info(f"Table {table_name} not compatible with bulk import (no auto PK). Using Django ORM.")
        
        # Fast import path: Use SQL-based import for simple cases
        if use_fast_import:
            logger.info(f"Using fast SQL-based import for {csv_filename}")
            
            # Check if this table has complex foreign key relationships that require custom handling
            # For now, we'll use fast import for tables without complex FK dependencies
            has_complex_fks = any(
                isinstance(field, models.ForeignKey) and 
                field.related_model._meta.db_table in self.id_mappings
                for field in model_fields.values()
            )
            
            if not has_complex_fks:
                try:
                    # Write CSV content to a temporary file
                    import tempfile
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as temp_file:
                        temp_file.write(csv_content)
                        temp_csv_path = temp_file.name
                    
                    try:
                        # Use fast SQL import
                        self._create_instances_from_csv_copy(temp_csv_path, model_class)
                        
                        # Get count of imported records
                        imported_count = model_class.objects.count()
                        logger.info(f"Fast import completed: {imported_count} records imported for {table_name}")
                        
                        # Create mock objects list for return value compatibility
                        imported_objects = list(model_class.objects.all()[:min(100, imported_count)])  # Limit to avoid memory issues
                        return imported_objects
                        
                    finally:
                        # Clean up temporary file
                        os.unlink(temp_csv_path)
                        
                except Exception as e:
                    logger.warning(f"Fast import failed for {csv_filename}: {e}. Falling back to standard import.")
                    # Continue with standard import below
            else:
                logger.info(f"Complex foreign keys detected for {table_name}, using standard import method")
        
        # Show CSV information for debugging (already parsed above)
        logger.info(f"CSV headers: {headers}")
        if rows:
            logger.info(f"First CSV row: {rows[0]}")
            logger.info(f"Column mapping: {column_mapping}")
        
        # Show which fields will be populated
        mapped_fields = []
        for col_idx, field_name in column_mapping.items():
            if col_idx < len(headers):
                mapped_fields.append(f"{headers[col_idx]} -> {field_name}")
        logger.info(f"Field mappings: {mapped_fields}")
        
        # Write detailed debug info to file
        try:
            # 'debug_path' is now validated to stay within 'self.results_dir'
            with open(debug_path, 'w') as f:
                f.write(f"=== DEBUG: Import of {csv_filename} ===\n")
                f.write(f"CSV filename: {csv_filename}\n")
                f.write(f"Table name: {table_name}\n")
                f.write(f"Model class: {model_class.__name__}\n")
                f.write(f"CSV headers: {headers}\n")
                if rows:
                    f.write(f"First 3 CSV rows:\n")
                    for i, row in enumerate(rows[:3]):
                        f.write(f"  Row {i+1}: {row}\n")
                f.write(f"Column mapping: {column_mapping}\n")
                f.write(f"Field mappings:\n")
                for mapping in mapped_fields:
                    f.write(f"  {mapping}\n")
                f.write(f"Total rows to import: {len(rows)}\n")
        except Exception as e:
            logger.warning(f"Could not write debug file: {e}")
        
        # Debug: Show available ID mappings
        if self.id_mappings:
            logger.info(f"Available ID mappings: {list(self.id_mappings.keys())}")
            for table, mappings in self.id_mappings.items():
                logger.info(f"  {table}: {len(mappings)} mappings")
                # Show first few mappings as examples
                sample_mappings = list(mappings.items())[:3]
                for old_id, obj in sample_mappings:
                    logger.info(f"    {old_id} -> object id {obj.id}")
        else:
            logger.info("No ID mappings available yet")
        
        # Check if the model has an explicit primary key
        pk_fields = [field for field in model_class._meta.fields if field.primary_key and field.name != 'id']
        has_explicit_pk = len(pk_fields) > 0
        
        # Check if ID column is present in CSV (for models using Django's auto ID)
        has_id_column = headers and headers[0].upper() == 'ID'
        column_offset = 1 if has_id_column else 0
        
        # Debug: Show ID column and PK detection
        logger.info(f"ID column detection: has_id_column={has_id_column}, has_explicit_pk={has_explicit_pk}")
        logger.info(f"Primary key fields: {[f.name for f in pk_fields]}")
        if headers:
            logger.info(f"First header: '{headers[0]}'")
        logger.info(f"Original column_offset: {column_offset}")
        
        # If we have an ID column and the model uses auto ID, we'll need to handle it specially
        should_store_id_mappings = has_id_column and not has_explicit_pk
        id_to_object_map = {} if should_store_id_mappings else None
        
        logger.info(f"Will store ID mappings: {should_store_id_mappings}")
        
        # Adjust column mapping if ID column is present
        adjusted_column_mapping = {}
        logger.info(f"Original column mapping: {column_mapping}")
        for col_idx, field_name in column_mapping.items():
            # For models with auto-generated IDs, the CSV has an ID column at index 0
            # so we need to skip it and map our column indices starting from index 1
            if has_id_column and not has_explicit_pk:
                new_idx = col_idx + 1
                adjusted_column_mapping[new_idx] = field_name
                logger.info(f"Adjusted mapping: {col_idx} -> {new_idx} for field {field_name}")
            else:
                adjusted_column_mapping[col_idx] = field_name
                logger.info(f"No adjustment: {col_idx} for field {field_name}")
        logger.info(f"Final adjusted column mapping: {adjusted_column_mapping}")

        # Clear existing data in the table first with proper foreign key handling
        existing_count = model_class.objects.count()
        logger.info(f"Table {table_name} has {existing_count} existing records before import")
        if existing_count > 0:
            # Use raw SQL for more efficient clearing and proper foreign key handling
            # Validate table_name against allowed tables before using in raw SQL to prevent SQL injection
            allowed_tables = set(self.model_map.keys())  # Model map should use canonical table names
            # SQLite can have table names with/without "pybirdai_" prefix
            if table_name not in allowed_tables:
                raise Exception(f"Blocked potentially unsafe table_name: {table_name}")
            if not self._is_safe_table_name(table_name):
                raise Exception(f"Unsafe table name (violates allowed character rules): {table_name}")
            with connection.cursor() as cursor:
                # Disable foreign key constraints for SQLite during clearing
                if connection.vendor == 'sqlite':
                    cursor.execute("PRAGMA foreign_keys = 0;")
                
                # Delete all records from the table
                cursor.execute(f"DELETE FROM {table_name};")
                
                # For SQLite, also reset the auto-increment counter if it exists
                if connection.vendor == 'sqlite':
                    cursor.execute("DELETE FROM sqlite_sequence WHERE name=?;", [table_name])
                    cursor.execute("PRAGMA foreign_keys = 1;")
                
            logger.info(f"Cleared {existing_count} existing records from {table_name}")
            
            # Verify clearing worked
            remaining_count = model_class.objects.count()
            logger.info(f"After clearing, table {table_name} has {remaining_count} records")
            
            if remaining_count > 0:
                raise Exception(f"Failed to clear table {table_name}. Still has {remaining_count} records after clearing.")

        # First pass: collect all foreign key references
        foreign_key_refs = {}
        for field_name, field in model_fields.items():
            if isinstance(field, models.ForeignKey):
                related_model = field.related_model
                # Check if the related model has an explicit primary key or uses Django's auto id
                related_has_explicit_pk = any(f.primary_key for f in related_model._meta.fields if f.name != 'id')
                related_table = related_model._meta.db_table
                foreign_key_refs[field_name] = {
                    'model': related_model,
                    'table': related_table,
                    'ids': set(),
                    'uses_auto_id': not related_has_explicit_pk
                }
                logger.debug(f"FK {field_name} -> {related_model.__name__} (table: {related_table}, uses_auto_id: {not related_has_explicit_pk})")

        # Scan rows to collect all foreign key IDs
        for row_num, row in enumerate(rows, 1):
            if not any(row):  # Skip empty rows
                continue
            
            for column_index, field_name in adjusted_column_mapping.items():
                if column_index < len(row) and field_name in foreign_key_refs:
                    value = row[column_index].strip() if isinstance(row[column_index], str) else row[column_index]
                    if value and value not in ('', 'None', 'NULL'):
                        try:
                            # For models using auto ID, the value should be an integer
                            if foreign_key_refs[field_name]['uses_auto_id']:
                                value = int(float(value))  # Handle cases where int comes as float string
                            foreign_key_refs[field_name]['ids'].add(value)
                        except (ValueError, TypeError):
                            logger.warning(f"Invalid foreign key value for {field_name}: {value}")

        # Pre-fetch all foreign key objects and handle missing ones
        foreign_key_cache = {}
        
        for field_name, ref_info in foreign_key_refs.items():
            logger.info(f"Processing FK {field_name}: {len(ref_info['ids'])} unique values, uses_auto_id={ref_info['uses_auto_id']}")
            if ref_info['ids']:
                related_model = ref_info['model']
                related_table = ref_info['table']
                
                if ref_info['uses_auto_id']:
                    # For models using auto ID, we rely on the global ID mappings
                    # Don't create missing objects here since we can't set their IDs
                    logger.info(f"Foreign key {field_name} references model {related_model.__name__} "
                              f"which uses auto-generated IDs. Will use ID mappings for resolution.")
                    foreign_key_cache[field_name] = {}
                else:
                    # For models with explicit primary keys, we can look them up normally
                    existing_ids = set(related_model.objects.filter(pk__in=ref_info['ids']).values_list('pk', flat=True))
                    missing_ids = ref_info['ids'] - existing_ids
                    
                    logger.info(f"FK {field_name}: {len(existing_ids)} exist, {len(missing_ids)} missing")
                    if missing_ids and len(missing_ids) <= 10:
                        logger.info(f"Missing IDs: {list(missing_ids)}")
                    
                    # Create missing foreign key objects
                    if missing_ids:
                        missing_objects = [related_model(pk=pk_id) for pk_id in missing_ids]
                        related_model.objects.bulk_create(missing_objects, batch_size=optimal_batch_size, ignore_conflicts=True)
                        logger.info(f"Created {len(missing_objects)} missing {related_model.__name__} objects")
                    
                    # Cache all foreign key objects
                    foreign_key_cache[field_name] = {
                        str(obj.pk): obj for obj in related_model.objects.filter(pk__in=ref_info['ids'])
                    }
                    logger.info(f"Cached {len(foreign_key_cache[field_name])} {related_model.__name__} objects for FK {field_name}")

        # Second pass: prepare objects for bulk creation
        objects_to_create = []
        errors = []
        old_id_to_row_data = {}  # Map old IDs to row data for models using auto ID
        
        for row_num, row in enumerate(rows, 1):
            if not any(row):  # Skip empty rows
                logger.debug(f"Skipping empty row {row_num}")
                continue

            obj_data = {}
            old_id = None
            
            # Extract the old ID if present
            if has_id_column and not has_explicit_pk:
                old_id = row[0].strip() if row[0] else None
                if old_id:
                    try:
                        old_id = int(float(old_id))  # Handle cases where int comes as float string
                    except (ValueError, TypeError):
                        logger.warning(f"Row {row_num}: failed to convert ID '{old_id}' to integer")
                        old_id = None

            # Use column index mapping to extract values
            for column_index, field_name in adjusted_column_mapping.items():
                if column_index < len(row) and field_name in model_fields:
                    value = row[column_index].strip() if isinstance(row[column_index], str) else row[column_index]
                    field = model_fields[field_name]
                    
                    if isinstance(field, models.ForeignKey) and value and value not in ('', 'None', 'NULL'):
                        # Handle foreign keys differently based on whether they use auto ID
                        if field_name in foreign_key_refs and foreign_key_refs[field_name]['uses_auto_id']:
                            # Check if we have a mapping for this foreign key from a previous import
                            related_table = foreign_key_refs[field_name]['table']
                            
                            try:
                                fk_old_id = int(float(value))
                                if (related_table in self.id_mappings and 
                                    fk_old_id in self.id_mappings[related_table]):
                                    # We have a mapping from a previous import
                                    obj_data[field_name] = self.id_mappings[related_table][fk_old_id]
                                    logger.info(f"Resolved foreign key {field_name} using existing mapping: {fk_old_id} -> {self.id_mappings[related_table][fk_old_id].id}")
                                else:
                                    # Store the foreign key reference value for later resolution
                                    obj_data[f'_fk_{field_name}'] = value
                                    # Don't set the actual foreign key yet
                            except (ValueError, TypeError):
                                logger.warning(f"Invalid foreign key value for {field_name}: {value}")
                        else:
                            # Use cached foreign key object for models with explicit PKs
                            value_str = str(value)
                            if field_name in foreign_key_cache and value_str in foreign_key_cache[field_name]:
                                obj_data[field_name] = foreign_key_cache[field_name][value_str]
                                logger.info(f"Row {row_num}: Set FK {field_name} = {value_str} -> {foreign_key_cache[field_name][value_str]}")
                            else:
                                logger.warning(f"Row {row_num}: Foreign key object not found for {field_name} with value '{value_str}'. Available keys: {list(foreign_key_cache.get(field_name, {}).keys())[:5]}")
                    else:
                        # Convert non-foreign key values
                        converted_value = self._convert_value(field, value, defer_foreign_keys=True)
                        if converted_value is not None:
                            obj_data[field_name] = converted_value

            if obj_data:
                try:
                    # Remove deferred foreign key fields from obj_data
                    deferred_fks = {k: v for k, v in obj_data.items() if k.startswith('_fk_')}
                    clean_obj_data = {k: v for k, v in obj_data.items() if not k.startswith('_fk_')}
                    
                    # Create model instance (without saving)
                    obj = model_class(**clean_obj_data)
                    objects_to_create.append(obj)
                    
                    # Store mapping of old ID to object data for later FK resolution
                    if old_id and should_store_id_mappings:
                        old_id_to_row_data[old_id] = {
                            'obj': obj,
                            'deferred_fks': deferred_fks,
                            'row_num': row_num
                        }
                    
                    if row_num % 1000 == 0:  # Log progress every 1000 rows
                        logger.debug(f"Processed {row_num} rows, prepared {len(objects_to_create)} objects")
                except Exception as e:
                    error_info = {
                        'row_num': row_num,
                        'error': str(e),
                        'row_data': row,
                        'obj_data': obj_data
                    }
                    errors.append(error_info)
                    logger.warning(f"Skipping row {row_num} due to error: {e}")
                    continue

        # Validate and deduplicate objects if model has explicit primary key
        if has_explicit_pk and objects_to_create:
            logger.info(f"Validating primary keys for {len(objects_to_create)} objects")
            pk_field_name = next((f.name for f in model_class._meta.fields if f.primary_key), None)
            
            if pk_field_name:
                seen_pks = set()
                unique_objects = []
                duplicate_count = 0
                
                for obj in objects_to_create:
                    pk_value = getattr(obj, pk_field_name, None)
                    if pk_value is not None:
                        if pk_value not in seen_pks:
                            seen_pks.add(pk_value)
                            unique_objects.append(obj)
                        else:
                            duplicate_count += 1
                
                if duplicate_count > 0:
                    logger.warning(f"Found {duplicate_count} duplicate primary keys in {table_name}. "
                                 f"Keeping {len(unique_objects)} unique objects.")
                    objects_to_create = unique_objects

        # Bulk create all objects
        imported_objects = []
        if objects_to_create:
            try:
                logger.info(f"About to bulk create {len(objects_to_create)} objects to {table_name}")
                logger.info(f"should_store_id_mappings: {should_store_id_mappings}")
                
                # Debug: Show first object to be created
                if objects_to_create:
                    first_obj = objects_to_create[0]
                    logger.info(f"First object to create: {first_obj}")
                    logger.info(f"First object fields: {first_obj.__dict__}")
                
                # For models that need ID mappings, don't ignore conflicts so we get proper IDs
                # For models with explicit primary keys, ignore conflicts to handle duplicates
                if should_store_id_mappings:
                    imported_objects = model_class.objects.bulk_create(
                        objects_to_create, 
                        batch_size=optimal_batch_size
                    )
                elif has_explicit_pk:
                    # Models with explicit primary keys may have duplicates, ignore conflicts
                    logger.info(f"Using ignore_conflicts=True for model {model_class.__name__} with explicit primary key")
                    imported_objects = model_class.objects.bulk_create(
                        objects_to_create, 
                        batch_size=optimal_batch_size,
                        ignore_conflicts=True
                    )
                else:
                    imported_objects = model_class.objects.bulk_create(
                        objects_to_create, 
                        batch_size=optimal_batch_size,
                        ignore_conflicts=False  # Changed to False to see actual errors
                    )
                logger.info(f"Successfully bulk created {len(imported_objects)} objects to {table_name}")
                
                # Verify final count in database
                final_count = model_class.objects.count()
                logger.info(f"Table {table_name} now has {final_count} total records in database")
                
                # Debug: Check if count is unexpected
                expected_new_count = len(imported_objects)
                if final_count != expected_new_count:
                    logger.warning(f"UNEXPECTED COUNT: Expected {expected_new_count} but table has {final_count} records!")
                    logger.warning(f"This suggests data from other CSV files may have been imported to this table!")
                
                # Handle ID mappings and deferred foreign keys
                if old_id_to_row_data and id_to_object_map is not None:
                    logger.info(f"Building ID mapping for {len(imported_objects)} objects")
                    
                    # Build mapping of old IDs to new objects
                    # Create reverse lookup for faster mapping: object -> old_id
                    obj_to_old_id = {data['obj']: old_id for old_id, data in old_id_to_row_data.items()}
                    
                    # Initialize global ID mappings for this model if needed
                    if table_name not in self.id_mappings:
                        self.id_mappings[table_name] = {}
                    
                    # Map objects to their old IDs efficiently
                    for i, obj in enumerate(imported_objects):
                        if i < len(objects_to_create):
                            created_obj = objects_to_create[i]
                            if created_obj in obj_to_old_id:
                                old_id = obj_to_old_id[created_obj]
                                id_to_object_map[old_id] = obj
                                self.id_mappings[table_name][old_id] = obj
                                # Only log a few examples to avoid log spam
                                if i < 5 or i % 1000 == 0:
                                    logger.info(f"Stored ID mapping: {table_name}[{old_id}] -> object with new id {obj.id}")
                    
                    # Now update objects with deferred foreign keys
                    objects_to_update = []
                    for old_id, data in old_id_to_row_data.items():
                        if old_id in id_to_object_map:
                            obj = id_to_object_map[old_id]
                            needs_update = False
                            
                            # Resolve deferred foreign keys
                            for fk_field, fk_value in data['deferred_fks'].items():
                                field_name = fk_field[4:]  # Remove '_fk_' prefix
                                try:
                                    fk_old_id = int(float(fk_value))
                                    resolved = False
                                    
                                    # First try local mapping from current import
                                    if fk_old_id in id_to_object_map:
                                        setattr(obj, field_name, id_to_object_map[fk_old_id])
                                        needs_update = True
                                        resolved = True
                                        logger.debug(f"Resolved FK {field_name} for object {old_id} -> {fk_old_id} (local)")
                                    else:
                                        # Try global mappings from previous imports
                                        if field_name in foreign_key_refs:
                                            related_table = foreign_key_refs[field_name]['table']
                                            
                                            if (related_table in self.id_mappings and 
                                                fk_old_id in self.id_mappings[related_table]):
                                                setattr(obj, field_name, self.id_mappings[related_table][fk_old_id])
                                                needs_update = True
                                                resolved = True
                                                logger.info(f"Resolved FK {field_name} for object {old_id} -> {fk_old_id} (global mapping)")
                                    
                                    if not resolved:
                                        logger.warning(f"Could not resolve FK {field_name} with value {fk_value} for object {old_id}")
                                        
                                except (ValueError, TypeError):
                                    logger.warning(f"Invalid FK value {fk_value} for field {field_name}")
                            
                            if needs_update:
                                objects_to_update.append(obj)
                    
                    # Bulk update objects with resolved foreign keys
                    if objects_to_update:
                        # Get field names from last processed object's deferred FKs
                        field_names = []
                        for old_id, data in old_id_to_row_data.items():
                            if data['deferred_fks']:
                                field_names = [fk_field[4:] for fk_field in data['deferred_fks'].keys()]
                                break
                        
                        if field_names:
                            model_class.objects.bulk_update(
                                objects_to_update, 
                                fields=field_names,
                                batch_size=optimal_batch_size
                            )
                            logger.info(f"Updated {len(objects_to_update)} objects with resolved foreign keys")
                
                elif should_store_id_mappings and not old_id_to_row_data:
                    logger.warning(f"Expected to store ID mappings but no old_id_to_row_data found!")
                elif not should_store_id_mappings:
                    logger.info(f"Not storing ID mappings (should_store_id_mappings={should_store_id_mappings})")
                
            except Exception as e:
                error_str = str(e).lower()
                
                # Check if it's a SQLite "too many variables" error
                if "too many sql variables" in error_str or "too many variables" in error_str:
                    logger.warning(f"SQLite variable limit exceeded for {table_name}. Attempting recovery with smaller batch size.")
                    
                    # Try with progressively smaller batch sizes
                    recovery_batch_sizes = [optimal_batch_size // 2, optimal_batch_size // 4, 10]
                    
                    for recovery_batch_size in recovery_batch_sizes:
                        if recovery_batch_size <= 0:
                            continue
                            
                        logger.info(f"Retrying {table_name} with batch size {recovery_batch_size}")
                        try:
                            # Split objects into smaller batches
                            imported_objects = []
                            for i in range(0, len(objects_to_create), recovery_batch_size):
                                batch = objects_to_create[i:i + recovery_batch_size]
                                if should_store_id_mappings:
                                    batch_result = model_class.objects.bulk_create(
                                        batch, 
                                        batch_size=recovery_batch_size
                                    )
                                elif has_explicit_pk:
                                    batch_result = model_class.objects.bulk_create(
                                        batch, 
                                        batch_size=recovery_batch_size,
                                        ignore_conflicts=True
                                    )
                                else:
                                    batch_result = model_class.objects.bulk_create(
                                        batch, 
                                        batch_size=recovery_batch_size,
                                        ignore_conflicts=False
                                    )
                                imported_objects.extend(batch_result)
                                
                            logger.info(f"Recovery successful for {table_name} with batch size {recovery_batch_size}")
                            break
                            
                        except Exception as recovery_error:
                            logger.warning(f"Recovery attempt with batch size {recovery_batch_size} failed: {recovery_error}")
                            continue
                    else:
                        # All recovery attempts failed
                        logger.error(f"All recovery attempts failed for {table_name}. Original error: {e}")
                        raise
                        
                elif "unique constraint failed" in error_str or "duplicate key" in error_str:
                    logger.error(f"UNIQUE constraint violation in {table_name}: {e}")
                    logger.error(f"This suggests duplicate primary keys in the CSV data.")
                    
                    # For explicit primary key models, try again with ignore_conflicts=True
                    if has_explicit_pk and not should_store_id_mappings:
                        logger.info(f"Retrying {table_name} with ignore_conflicts=True to handle duplicates")
                        try:
                            imported_objects = model_class.objects.bulk_create(
                                objects_to_create, 
                                batch_size=optimal_batch_size,
                                ignore_conflicts=True
                            )
                            logger.info(f"Recovery successful for {table_name} by ignoring duplicate conflicts")
                        except Exception as recovery_error:
                            logger.error(f"Recovery attempt failed: {recovery_error}")
                            raise
                    else:
                        raise
                else:
                    logger.error(f"Bulk create failed for {table_name}: {e}")
                    # If bulk create fails, log the errors
                    for error in errors[:10]:  # Show first 10 errors
                        logger.error(f"Row {error['row_num']}: {error['error']}")
                    raise

        # Log any errors encountered
        if errors:
            logger.warning(f"Encountered {len(errors)} errors during import of {table_name}")
            for error in errors[:5]:  # Show first 5 errors
                logger.debug(f"Row {error['row_num']}: {error['error']}")

        return imported_objects

    def import_from_csv_string(self, csv_string, filename="data.csv", use_fast_import=False):
        """Import CSV data from a string"""
        logger.info(f"Importing CSV data from string (filename: {filename}, fast_import={use_fast_import})")
        try:
            imported_objects = self.import_csv_file(filename, csv_string, use_fast_import=use_fast_import)
            result = {
                filename: {
                    'success': True,
                    'imported_count': len(imported_objects),
                    'objects': imported_objects
                }
            }
            logger.info(f"Successfully imported {len(imported_objects)} objects from CSV string")
            # Save results
            self._save_results(result, "csv_string_import")
            return result
        except Exception as e:
            logger.error(f"Failed to import CSV string for {filename}: {e}")
            result = {
                filename: {
                    'success': False,
                    'error': str(e)
                }
            }
            # Save results even on failure
            self._save_results(result, "csv_string_import")
            return result

    def import_from_path(self, path):
        """Import CSV files from either a zip file or a directory"""
        logger.info(f"Starting import from path: {path}")

        if os.path.isfile(path):
            if path.endswith('.zip'):
                logger.info(f"Processing zip file: {path}")
                result = self.import_zip_file(path)
                self._save_results(result, "zip_import")
                return result
            elif path.endswith('.csv'):
                logger.info(f"Processing single CSV file: {path}")
                # Single CSV file
                with open(path, 'r', encoding='utf-8') as f:
                    csv_content = f.read()
                filename = os.path.basename(path)
                imported_objects = self.import_csv_file(filename, csv_content)
                result = {
                    filename: {
                        'success': True,
                        'imported_count': len(imported_objects),
                        'objects': imported_objects
                    }
                }
                self._save_results(result, "single_csv_import")
                return result
            else:
                error_msg = f"Unsupported file type: {path}"
                logger.error(error_msg)
                raise ValueError(error_msg)
        elif os.path.isdir(path):
            logger.info(f"Processing directory: {path}")
            result = self.import_folder(path)
            self._save_results(result, "folder_import")
            return result
        else:
            error_msg = f"Path does not exist: {path}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def import_folder(self, folder_path):
        """Import all CSV files from a folder"""
        logger.info(f"Importing CSV files from folder: {folder_path}")
        results = {}

        # Find all CSV files in the folder
        csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
        logger.info(f"Found {len(csv_files)} CSV files in folder")

        for csv_file_path in csv_files:
            filename = os.path.basename(csv_file_path)
            logger.info(f"Processing file: {filename}")
            try:
                with open(csv_file_path, 'r', encoding='utf-8') as f:
                    csv_content = f.read()
                imported_objects = self.import_csv_file(filename, csv_content)
                results[filename] = {
                    'success': True,
                    'imported_count': len(imported_objects),
                    'objects': imported_objects
                }
                logger.info(f"Successfully processed {filename}: {len(imported_objects)} objects imported")
            except Exception as e:
                logger.error(f"Failed to process {filename}: {e}")
                results[filename] = {
                    'success': False,
                    'error': str(e)
                }

        logger.info(f"Completed folder import. Processed {len(csv_files)} files")
        return results

    def import_zip_file(self, zip_file_path_or_content):
        logger.info("Starting zip file import")
        if isinstance(zip_file_path_or_content, str):
            # It's a file path
            logger.info(f"Processing zip file from path: {zip_file_path_or_content}")
            with zipfile.ZipFile(zip_file_path_or_content, 'r') as zip_file:
                return self._process_zip_contents(zip_file)
        else:
            # It's file content (bytes)
            logger.info("Processing zip file from bytes content")
            with zipfile.ZipFile(io.BytesIO(zip_file_path_or_content), 'r') as zip_file:
                return self._process_zip_contents(zip_file)

    def _process_zip_contents(self, zip_file):
        """Process contents of an opened zip file"""
        logger.debug("Processing zip file contents")
        results = {}
        csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
        logger.info(f"Found {len(csv_files)} CSV files in zip archive")

        for csv_filename in csv_files:
            logger.info(f"Processing CSV file from zip: {csv_filename}")
            try:
                csv_content = zip_file.read(csv_filename).decode('utf-8')
                imported_objects = self.import_csv_file(csv_filename, csv_content)
                results[csv_filename] = {
                    'success': True,
                    'imported_count': len(imported_objects),
                    'objects': imported_objects
                }
                logger.info(f"Successfully processed {csv_filename}: {len(imported_objects)} objects imported")
            except Exception as e:
                logger.error(f"Failed to process {csv_filename} from zip: {e}")
                results[csv_filename] = {
                    'success': False,
                    'error': str(e)
                }

        logger.info(f"Completed zip file processing. Processed {len(csv_files)} CSV files")
        return results

    def import_from_csv_strings(self, csv_strings_list):
        """Import CSV data from a list of CSV strings"""
        logger.info(f"Importing CSV data from {len(csv_strings_list)} CSV strings")
        results = {}

        for filename, csv_string in csv_strings_list.items():
            logger.info(f"Processing CSV string for filename: {filename}")
            try:
                imported_objects = self.import_csv_file(filename, csv_string)
                results[filename] = {
                    'success': True,
                    'imported_count': len(imported_objects),
                    'objects': imported_objects
                }
                logger.info(f"Successfully processed {filename}: {len(imported_objects)} objects imported")
            except Exception as e:
                logger.error(f"Failed to process CSV string for {filename}: {e}")
                results[filename] = {
                    'success': False,
                    'error': str(e)
                }

        logger.info(f"Completed CSV strings import. Processed {len(csv_strings_list)} files")
        # Save results
        self._save_results(results, "csv_strings_import")
        return results

    def import_from_csv_strings_ordered(self, csv_strings_list, use_fast_import=False):
        """Import CSV data from a list of CSV strings in dependency order"""
        logger.info(f"Starting ordered import from {len(csv_strings_list)} CSV strings (fast_import={use_fast_import})")
        
        # Debug: Show all available CSV files
        logger.info(f"Available CSV files: {list(csv_strings_list.keys())}")
        
        # Get the import order
        import_order = self._get_import_order()
        results = {}
        
        # Debug: Show import order
        logger.info(f"Import order: {import_order}")
        
        # Import files in dependency order
        for table_name in import_order:
            # Find CSV file for this table
            csv_filename = None
            
            # Debug: Show matching attempt
            logger.info(f"Looking for CSV file for table: {table_name}")
            for filename in csv_strings_list.keys():
                converted_table_name = self._get_table_name_from_csv_filename(filename)
                logger.info(f"  File '{filename}' converts to table '{converted_table_name}'")
                if converted_table_name == table_name:
                    csv_filename = filename
                    break
            
            if csv_filename and csv_filename in csv_strings_list:
                logger.info(f"Importing {csv_filename} for table {table_name}")
                try:
                    csv_content = csv_strings_list[csv_filename]
                    imported_objects = self.import_csv_file(csv_filename, csv_content, use_fast_import=use_fast_import)
                    results[csv_filename] = {
                        'success': True,
                        'imported_count': len(imported_objects),
                        'objects': imported_objects
                    }
                    logger.info(f"Successfully imported {csv_filename}: {len(imported_objects)} objects")
                except Exception as e:
                    logger.error(f"Failed to import {csv_filename}: {e}")
                    results[csv_filename] = {
                        'success': False,
                        'error': str(e)
                    }
            else:
                logger.info(f"No CSV file found for table {table_name} (expected filename pattern)")
        
        # Import any remaining CSV files that weren't in the ordered list
        for filename, csv_content in csv_strings_list.items():
            if filename not in results:
                logger.info(f"Importing remaining file: {filename}")
                try:
                    imported_objects = self.import_csv_file(filename, csv_content, use_fast_import=use_fast_import)
                    results[filename] = {
                        'success': True,
                        'imported_count': len(imported_objects),
                        'objects': imported_objects
                    }
                    logger.info(f"Successfully imported {filename}: {len(imported_objects)} objects")
                except Exception as e:
                    logger.error(f"Failed to import {filename}: {e}")
                    results[filename] = {
                        'success': False,
                        'error': str(e)
                    }
        
        # Save results
        self._save_results(results, "csv_strings_ordered_import")
        logger.info(f"Completed ordered CSV strings import. Processed {len(results)} files")
        return results


    def _is_safe_table_name(self, table_name):
        """Return True if table_name contains only allowed characters and matches expected pattern."""
        # Only letters, digits, and underscores permitted
        return bool(re.fullmatch(r'[A-Za-z0-9_]+', table_name))

def import_bird_data_from_csv_export(path_or_content, use_fast_import=False):
    """
    Convenience function to import bird data from a CSV export.

    Args:
        path_or_content: Either a file path (string) to a zip file, folder, or CSV file, or file content (bytes) for zip
        use_fast_import: If True, use fast SQL-based import method

    Returns:
        Dictionary with import results for each CSV file
    """
    logger.info("Starting bird data import from CSV export")
    importer = CSVDataImporter()

    # If it's bytes, treat as zip content
    if isinstance(path_or_content, bytes):
        logger.info("Processing as zip file content (bytes)")
        result = importer.import_zip_file(path_or_content)
        importer._save_results(result, "bird_data_import_bytes")
    else:
        logger.info(f"Processing as file path: {path_or_content}")
        result = importer.import_from_path(path_or_content)
        importer._save_results(result, "bird_data_import_path")

    logger.info("Completed bird data import from CSV export")
    return result

    def import_from_path_ordered(self, path, use_fast_import=False):
        """Import CSV files from a path in dependency order"""
        logger.info(f"Starting ordered import from path: {path} (fast_import={use_fast_import})")

        # First, collect all available CSV files
        csv_files_data = {}

        if os.path.isfile(path):
            if path.endswith('.zip'):
                logger.info(f"Processing zip file: {path}")
                with zipfile.ZipFile(path, 'r') as zip_file:
                    csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                    for csv_filename in csv_files:
                        csv_content = zip_file.read(csv_filename).decode('utf-8')
                        csv_files_data[csv_filename] = csv_content
            elif path.endswith('.csv'):
                filename = os.path.basename(path)
                with open(path, 'r', encoding='utf-8') as f:
                    csv_files_data[filename] = f.read()
        elif os.path.isdir(path):
            csv_files = glob.glob(os.path.join(path, "*.csv"))
            for csv_file_path in csv_files:
                filename = os.path.basename(csv_file_path)
                with open(csv_file_path, 'r', encoding='utf-8') as f:
                    csv_files_data[filename] = f.read()

        # Now import in dependency order
        results = {}
        import_order = self._get_import_order()

        for table_name in import_order:
            # Find CSV file for this table
            csv_filename = None
            for filename in csv_files_data.keys():
                if self._get_table_name_from_csv_filename(filename) == table_name:
                    csv_filename = filename
                    break

            if csv_filename and csv_filename in csv_files_data:
                logger.info(f"Importing {csv_filename} for table {table_name}")
                try:
                    imported_objects = self.import_csv_file(csv_filename, csv_files_data[csv_filename], use_fast_import=use_fast_import)
                    results[csv_filename] = {
                        'success': True,
                        'imported_count': len(imported_objects),
                        'objects': imported_objects
                    }
                    logger.info(f"Successfully imported {csv_filename}: {len(imported_objects)} objects")
                except Exception as e:
                    logger.error(f"Failed to import {csv_filename}: {e}")
                    results[csv_filename] = {
                        'success': False,
                        'error': str(e)
                    }
            else:
                logger.debug(f"No CSV file found for table {table_name}")

        # Import any remaining CSV files that weren't in the ordered list
        for filename, csv_content in csv_files_data.items():
            if filename not in results:
                logger.info(f"Importing remaining file: {filename}")
                try:
                    imported_objects = self.import_csv_file(filename, csv_content, use_fast_import=use_fast_import)
                    results[filename] = {
                        'success': True,
                        'imported_count': len(imported_objects),
                        'objects': imported_objects
                    }
                except Exception as e:
                    logger.error(f"Failed to import {filename}: {e}")
                    results[filename] = {
                        'success': False,
                        'error': str(e)
                    }

        self._save_results(results, "ordered_import")
        logger.info(f"Completed ordered import. Processed {len(results)} files")
        return results

def import_bird_data_from_csv_export_ordered(path_or_content, use_fast_import=False):
    """
    Convenience function to import bird data from a CSV export in dependency order.
    This ensures foreign key relationships are respected during import.

    Args:
        path_or_content: Either a file path (string) to a zip file, folder, or CSV file, or file content (bytes) for zip
        use_fast_import: If True, use fast SQL-based import method

    Returns:
        Dictionary with import results for each CSV file
    """
    logger.info("Starting ordered bird data import from CSV export")
    importer = CSVDataImporter()

    # If it's bytes, treat as zip content - we need to save it temporarily for ordered import
    if isinstance(path_or_content, bytes):
        logger.info("Processing as zip file content (bytes) - saving temporarily for ordered import")
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
            temp_file.write(path_or_content)
            temp_file_path = temp_file.name

        try:
            result = importer.import_from_path_ordered(temp_file_path, use_fast_import=use_fast_import)
            importer._save_results(result, "bird_data_import_ordered_bytes")
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)
    else:
        logger.info(f"Processing as file path in ordered mode: {path_or_content}")
        result = importer.import_from_path_ordered(path_or_content, use_fast_import=use_fast_import)
        importer._save_results(result, "bird_data_import_ordered_path")

    logger.info("Completed ordered bird data import from CSV export")
    return result
