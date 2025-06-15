import csv
import zipfile
import io
import os
import glob
import logging
import json
from datetime import datetime
from django.db import transaction
from django.db import models
from pybirdai import bird_meta_data_model
from pybirdai.utils.clone_mode.clone_mode_column_index import ColumnIndexes
import traceback

# Set up logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

class CSVDataImporter:
    def __init__(self, results_dir="import_results"):
        self.model_map = {}
        self.column_mappings = {}
        self.results_dir = results_dir
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
            col_idx.cube_structure_item_variable_index: 'variable_id',
            col_idx.cube_structure_item_variable_id: 'variable_id_2',
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

    def import_csv_file(self, csv_filename, csv_content):
        """Import a single CSV file using column index mappings"""
        if "bird" in csv_filename:
            return []
        logger.info(f"Starting import of CSV file: {csv_filename}")

        table_name = self._get_table_name_from_csv_filename(csv_filename)

        if table_name not in self.model_map:
            error_msg = f"No model found for table: {table_name}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if table_name not in self.column_mappings:
            error_msg = f"No column mapping found for table: {table_name}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        model_class = self.model_map[table_name]
        column_mapping = self.column_mappings[table_name]
        model_fields = self._get_model_fields(model_class)

        logger.info(f"Using model {model_class.__name__} for table {table_name}")

        headers, rows = self._parse_csv_content(csv_content)

        # Clear existing data in the table first
        existing_count = model_class.objects.count()
        if existing_count > 0:
            model_class.objects.all().delete()
            logger.info(f"Cleared {existing_count} existing records from {table_name}")

        # First pass: collect all foreign key references
        foreign_key_refs = {}
        for field_name, field in model_fields.items():
            if isinstance(field, models.ForeignKey):
                foreign_key_refs[field_name] = {
                    'model': field.related_model,
                    'ids': set()
                }

        # Scan rows to collect all foreign key IDs
        for row_num, row in enumerate(rows, 1):
            if not any(row):  # Skip empty rows
                continue
            
            for column_index, field_name in column_mapping.items():
                if column_index < len(row) and field_name in foreign_key_refs:
                    value = row[column_index].strip() if isinstance(row[column_index], str) else row[column_index]
                    if value and value not in ('', 'None', 'NULL'):
                        foreign_key_refs[field_name]['ids'].add(value)

        # Pre-fetch all foreign key objects and create missing ones
        foreign_key_cache = {}
        for field_name, ref_info in foreign_key_refs.items():
            if ref_info['ids']:
                related_model = ref_info['model']
                existing_ids = set(related_model.objects.filter(pk__in=ref_info['ids']).values_list('pk', flat=True))
                missing_ids = ref_info['ids'] - existing_ids
                
                # Create missing foreign key objects
                if missing_ids:
                    missing_objects = [related_model(pk=pk_id) for pk_id in missing_ids]
                    related_model.objects.bulk_create(missing_objects, batch_size=1000, ignore_conflicts=True)
                    logger.info(f"Created {len(missing_objects)} missing {related_model.__name__} objects")
                
                # Cache all foreign key objects
                foreign_key_cache[field_name] = {
                    obj.pk: obj for obj in related_model.objects.filter(pk__in=ref_info['ids'])
                }

        # Second pass: prepare objects for bulk creation
        objects_to_create = []
        errors = []
        
        for row_num, row in enumerate(rows, 1):
            if not any(row):  # Skip empty rows
                logger.debug(f"Skipping empty row {row_num}")
                continue

            obj_data = {}

            # Use column index mapping to extract values
            for column_index, field_name in column_mapping.items():
                if column_index < len(row) and field_name in model_fields:
                    value = row[column_index].strip() if isinstance(row[column_index], str) else row[column_index]
                    field = model_fields[field_name]
                    
                    if isinstance(field, models.ForeignKey) and value and value not in ('', 'None', 'NULL'):
                        # Use cached foreign key object
                        if field_name in foreign_key_cache and value in foreign_key_cache[field_name]:
                            obj_data[field_name] = foreign_key_cache[field_name][value]
                        else:
                            logger.warning(f"Foreign key object not found for {field_name} with value {value}")
                    else:
                        # Convert non-foreign key values
                        converted_value = self._convert_value(field, value, defer_foreign_keys=True)
                        if converted_value is not None:
                            obj_data[field_name] = converted_value

            if obj_data:
                try:
                    # Create model instance (without saving)
                    obj = model_class(**obj_data)
                    objects_to_create.append(obj)
                    
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

        # Bulk create all objects
        imported_objects = []
        if objects_to_create:
            try:
                imported_objects = model_class.objects.bulk_create(
                    objects_to_create, 
                    batch_size=1000,
                    ignore_conflicts=True
                )
                logger.info(f"Successfully bulk created {len(imported_objects)} objects to {table_name}")
            except Exception as e:
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

    def import_from_csv_string(self, csv_string, filename="data.csv"):
        """Import CSV data from a string"""
        logger.info(f"Importing CSV data from string (filename: {filename})")
        try:
            imported_objects = self.import_csv_file(filename, csv_string)
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


def import_bird_data_from_csv_export(path_or_content):
    """
    Convenience function to import bird data from a CSV export.

    Args:
        path_or_content: Either a file path (string) to a zip file, folder, or CSV file, or file content (bytes) for zip

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

    def import_from_path_ordered(self, path):
        """Import CSV files from a path in dependency order"""
        logger.info(f"Starting ordered import from path: {path}")

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
                    imported_objects = self.import_csv_file(csv_filename, csv_files_data[csv_filename])
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
                    imported_objects = self.import_csv_file(filename, csv_content)
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

def import_bird_data_from_csv_export_ordered(path_or_content):
    """
    Convenience function to import bird data from a CSV export in dependency order.
    This ensures foreign key relationships are respected during import.

    Args:
        path_or_content: Either a file path (string) to a zip file, folder, or CSV file, or file content (bytes) for zip

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
            result = importer.import_from_path_ordered(temp_file_path)
            importer._save_results(result, "bird_data_import_ordered_bytes")
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)
    else:
        logger.info(f"Processing as file path in ordered mode: {path_or_content}")
        result = importer.import_from_path_ordered(path_or_content)
        importer._save_results(result, "bird_data_import_ordered_path")

    logger.info("Completed ordered bird data import from CSV export")
    return result
