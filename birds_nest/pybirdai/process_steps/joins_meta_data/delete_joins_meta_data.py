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
from pybirdai.context.sdd_context_django import SDDContext
from pybirdai.models.bird_meta_data_model import *
from pybirdai.models.bird_meta_data_model_extension import (
    MAPPING_ORDINATE_LINK, FRAMEWORK_TABLE, FRAMEWORK_SUBDOMAIN, FRAMEWORK_HIERARCHY
)
from django.apps import apps
from django.db import connection
from django.db.models.fields import CharField,DateTimeField,BooleanField,FloatField,BigIntegerField
import os
import csv
import json
import logging
from pathlib import Path
from typing import List, Any, Optional
from django.db import connection

from pybirdai.process_steps.joins_meta_data.ldm_search import ELDMSearch

logger = logging.getLogger(__name__)

class TransformationMetaDataDestroyer:
    """
    A class for creating generation rules for reports and tables.
    """

    def delete_output_concepts(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Generate generation rules for the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """
        with connection.cursor() as cursor:
            # Helper to check if table exists before delete
            def safe_delete(table_name: str, condition: str = ""):
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    [table_name]
                )
                if cursor.fetchone():
                    if condition:
                        cursor.execute(f"DELETE FROM {table_name} WHERE {condition}")
                    else:
                        cursor.execute(f"DELETE FROM {table_name}")
                else:
                    print(f"Info: Table '{table_name}' does not exist yet, skipping deletion")

            safe_delete("pybirdai_cube_to_combination")
            safe_delete("pybirdai_combination_item")
            safe_delete("pybirdai_combination")
            safe_delete("pybirdai_cube", "cube_structure_id_id like '%structure'")
            safe_delete("pybirdai_cube_structure_item", "cube_structure_id_id like '%structure'")
            safe_delete("pybirdai_cube_structure", "cube_structure_id like '%structure'")
            print("DELETE FROM pybirdai_cube_structure where cube_structure_id like '%structure'")

        # check if we should really delete all of these or just some.

        for key,value in sdd_context.bird_cube_dictionary.items():
            if key.endswith('_cube_structure'):
                del sdd_context.bird_cube_dictionary[key]
        for key,value in sdd_context.bird_cube_structure_item_dictionary.items():
            if key.endswith('_cube_structure'):
                del sdd_context.bird_cube_structure_item_dictionary[key]
        for key,value in sdd_context.bird_cube_structure_dictionary.items():
            if key.endswith('_cube_structure'):
                del sdd_context.bird_cube_structure_dictionary[key]

        sdd_context.combination_item_dictionary = {}
        sdd_context.combination_dictionary = {}
        sdd_context.combination_to_rol_cube_map = {}


    def delete_joins_meta_data(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Generate generation rules for the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """

        model_classes = [CUBE_LINK,
        CUBE_STRUCTURE_ITEM_LINK]

        for model_cls in model_classes:
            self.delete_items_for_sqlite(model_cls)



        sdd_context.cube_link_dictionary = {}
        sdd_context.cube_link_to_foreign_cube_map = {}
        sdd_context.cube_link_to_join_identifier_map = {}
        sdd_context.cube_link_to_join_for_report_id_map = {}
        sdd_context.cube_structure_item_links_dictionary = {}
        sdd_context.cube_structure_item_link_to_cube_link_map = {}


    def delete_semantic_integration_meta_data(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Generate generation rules for the given context and framework.

        Args:
            context (Any): The context object containing necessary data.
            sdd_context (Any): The SDD context object.
            framework (str): The framework being used (e.g., "FINREP_REF").
        """

        model_classes = [MAPPING_TO_CUBE,
        MAPPING_DEFINITION,
        VARIABLE_MAPPING_ITEM,
        VARIABLE_MAPPING,
        MEMBER_MAPPING_ITEM,
        MEMBER_MAPPING]

        for model_cls in model_classes:
            self.delete_items_for_sqlite(model_cls)



        sdd_context.mapping_definition_dictionary = {}
        sdd_context.variable_mapping_dictionary = {}
        sdd_context.variable_mapping_item_dictionary = {}
        sdd_context.member_mapping_dictionary = {}
        sdd_context.member_mapping_items_dictionary = {}
        sdd_context.mapping_to_cube_dictionary = {}

        TransformationMetaDataDestroyer.delete_joins_meta_data(self,context,sdd_context,framework)

    def delete_items_for_sqlite(self,model_clss):
        # Define allowed table names to prevent SQL injection
        ALLOWED_TABLES = {
            'pybirdai_cube_link',
            'pybirdai_cube_structure_item_link',
            'pybirdai_cube_structure_item',
            'pybirdai_cube_structure',
            'pybirdai_cube',
            'pybirdai_domain',
            'pybirdai_variable',
            'pybirdai_member',
            'pybirdai_member_mapping',
            'pybirdai_member_mapping_item',
            'pybirdai_variable_mapping',
            'pybirdai_variable_mapping_item',
            'pybirdai_table_cell',
            'pybirdai_cell_position',
            'pybirdai_axis_ordinate',
            'pybirdai_ordinate_item',
            'pybirdai_mapping_definition',
            'pybirdai_mapping_to_cube',
            'pybirdai_mapping_ordinate_link',
            'pybirdai_table',
            'pybirdai_axis',
            'pybirdai_axis_ordinate',
            'pybirdai_subdomain',
            'pybirdai_subdomain_enumeration',
            'pybirdai_facet_collection',
            'pybirdai_maintenance_agency',
            'pybirdai_framework',
            'pybirdai_framework_table',
            'pybirdai_framework_subdomain',
            'pybirdai_framework_hierarchy',
            'pybirdai_member_hierarchy',
            'pybirdai_member_hierarchy_node',
            'pybirdai_combination',
            'pybirdai_combination_item',
            'pybirdai_cube_to_combination'
        }

        with connection.cursor() as cursor:
            cursor.execute("PRAGMA foreign_keys = 0;")
            for model_cls in model_clss:
                table_name = f"pybirdai_{model_cls.__name__.lower()}"
                if table_name in ALLOWED_TABLES:
                    # Check if table exists before attempting to delete
                    cursor.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name=%s",
                        (table_name,)
                    )
                    if cursor.fetchone():
                        cursor.execute(f"DELETE FROM {table_name};")
                    else:
                        print(f"Info: Table '{table_name}' does not exist yet, skipping deletion")
                else:
                    print(f"Warning: Table '{table_name}' not in allowed list, skipping deletion")
            cursor.execute("PRAGMA foreign_keys = 1;")

    def delete_bird_metadata_database(self, context: Any, sdd_context: Any, framework: str) -> None:
        """
        Delete the Bird Metadata Database.
        """
        model_classes = [
            # Junction tables from bird_meta_data_model_extension (delete first due to FK constraints)
            FRAMEWORK_TABLE,
            FRAMEWORK_SUBDOMAIN,
            FRAMEWORK_HIERARCHY,
            MAPPING_ORDINATE_LINK,
            # Core tables
            CUBE_LINK,
            CUBE_STRUCTURE_ITEM_LINK,
            CUBE_STRUCTURE_ITEM,
            CUBE_STRUCTURE,
            CUBE,
            DOMAIN,
            VARIABLE,
            MEMBER,
            MEMBER_MAPPING,
            MEMBER_MAPPING_ITEM,
            VARIABLE_MAPPING,
            VARIABLE_MAPPING_ITEM,
            TABLE_CELL,
            CELL_POSITION,
            AXIS_ORDINATE,
            ORDINATE_ITEM,
            MAPPING_DEFINITION,
            MAPPING_TO_CUBE,
            TABLE,
            CELL_POSITION,
            AXIS,
            SUBDOMAIN,
            SUBDOMAIN_ENUMERATION,
            FACET_COLLECTION,
            MAINTENANCE_AGENCY,
            FRAMEWORK,
            MEMBER_HIERARCHY,
            MEMBER_HIERARCHY_NODE,
            COMBINATION,
            COMBINATION_ITEM,
            CUBE_TO_COMBINATION
        ]
        self.delete_items_for_sqlite(model_classes)

        sdd_context.mapping_definition_dictionary = {}
        sdd_context.variable_mapping_dictionary = {}
        sdd_context.variable_mapping_item_dictionary = {}
        sdd_context.bird_cube_structure_dictionary = {}
        sdd_context.bird_cube_dictionary = {}
        sdd_context.bird_cube_structure_item_dictionary = {}
        sdd_context.mapping_to_cube_dictionary = {}
        sdd_context.agency_dictionary = {}
        sdd_context.framework_dictionary = {}
        sdd_context.domain_dictionary = {}
        sdd_context.member_dictionary = {}
        sdd_context.member_id_to_domain_map = {}
        sdd_context.member_id_to_member_code_map = {}
        sdd_context.variable_dictionary = {}
        sdd_context.variable_to_domain_map = {}
        sdd_context.variable_to_long_names_map = {}
        sdd_context.variable_to_primary_concept_map = {}
        sdd_context.member_hierarchy_dictionary = {}
        sdd_context.member_hierarchy_node_dictionary = {}
        sdd_context.report_tables_dictionary = {}
        sdd_context.axis_dictionary = {}
        sdd_context.axis_ordinate_dictionary = {}
        sdd_context.axis_ordinate_to_ordinate_items_map = {}
        sdd_context.table_cell_dictionary = {}
        sdd_context.table_to_table_cell_dictionary = {}
        sdd_context.cell_positions_dictionary = {}
        sdd_context.member_mapping_dictionary = {}
        sdd_context.member_mapping_items_dictionary = {}
        sdd_context.combination_item_dictionary = {}
        sdd_context.combination_dictionary = {}
        sdd_context.combination_to_rol_cube_map = {}
        sdd_context.cube_link_dictionary = {}
        sdd_context.cube_link_to_foreign_cube_map = {}
        sdd_context.cube_link_to_join_identifier_map = {}
        sdd_context.cube_link_to_join_for_report_id_map = {}
        sdd_context.cube_structure_item_links_dictionary = {}
        sdd_context.cube_structure_item_link_to_cube_link_map = {}
        sdd_context.subdomain_dictionary = {}
        sdd_context.subdomain_to_domain_map ={}
        sdd_context.subdomain_enumeration_dictionary = {}
        sdd_context.members_that_are_nodes = {}
        sdd_context.member_plus_hierarchy_to_child_literals = {}
        sdd_context.domain_to_hierarchy_dictionary = {}



        SDDContext.mapping_definition_dictionary = {}
        SDDContext.variable_mapping_dictionary = {}
        SDDContext.variable_mapping_item_dictionary = {}
        SDDContext.bird_cube_structure_dictionary = {}
        SDDContext.bird_cube_dictionary = {}
        SDDContext.bird_cube_structure_item_dictionary = {}
        SDDContext.mapping_to_cube_dictionary = {}
        SDDContext.agency_dictionary = {}
        SDDContext.framework_dictionary = {}
        SDDContext.domain_dictionary = {}
        SDDContext.member_dictionary = {}
        SDDContext.member_id_to_domain_map = {}
        SDDContext.member_id_to_member_code_map = {}
        SDDContext.variable_dictionary = {}
        SDDContext.variable_to_domain_map = {}
        SDDContext.variable_to_long_names_map = {}
        SDDContext.variable_to_primary_concept_map = {}
        SDDContext.member_hierarchy_dictionary = {}
        SDDContext.member_hierarchy_node_dictionary = {}
        SDDContext.report_tables_dictionary = {}
        SDDContext.axis_dictionary = {}
        SDDContext.axis_ordinate_dictionary = {}
        SDDContext.axis_ordinate_to_ordinate_items_map = {}
        SDDContext.table_cell_dictionary = {}
        SDDContext.table_to_table_cell_dictionary = {}
        SDDContext.cell_positions_dictionary = {}
        SDDContext.member_mapping_dictionary = {}
        SDDContext.member_mapping_items_dictionary = {}
        SDDContext.combination_item_dictionary = {}
        SDDContext.combination_dictionary = {}
        SDDContext.combination_to_rol_cube_map = {}
        SDDContext.cube_link_dictionary = {}
        SDDContext.cube_link_to_foreign_cube_map = {}
        SDDContext.cube_link_to_join_identifier_map = {}
        SDDContext.cube_link_to_join_for_report_id_map = {}
        SDDContext.cube_structure_item_links_dictionary = {}
        SDDContext.cube_structure_item_link_to_cube_link_map = {}
        SDDContext.subdomain_dictionary = {}
        SDDContext.subdomain_to_domain_map ={}
        SDDContext.subdomain_enumeration_dictionary = {}
        SDDContext.members_that_are_nodes = {}
        SDDContext.member_plus_hierarchy_to_child_literals = {}
        SDDContext.domain_to_hierarchy_dictionary = {}

    # =========================================================================
    # Framework-Specific Deletion with Orphan Cleanup (Config-Driven)
    # =========================================================================

    def _load_orphan_config(self) -> dict:
        """Load the orphan cleanup configuration from JSON file."""
        config_path = Path(__file__).parent.parent.parent / 'config' / 'orphan_cleanup_config.json'
        if not config_path.exists():
            logger.warning(f"Orphan cleanup config not found at {config_path}, using defaults")
            return {}

        with open(config_path, 'r') as f:
            return json.load(f)

    def _table_exists(self, cursor, table_name: str) -> bool:
        """Check if a table exists in the database."""
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=%s",
            [table_name]
        )
        return cursor.fetchone() is not None

    def _safe_delete(self, cursor, table_name: str, condition: str = "", params: list = None):
        """Safely delete from a table with optional condition."""
        if not self._table_exists(cursor, table_name):
            logger.info(f"Table '{table_name}' does not exist, skipping deletion")
            return 0

        if condition:
            sql = f"DELETE FROM {table_name} WHERE {condition}"
            cursor.execute(sql, params or [])
        else:
            cursor.execute(f"DELETE FROM {table_name}")

        return cursor.rowcount

    def delete_framework_with_orphan_cleanup(
        self,
        context: Any,
        sdd_context: Any,
        framework_id: str
    ) -> dict:
        """
        Delete framework-specific output data and clean up orphaned input model records.

        This method preserves the input model (DOMAIN, VARIABLE, MEMBER, etc.) and only
        deletes framework-specific output (CUBE, CUBE_LINK, MAPPING, etc.) plus any
        orphaned records that are no longer referenced.

        Args:
            context: The context object containing necessary data.
            sdd_context: The SDD context object.
            framework_id: The framework ID to delete (e.g., 'FINREP_REF', 'ANCRDT').

        Returns:
            dict: Summary of deleted records by table.
        """
        config = self._load_orphan_config()
        deletion_summary = {}

        logger.info(f"Starting framework-specific deletion for: {framework_id}")

        with connection.cursor() as cursor:
            cursor.execute("PRAGMA foreign_keys = 0;")

            try:
                # Step 1: Delete framework output tables
                deletion_summary['framework_output'] = self._delete_framework_output(
                    cursor, framework_id, config
                )

                # Step 2: Delete junction table entries
                deletion_summary['junction_tables'] = self._delete_junction_tables(
                    cursor, framework_id, config
                )

                # Step 3: Clean up orphaned input model records
                deletion_summary['orphan_cleanup'] = self._cleanup_orphans(
                    cursor, config
                )

            finally:
                cursor.execute("PRAGMA foreign_keys = 1;")

        # Step 4: Clear framework-specific SDDContext entries
        self._clear_framework_sdd_context(sdd_context, framework_id, config)

        logger.info(f"Framework deletion complete for: {framework_id}")
        logger.info(f"Deletion summary: {deletion_summary}")

        return deletion_summary

    def _delete_framework_output(self, cursor, framework_id: str, config: dict) -> dict:
        """Delete framework-specific output tables."""
        summary = {}
        output_config = config.get('framework_output_tables', {})
        tables = output_config.get('tables', [])

        # Build list of cubes for this framework first
        cube_table = 'pybirdai_cube'
        if self._table_exists(cursor, cube_table):
            cursor.execute(
                f"SELECT cube_id FROM {cube_table} WHERE framework_id_id = %s",
                [framework_id]
            )
            framework_cubes = [row[0] for row in cursor.fetchall()]
        else:
            framework_cubes = []

        logger.info(f"Found {len(framework_cubes)} cubes for framework {framework_id}")

        # Delete in reverse dependency order
        for table_config in reversed(tables):
            table_name = f"pybirdai_{table_config['name'].lower()}"
            filter_type = table_config.get('filter_type', 'direct')
            filter_field = table_config.get('filter_field')

            if filter_type == 'direct' and filter_field:
                # Direct framework_id filter
                deleted = self._safe_delete(
                    cursor, table_name,
                    f"{filter_field} = %s",
                    [framework_id]
                )
            elif filter_type == 'via_fk' and framework_cubes:
                # Filter via foreign key to CUBE
                fk_path = table_config.get('fk_path', '')
                if 'cube' in fk_path.lower():
                    placeholders = ','.join(['%s' for _ in framework_cubes])
                    # Extract the FK field name (e.g., foreign_cube_id_id from foreign_cube_id__framework_id_id)
                    fk_field = fk_path.split('__')[0] + '_id'
                    deleted = self._safe_delete(
                        cursor, table_name,
                        f"{fk_field} IN ({placeholders})",
                        framework_cubes
                    )
                else:
                    deleted = 0
            elif filter_type == 'via_cube_reference':
                # Delete CUBE_STRUCTUREs referenced by framework CUBEs
                # Must explicitly delete CUBE_STRUCTURE_ITEMs first (raw SQL doesn't trigger Django CASCADE)
                cube_table = 'pybirdai_cube'
                if self._table_exists(cursor, cube_table):
                    cursor.execute(
                        f"SELECT DISTINCT cube_structure_id_id FROM {cube_table} "
                        f"WHERE framework_id_id = %s AND cube_structure_id_id IS NOT NULL",
                        [framework_id]
                    )
                    cube_structure_ids = [row[0] for row in cursor.fetchall()]
                    if cube_structure_ids:
                        placeholders = ','.join(['%s' for _ in cube_structure_ids])
                        # First delete CUBE_STRUCTURE_ITEMs explicitly
                        deleted_items = self._safe_delete(
                            cursor, 'pybirdai_cube_structure_item',
                            f"cube_structure_id_id IN ({placeholders})",
                            cube_structure_ids
                        )
                        logger.info(f"Deleted {deleted_items} CUBE_STRUCTURE_ITEMs via CUBE_STRUCTURE reference")
                        # Then delete CUBE_STRUCTUREs
                        deleted = self._safe_delete(
                            cursor, table_name,
                            f"cube_structure_id IN ({placeholders})",
                            cube_structure_ids
                        )
                        logger.info(f"Deleted {deleted} CUBE_STRUCTUREs via CUBE reference")
                    else:
                        deleted = 0
                else:
                    deleted = 0
            elif filter_type == 'orphan':
                # Will be handled in orphan cleanup phase
                deleted = 0
            elif filter_type == 'via_parent':
                # Delete via parent table relationship - handled by cascade or orphan cleanup
                deleted = 0
            else:
                deleted = 0

            summary[table_name] = deleted
            if deleted > 0:
                logger.info(f"Deleted {deleted} rows from {table_name}")

        return summary

    def _delete_junction_tables(self, cursor, framework_id: str, config: dict) -> dict:
        """Delete junction table entries for the framework."""
        summary = {}
        output_config = config.get('framework_output_tables', {})
        junction_tables = output_config.get('junction_tables', [])

        for jt_config in junction_tables:
            table_name = f"pybirdai_{jt_config['name'].lower()}"
            filter_field = jt_config.get('filter_field')

            if filter_field:
                deleted = self._safe_delete(
                    cursor, table_name,
                    f"{filter_field} = %s",
                    [framework_id]
                )
                summary[table_name] = deleted
                if deleted > 0:
                    logger.info(f"Deleted {deleted} rows from junction table {table_name}")

        return summary

    def _cleanup_orphans(self, cursor, config: dict) -> dict:
        """Clean up orphaned input model records based on config."""
        summary = {}
        orphan_config = config.get('orphan_cleanup', {})
        cleanup_order = orphan_config.get('cleanup_order', [])
        orphan_conditions = orphan_config.get('orphan_conditions', {})

        for table_name in cleanup_order:
            db_table_name = f"pybirdai_{table_name.lower()}"
            conditions = orphan_conditions.get(table_name, {})
            referenced_by = conditions.get('referenced_by', [])

            if not self._table_exists(cursor, db_table_name):
                continue

            if not referenced_by:
                # No orphan condition defined, skip
                continue

            # Build orphan detection query
            # A record is orphaned if it's not referenced by ANY of the referencing tables
            orphan_conditions_sql = []
            for ref in referenced_by:
                ref_table = f"pybirdai_{ref['table'].lower()}"
                ref_field = ref['field']
                local_field = ref['local_field']

                if self._table_exists(cursor, ref_table):
                    orphan_conditions_sql.append(
                        f"{local_field} NOT IN (SELECT {ref_field} FROM {ref_table} WHERE {ref_field} IS NOT NULL)"
                    )

            if orphan_conditions_sql:
                # Delete where record is not referenced by any table
                where_clause = " AND ".join(orphan_conditions_sql)
                deleted = self._safe_delete(cursor, db_table_name, where_clause)
                summary[db_table_name] = deleted
                if deleted > 0:
                    logger.info(f"Cleaned up {deleted} orphaned rows from {db_table_name}")

        return summary

    def _clear_framework_sdd_context(self, sdd_context: Any, framework_id: str, config: dict) -> None:
        """Clear framework-specific entries from SDDContext dictionaries."""
        context_config = config.get('sdd_context_cleanup', {})
        dictionaries = context_config.get('dictionaries', [])

        # Common framework prefixes to match
        prefixes = [
            framework_id,
            framework_id.upper(),
            framework_id.lower(),
            framework_id.replace('_REF', ''),
        ]

        for dict_name in dictionaries:
            if hasattr(sdd_context, dict_name):
                dict_obj = getattr(sdd_context, dict_name)
                if isinstance(dict_obj, dict):
                    # Find keys that belong to this framework
                    keys_to_remove = []
                    for key in dict_obj.keys():
                        key_str = str(key)
                        for prefix in prefixes:
                            if prefix in key_str:
                                keys_to_remove.append(key)
                                break

                    # Remove the keys
                    for key in keys_to_remove:
                        del dict_obj[key]

                    if keys_to_remove:
                        logger.debug(f"Cleared {len(keys_to_remove)} entries from {dict_name}")

            # Also clear class-level dictionary if it exists
            if hasattr(SDDContext, dict_name):
                class_dict = getattr(SDDContext, dict_name)
                if isinstance(class_dict, dict):
                    keys_to_remove = []
                    for key in class_dict.keys():
                        key_str = str(key)
                        for prefix in prefixes:
                            if prefix in key_str:
                                keys_to_remove.append(key)
                                break

                    for key in keys_to_remove:
                        del class_dict[key]
