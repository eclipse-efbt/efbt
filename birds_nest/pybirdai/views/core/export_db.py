# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
import os
import shutil
import django
from django.db import models
from django.conf import settings
import sys

import logging
import inspect
import zipfile
import traceback

from pybirdai.services.framework_selection import FrameworkSelectionService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("visualization_service.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class DjangoSetup:
    @staticmethod
    def configure_django():
        """Configure Django settings without starting the application"""
        if not settings.configured:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
            sys.path.insert(0, project_root)
            os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'
            logger.info("Configuring Django with settings module: %s", os.environ['DJANGO_SETTINGS_MODULE'])
            django.setup()
            logger.debug("Django setup complete")

def _export_database_to_csv_logic(framework_ids=None):
    """
    Export database to CSV files, optionally filtered by framework(s).

    Uses Django ORM for efficient querying with framework subgraph filtering.

    Args:
        framework_ids: Optional list of framework IDs to filter data. If None, exports all data.
                      Supports single framework or paired frameworks (base + _REF).

    Returns:
        Tuple of (zip_file_path, extract_dir)
    """
    import re
    from pybirdai.models import bird_meta_data_model
    from django.db import connection

    def clean_whitespace(text):
        return re.sub(r'\s+', ' ', str(text).replace('\r', '').replace('\n', ' ')) if text else text

    def format_csv_value(val):
        """Format a value for CSV output."""
        if val is None:
            return ''
        str_val = str(clean_whitespace(val))
        if ',' in str_val or '"' in str_val:
            return f'"{str_val.replace(chr(34), chr(34)+chr(34))}"'
        return str_val

    def get_field_value(obj, field):
        """Get field value from object, handling ForeignKey fields."""
        value = getattr(obj, field.name, None)
        if value is None:
            return None
        # For ForeignKey fields, get the primary key value
        if field.is_relation and hasattr(value, 'pk'):
            return value.pk
        return value

    def should_include_table_for_any_framework(framework_ids, table_name):
        """Check if table should be included for any of the selected frameworks."""
        for fid in framework_ids:
            if FrameworkSelectionService.should_include_table(fid, table_name):
                return True
        return False

    # Create a zip file path in results directory
    results_dir = os.path.join(settings.BASE_DIR, 'results')
    os.makedirs(results_dir, exist_ok=True)

    # Use framework-specific filename if filtered
    if framework_ids:
        zip_file_path = os.path.join(results_dir, f"database_export_{'_'.join(framework_ids)}.zip")
    else:
        zip_file_path = os.path.join(results_dir, 'database_export.zip')

    # Get all model classes from bird_meta_data_model
    valid_table_names = set()
    model_map = {}  # Store model classes for reference
    for name, obj in inspect.getmembers(bird_meta_data_model):
        if inspect.isclass(obj) and issubclass(obj, models.Model) and obj != models.Model:
            valid_table_names.add(obj._meta.db_table)
            model_map[obj._meta.db_table] = obj

    with zipfile.ZipFile(zip_file_path, 'w') as zip_file:
        # Get all table names from SQLite and sort them
        with connection.cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'django_%' ORDER BY name")
            tables = cursor.fetchall()

        # Export each table to a CSV file
        for table in tables:
            is_meta_data_table = False
            table_name = table[0]

            # Check if table should be included for the selected framework(s)
            if framework_ids:
                if not should_include_table_for_any_framework(framework_ids, table_name):
                    logger.debug(f"Skipping table {table_name} - not in whitelist for frameworks {framework_ids}")
                    continue

            if table_name in valid_table_names:
                is_meta_data_table = True
                # Get the model class for this table
                model_class = model_map[table_name]

                # Check if model has an explicit primary key
                has_explicit_pk = any(field.primary_key for field in model_class._meta.fields if field.name != 'id')

                # Get fields in the order they're defined in the model
                fields = model_class._meta.fields
                headers = []
                field_list = []

                # If model uses Django's auto ID and has no explicit PK, include the ID field
                if not has_explicit_pk:
                    headers.append('ID')
                    # Find the id field
                    id_field = next((f for f in fields if f.name == 'id'), None)
                    if id_field:
                        field_list.append(id_field)

                for field in fields:
                    # Skip the id field if we already added it or if there's an explicit PK
                    if field.name == 'id':
                        continue
                    headers.append(field.name.upper())
                    field_list.append(field)

                # Create CSV in memory
                csv_content = []
                csv_content.append(','.join(headers))

                # Get queryset - use Django ORM with framework filtering
                model_name = table_name.replace('pybirdai_', '').upper()

                if framework_ids:
                    # Try to get filtered queryset using subgraph traversal for each framework
                    # and union the results
                    combined_queryset = None

                    for fid in framework_ids:
                        queryset = FrameworkSelectionService.get_filtered_queryset_for_model(
                            model_name, fid
                        )

                        if queryset is None:
                            # No specific filter - check for framework_id field
                            if hasattr(model_class, 'framework_id'):
                                queryset = model_class.objects.filter(framework_id=fid)
                            else:
                                # No filter available - export empty queryset (safer than exporting all)
                                logger.warning(f"No framework filter available for {model_name} - exporting empty")
                                queryset = model_class.objects.none()

                        # Union querysets
                        if combined_queryset is None:
                            combined_queryset = queryset
                        else:
                            combined_queryset = combined_queryset | queryset

                    # Ensure we have distinct results after union
                    queryset = combined_queryset.distinct() if combined_queryset else model_class.objects.none()
                else:
                    queryset = model_class.objects.all()

                # Order by primary key for consistent output
                pk_name = model_class._meta.pk.name if model_class._meta.pk else 'id'
                queryset = queryset.order_by(pk_name)

                # Use select_related for ForeignKey fields to reduce queries
                fk_fields = [f.name for f in field_list if f.is_relation and not f.many_to_many]
                if fk_fields:
                    queryset = queryset.select_related(*fk_fields)

                # Iterate efficiently with iterator()
                for obj in queryset.iterator():
                    row_values = []
                    for field in field_list:
                        value = get_field_value(obj, field)
                        row_values.append(format_csv_value(value))
                    csv_content.append(','.join(row_values))
            else:
                continue
            #     # Fallback for tables without models : does not work because of Aorta Models
            #     with connection.cursor() as cursor:
            #         # Get column names
            #         cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            #         headers = []
            #         column_names = []
            #         for desc in cursor.description:
            #             # Skip the id column
            #             if desc[0].lower() != 'id':
            #                 headers.append(desc[0].upper())
            #                 column_names.append(desc[0])

            #         # Get data with escaped column names and ordered by all columns for consistency
            #         escaped_headers = [f'"{h.lower()}"' if h.lower() == 'order' else h.lower() for h in column_names]
            #         query = f"SELECT {','.join(escaped_headers)} FROM {table_name} ORDER BY {', '.join(escaped_headers)}"
            #         print(query)
            #         cursor.execute(query)
            #         rows = cursor.fetchall()

            #         # Create CSV in memory
            #         csv_content = []
            #         csv_content.append(','.join(headers))
            #         for row in rows:
            #             # Convert all values to strings and handle None values
            #             csv_row = [str(clean_whitespace(val)) if val is not None else '' for val in row]
            #             # Escape commas and quotes in values
            #             processed_row = []
            #             for val in csv_row:
            #                 if ',' in val or '"' in val:
            #                     escaped_val = val.replace('"', '""').replace("'", '""')
            #                     processed_row.append(f'"{escaped_val}"')
            #                 else:
            #                     processed_row.append(val)
            #             csv_content.append(','.join(processed_row))

            # Add CSV to zip file
            if is_meta_data_table:
                zip_file.writestr(f"{table_name.replace('pybirdai_', '')}.csv", '\n'.join(csv_content))
            else:
                zip_file.writestr(f"{table_name.replace('pybirdai_', 'bird_')}.csv", '\n'.join(csv_content))

    # Unzip the file in the database_export folder
    # Clear the directory first to remove old files from previous exports
    extract_dir = os.path.join(results_dir, 'database_export')
    if os.path.exists(extract_dir):
        shutil.rmtree(extract_dir)
    os.makedirs(extract_dir, exist_ok=True)

    with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
        zip_file.extractall(extract_dir)

    # Copy join configuration files for the selected framework(s)
    if framework_ids:
        joins_config_base = os.path.join(settings.BASE_DIR, 'resources', 'joins_configuration')
        if os.path.exists(joins_config_base):
            export_joins_dir = os.path.join(extract_dir, 'joins_configuration')
            os.makedirs(export_joins_dir, exist_ok=True)

            for fid in framework_ids:
                # Base framework without _REF suffix (e.g., ANCRDT from ANCRDT_REF)
                base_fid = fid[:-4] if fid.endswith('_REF') else fid

                # Check top-level directory for files matching framework
                for filename in os.listdir(joins_config_base):
                    filepath = os.path.join(joins_config_base, filename)
                    if os.path.isfile(filepath) and filename.endswith('.csv'):
                        if fid in filename or base_fid in filename:
                            dst_path = os.path.join(export_joins_dir, filename)
                            shutil.copy2(filepath, dst_path)
                            logger.info(f"Copied join config file: {filename}")

                # Also check framework-specific subdirectory (e.g., ancrdt/)
                framework_subdir = os.path.join(joins_config_base, base_fid.lower())
                if os.path.isdir(framework_subdir):
                    for filename in os.listdir(framework_subdir):
                        if filename.endswith('.csv'):
                            src_path = os.path.join(framework_subdir, filename)
                            dst_path = os.path.join(export_joins_dir, filename)
                            shutil.copy2(src_path, dst_path)
                            logger.info(f"Copied join config file from {base_fid.lower()}/: {filename}")

    # Copy filter code files for the selected framework(s)
    if framework_ids:
        filter_code_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'process_steps', 'filter_code')
        if os.path.exists(filter_code_dir):
            export_filter_dir = os.path.join(extract_dir, 'filter_code')
            os.makedirs(export_filter_dir, exist_ok=True)

            # Always include shared supporting files
            SHARED_FILTER_FILES = ['report_cells.py', 'automatic_tracking_wrapper.py']

            for filename in os.listdir(filter_code_dir):
                if filename.endswith('.py') and filename != '__init__.py':
                    should_copy = False

                    # Always include shared files
                    if filename in SHARED_FILTER_FILES:
                        should_copy = True
                    else:
                        # Check if file matches any selected framework
                        for fid in framework_ids:
                            base_fid = fid[:-4] if fid.endswith('_REF') else fid
                            if base_fid in filename or fid in filename:
                                should_copy = True
                                break

                    if should_copy:
                        src_path = os.path.join(filter_code_dir, filename)
                        dst_path = os.path.join(export_filter_dir, filename)
                        shutil.copy2(src_path, dst_path)
                        logger.info(f"Copied filter code file: {filename}")

    return zip_file_path, extract_dir

if __name__ == '__main__':
    DjangoSetup.configure_django()
    _export_database_to_csv_logic()
