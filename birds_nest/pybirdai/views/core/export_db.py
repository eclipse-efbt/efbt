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
import django
from django.db import models
from django.conf import settings
import sys
import json
import ast
from datetime import datetime

import logging
import inspect
import zipfile
import traceback

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

def _export_database_to_csv_logic():
    import re
    from pybirdai.models import bird_meta_data_model
    from pybirdai.models import bird_data_model
    from django.db import transaction, connection
    from django.http import HttpResponse, JsonResponse, HttpResponseBadRequest
    def clean_whitespace(text):
        return re.sub(r'\s+', ' ', str(text).replace('\r', '').replace('\n', ' ')) if text else text
    # Create a zip file path in results directory
    results_dir = os.path.join(settings.BASE_DIR, 'results')
    os.makedirs(results_dir, exist_ok=True)
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

            if table_name in valid_table_names:
                is_meta_data_table = True
                # Get the model class for this table
                model_class = model_map[table_name]

                # Check if model has an explicit primary key
                has_explicit_pk = any(field.primary_key for field in model_class._meta.fields if field.name != 'id')

                # Get fields in the order they're defined in the model
                fields = model_class._meta.fields
                headers = []
                db_headers = []

                # If model uses Django's auto ID and has no explicit PK, include the ID field
                if not has_explicit_pk:
                    headers.append('ID')
                    db_headers.append('id')

                for field in fields:
                    # Skip the id field if we already added it or if there's an explicit PK
                    if field.name == 'id' and has_explicit_pk:
                        continue
                    elif field.name == 'id' and not has_explicit_pk:
                        # We already added it above
                        continue
                    headers.append(field.name.upper())  # Convert header to uppercase
                    # Use field.column to get the actual database column name
                    # This handles ForeignKey fields and custom db_column settings
                    db_headers.append(field.column)

                # Create CSV in memory
                csv_content = []
                csv_content.append(','.join(headers))

                # Get data with escaped column names and ordered by primary key
                with connection.cursor() as cursor:
                    escaped_headers = [f'"{h}"' if h == 'order' else h for h in db_headers]
                    # Get primary key column name - validate table name against our whitelist
                    if table_name not in valid_table_names:
                        continue
                    # Use parameterized query for table info - note: SQLite PRAGMA doesn't support parameters
                    # but we validate table_name against valid_table_names whitelist above
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    table_info = cursor.fetchall()
                    pk_columns = []

                    # Collect all primary key columns for composite keys
                    for col in table_info:
                        if col[5] == 1:  # 5 is the index for pk flag in table_info
                            pk_columns.append(col[1])  # 1 is the index for column name

                    # Build ORDER BY clause - handle composite keys and sort by all columns for consistency
                    if pk_columns:
                        order_by = f"ORDER BY {', '.join(pk_columns)}"
                    else:
                        # If no primary key, sort by id if it exists, otherwise by all columns
                        if 'id' in db_headers:
                            order_by = "ORDER BY id"
                        else:
                            order_by = f"ORDER BY {', '.join(escaped_headers)}"

                    # Build the query - table_name is already validated against whitelist
                    # Column names are derived from model fields, not user input
                    escaped_headers_join = ',\n    '.join(escaped_headers)
                    query = f"SELECT {escaped_headers_join} \n FROM {table_name} \n {order_by}"
                    cursor.execute(query)
                    rows = cursor.fetchall()

                    for row in rows:
                        # Convert all values to strings and handle None values
                        csv_row = [str(clean_whitespace(val)) if val is not None else '' for val in row]
                        # Escape commas and quotes in values
                        processed_row = []
                        for val in csv_row:
                            if ',' in val or '"' in val:
                                escaped_val = val.replace('"', '""')
                                processed_row.append(f'"{escaped_val}"')
                            else:
                                processed_row.append(val)
                        csv_content.append(','.join(processed_row))
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

    return zip_file_path, results_dir


def _validate_python_syntax(filepath):
    """Validate that a Python file has valid syntax.

    Returns True if valid, False otherwise.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        ast.parse(content)
        return True
    except SyntaxError as e:
        logger.warning(f"Syntax error in {filepath}: {e}")
        return False
    except Exception as e:
        logger.warning(f"Error reading {filepath}: {e}")
        return False


def _export_filter_code_files(zip_file, base_dir):
    """Export filter code from production and staging directories.

    Returns a dict with counts of exported files.
    """
    counts = {'production': 0, 'staging': 0}

    # Production: pybirdai/process_steps/filter_code/
    production_dir = os.path.join(base_dir, 'pybirdai', 'process_steps', 'filter_code')
    if os.path.exists(production_dir):
        for filename in os.listdir(production_dir):
            if filename.endswith('.py') and not filename.startswith('__'):
                filepath = os.path.join(production_dir, filename)
                if os.path.isfile(filepath):
                    zip_file.write(filepath, f'filter_code/production/{filename}')
                    counts['production'] += 1

    # Staging: results/generated_python_joins/
    staging_dir = os.path.join(base_dir, 'results', 'generated_python_joins')
    if os.path.exists(staging_dir):
        for filename in os.listdir(staging_dir):
            if filename.endswith('.py') and not filename.startswith('__'):
                filepath = os.path.join(staging_dir, filename)
                if os.path.isfile(filepath):
                    zip_file.write(filepath, f'filter_code/staging/{filename}')
                    counts['staging'] += 1

    logger.info(f"Exported filter code: {counts['production']} production, {counts['staging']} staging")
    return counts


def _export_derivation_files(zip_file, base_dir):
    """Export derivation production files and config.

    Returns a dict with counts of exported files.
    """
    counts = {'files': 0, 'config': False}
    derivation_base = os.path.join(base_dir, 'resources', 'derivation_files')

    # manually_generated/ (production)
    manual_dir = os.path.join(derivation_base, 'manually_generated')
    if os.path.exists(manual_dir):
        for filename in os.listdir(manual_dir):
            if filename.endswith('.py') and not filename.startswith('__'):
                filepath = os.path.join(manual_dir, filename)
                if os.path.isfile(filepath):
                    zip_file.write(filepath, f'derivation_files/manually_generated/{filename}')
                    counts['files'] += 1

    # derivation_config.csv
    config_path = os.path.join(derivation_base, 'derivation_config.csv')
    if os.path.exists(config_path):
        zip_file.write(config_path, 'derivation_files/derivation_config.csv')
        counts['config'] = True

    logger.info(f"Exported derivation files: {counts['files']} files, config={counts['config']}")
    return counts


def _export_joins_configuration(zip_file, base_dir):
    """Export joins configuration CSV files.

    Returns a dict with count of exported files.
    """
    counts = {'files': 0}
    joins_config_dir = os.path.join(base_dir, 'artefacts', 'joins_configuration')

    if os.path.exists(joins_config_dir):
        for filename in os.listdir(joins_config_dir):
            if filename.endswith('.csv'):
                filepath = os.path.join(joins_config_dir, filename)
                if os.path.isfile(filepath):
                    zip_file.write(filepath, f'joins_configuration/{filename}')
                    counts['files'] += 1

    logger.info(f"Exported joins configuration: {counts['files']} files")
    return counts


def _create_manifest(zip_file, counts):
    """Create manifest.json with export metadata."""
    manifest = {
        'version': '2.0',
        'format': 'enhanced',
        'exported_at': datetime.now().isoformat(),
        'counts': counts
    }
    zip_file.writestr('manifest.json', json.dumps(manifest, indent=2))
    logger.info(f"Created manifest.json with export metadata")


def _export_database_to_csv_enhanced():
    """Enhanced export that includes database, filter code, derivation files, and joins configuration.

    Export structure (local artefacts/ and GitHub artefacts/):
    artefacts/
    ├── smcubes_artefacts/                 # Database CSVs
    │   ├── cube.csv
    │   ├── variable.csv
    │   └── ...
    ├── filter_code/
    │   ├── production/                    # From pybirdai/process_steps/filter_code/
    │   │   └── F_*.py, report_cells.py, etc.
    │   └── staging/                       # From results/generated_python_joins/
    │       └── F_*.py (pending edits)
    ├── derivation_files/
    │   ├── manually_generated/            # Production derivation code
    │   │   └── *.py
    │   └── derivation_config.csv          # Enabled rules config
    ├── joins_configuration/               # Joins config CSVs
    │   └── *.csv
    └── manifest.json                      # Metadata about export
    """
    import re
    from pybirdai.models import bird_meta_data_model
    from django.db import connection

    def clean_whitespace(text):
        return re.sub(r'\s+', ' ', str(text).replace('\r', '').replace('\n', ' ')) if text else text

    # Create a zip file path in artefacts directory
    artefacts_dir = os.path.join(settings.BASE_DIR, 'artefacts')
    os.makedirs(artefacts_dir, exist_ok=True)
    zip_file_path = os.path.join(artefacts_dir, 'artefacts_export.zip')

    # Track export counts
    export_counts = {
        'database_tables': 0,
        'database_records': 0,
        'filter_code': {},
        'derivation_files': {},
        'joins_configuration': {}
    }

    # Get all model classes from bird_meta_data_model
    valid_table_names = set()
    model_map = {}
    for name, obj in inspect.getmembers(bird_meta_data_model):
        if inspect.isclass(obj) and issubclass(obj, models.Model) and obj != models.Model:
            valid_table_names.add(obj._meta.db_table)
            model_map[obj._meta.db_table] = obj

    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # Get all table names from SQLite and sort them
        with connection.cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'django_%' ORDER BY name")
            tables = cursor.fetchall()

        # Export each table to a CSV file in database/ subdirectory
        for table in tables:
            table_name = table[0]

            if table_name in valid_table_names:
                model_class = model_map[table_name]

                # Check if model has an explicit primary key
                has_explicit_pk = any(field.primary_key for field in model_class._meta.fields if field.name != 'id')

                # Get fields in the order they're defined in the model
                fields = model_class._meta.fields
                headers = []
                db_headers = []

                if not has_explicit_pk:
                    headers.append('ID')
                    db_headers.append('id')

                for field in fields:
                    if field.name == 'id' and has_explicit_pk:
                        continue
                    elif field.name == 'id' and not has_explicit_pk:
                        continue
                    headers.append(field.name.upper())
                    db_headers.append(field.column)

                # Create CSV in memory
                csv_content = []
                csv_content.append(','.join(headers))

                with connection.cursor() as cursor:
                    escaped_headers = [f'"{h}"' if h == 'order' else h for h in db_headers]

                    if table_name not in valid_table_names:
                        continue

                    cursor.execute(f"PRAGMA table_info({table_name})")
                    table_info = cursor.fetchall()
                    pk_columns = []

                    for col in table_info:
                        if col[5] == 1:
                            pk_columns.append(col[1])

                    if pk_columns:
                        order_by = f"ORDER BY {', '.join(pk_columns)}"
                    else:
                        if 'id' in db_headers:
                            order_by = "ORDER BY id"
                        else:
                            order_by = f"ORDER BY {', '.join(escaped_headers)}"

                    escaped_headers_join = ',\n    '.join(escaped_headers)
                    query = f"SELECT {escaped_headers_join} \n FROM {table_name} \n {order_by}"
                    cursor.execute(query)
                    rows = cursor.fetchall()

                    for row in rows:
                        csv_row = [str(clean_whitespace(val)) if val is not None else '' for val in row]
                        processed_row = []
                        for val in csv_row:
                            if ',' in val or '"' in val:
                                escaped_val = val.replace('"', '""')
                                processed_row.append(f'"{escaped_val}"')
                            else:
                                processed_row.append(val)
                        csv_content.append(','.join(processed_row))

                # Write to smcubes_artefacts/ subdirectory
                csv_filename = f"{table_name.replace('pybirdai_', '')}.csv"
                zip_file.writestr(f"smcubes_artefacts/{csv_filename}", '\n'.join(csv_content))
                export_counts['database_tables'] += 1
                export_counts['database_records'] += len(rows)

        logger.info(f"Exported {export_counts['database_tables']} database tables")

        # Export filter code files
        export_counts['filter_code'] = _export_filter_code_files(zip_file, settings.BASE_DIR)

        # Export derivation files
        export_counts['derivation_files'] = _export_derivation_files(zip_file, settings.BASE_DIR)

        # Export joins configuration
        export_counts['joins_configuration'] = _export_joins_configuration(zip_file, settings.BASE_DIR)

        # Create manifest
        _create_manifest(zip_file, export_counts)

    # Extract the zip to the artefacts directory for local use
    # Clean up old export files before extracting new ones
    import shutil
    for subdir in ['smcubes_artefacts', 'filter_code', 'derivation_files', 'joins_configuration']:
        subdir_path = os.path.join(artefacts_dir, subdir)
        if os.path.exists(subdir_path):
            shutil.rmtree(subdir_path)

    # Remove old manifest if exists
    manifest_path = os.path.join(artefacts_dir, 'manifest.json')
    if os.path.exists(manifest_path):
        os.remove(manifest_path)

    logger.info(f"Cleaned up old artefacts directory: {artefacts_dir}")

    with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
        zip_file.extractall(artefacts_dir)

    return zip_file_path, artefacts_dir


if __name__ == '__main__':
    DjangoSetup.configure_django()
    _export_database_to_csv_enhanced()
