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
import numpy as np
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
                    # If it's a foreign key, append _id for the actual DB column
                    if isinstance(field, models.ForeignKey):
                        db_headers.append(f"{field.name}_id")
                    else:
                        db_headers.append(field.name)

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
                    query = f"SELECT {',\n    '.join(escaped_headers)} \n FROM {table_name} \n {order_by}"
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

    # Unzip the file in the database_export folder
    extract_dir = os.path.join(results_dir, 'database_export')
    os.makedirs(extract_dir, exist_ok=True)

    with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
        zip_file.extractall(extract_dir)

    return zip_file_path, extract_dir

if __name__ == '__main__':
    DjangoSetup.configure_django()
    _export_database_to_csv_logic()
