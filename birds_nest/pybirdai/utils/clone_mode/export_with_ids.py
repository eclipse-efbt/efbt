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
"""
Export database to CSV with proper ID field handling for models without explicit primary keys.
This ensures foreign key relationships can be properly restored during import.
"""

import csv
import zipfile
import os
import inspect
from django.db import connection, models
from django.conf import settings
from pybirdai import bird_meta_data_model
import re


def clean_whitespace(text):
    """Clean whitespace from text values"""
    return re.sub(r'\s+', ' ', str(text).replace('\r', '').replace('\n', ' ')) if text else text


def export_database_to_csv_with_ids(output_path=None):
    """
    Export database to CSV files, including ID fields for models that use Django's auto-generated primary key.
    
    Args:
        output_path: Path for the output zip file. If None, uses default location.
    
    Returns:
        Path to the created zip file
    """
    # Default output path
    if output_path is None:
        results_dir = os.path.join(settings.BASE_DIR, 'results')
        os.makedirs(results_dir, exist_ok=True)
        output_path = os.path.join(results_dir, 'database_export_with_ids.zip')
    
    # Get all model classes from bird_meta_data_model
    valid_table_names = set()
    model_map = {}  # Store model classes for reference
    for name, obj in inspect.getmembers(bird_meta_data_model):
        if inspect.isclass(obj) and issubclass(obj, models.Model) and obj != models.Model:
            valid_table_names.add(obj._meta.db_table)
            model_map[obj._meta.db_table] = obj
    
    with zipfile.ZipFile(output_path, 'w') as zip_file:
        # Get all table names from SQLite and sort them
        with connection.cursor() as cursor:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'django_%' ORDER BY name")
            tables = cursor.fetchall()
        
        # Export each table to a CSV file
        for table in tables:
            table_name = table[0]
            
            if table_name in valid_table_names:
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
                    # Get primary key column name - table_name is validated against valid_table_names set
                    if table_name not in valid_table_names:
                        continue  # Skip unsafe table names
                    # SQLite PRAGMA doesn't support parameterized queries, but table name is validated above
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    table_info = cursor.fetchall()
                    pk_columns = []
                    
                    # Collect all primary key columns for composite keys
                    for col in table_info:
                        if col[5] == 1:  # 5 is the index for pk flag in table_info
                            pk_columns.append(col[1])  # 1 is the index for column name
                    
                    # Build ORDER BY clause
                    if pk_columns:
                        order_by = f"ORDER BY {', '.join(pk_columns)}"
                    else:
                        # If no primary key, sort by id if it exists, otherwise by all columns
                        if 'id' in db_headers:
                            order_by = "ORDER BY id"
                        else:
                            order_by = f"ORDER BY {', '.join(escaped_headers)}"
                    
                    # Table name validated above, column names come from model field definitions
                    cursor.execute(f"SELECT {','.join(escaped_headers)} FROM {table_name} {order_by}")
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
                
                # Determine CSV filename
                csv_filename = f"bird_{table_name.replace('pybirdai_', '')}.csv"
                
                # Add CSV content to zip file
                csv_data = '\n'.join(csv_content)
                zip_file.writestr(csv_filename, csv_data)
                
                print(f"Exported {table_name} to {csv_filename} ({len(rows)} rows)")
                if not has_explicit_pk:
                    print(f"  Note: Included ID field for {table_name} (uses Django auto-generated primary key)")
    
    print(f"\nExport complete. Zip file created at: {output_path}")
    return output_path