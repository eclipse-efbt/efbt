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

"""
Views for LDM data visualizations.
"""

import os
import json
import glob as glob_module
from django.shortcuts import render
from django.http import Http404, JsonResponse
from django.conf import settings


def discriminator_tree_view(request, entity_name=None):
    """
    Render the discriminator decision tree visualization.

    This view displays valid combinations of discriminators for LDM entities
    as an interactive tree structure.
    """
    base_dir = settings.BASE_DIR
    csv_dir = os.path.join(base_dir, 'results', 'csv')

    # Get list of available entities from CSV files
    pattern = os.path.join(csv_dir, '*_discrimitor_combinations_summary.csv')
    csv_files = glob_module.glob(pattern)

    available_entities = []
    for f in csv_files:
        filename = os.path.basename(f)
        entity = filename.replace('_discrimitor_combinations_summary.csv', '')
        if entity:
            available_entities.append(entity)

    available_entities.sort()

    # Default to first entity if none specified
    if not entity_name and available_entities:
        entity_name = available_entities[0]
    elif not entity_name:
        entity_name = 'INSTRMNT'  # Fallback default

    # Read the summary CSV data for the tree structure
    summary_path = os.path.join(csv_dir, f'{entity_name}_discrimitor_combinations_summary.csv')
    full_path = os.path.join(csv_dir, f'{entity_name}_discrimitor_combinations_full.csv')

    csv_data = ""
    il_mapping = {}

    # Read summary CSV for tree data
    if os.path.exists(summary_path):
        with open(summary_path, 'r', encoding='utf-8') as f:
            csv_data = f.read()

    # Read full CSV to extract IL column mappings (second row has IL column names)
    # The discriminators themselves don't have IL mappings, but their corresponding
    # TYPE attributes do. We need to find the TYPE attribute at the same path level.
    if os.path.exists(full_path):
        with open(full_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if len(lines) >= 2:
                # Parse header row (LDM columns)
                ldm_headers = _parse_csv_line(lines[0])
                # Parse second row (IL column mappings)
                il_headers = _parse_csv_line(lines[1])

                # First, build a map of path prefix to (attribute, IL column) pairs
                path_to_type_attrs = {}
                for i, ldm_col in enumerate(ldm_headers):
                    if i < len(il_headers):
                        il_col = il_headers[i].strip()
                        if il_col and il_col != 'UNKNOWN':
                            # Get the path prefix (everything before the last part)
                            parts = ldm_col.rsplit('.', 1)
                            if len(parts) == 2:
                                path_prefix = parts[0]
                                attr_name = parts[1]
                                # Look for TYPE or INDCTR attributes
                                if 'TYP' in attr_name or attr_name.endswith('_INDCTR'):
                                    if path_prefix not in path_to_type_attrs:
                                        path_to_type_attrs[path_prefix] = []
                                    path_to_type_attrs[path_prefix].append((attr_name, il_col))

                # Now map discriminators to their corresponding IL columns
                for ldm_col in ldm_headers:
                    if ldm_col.endswith('_delegate') or ldm_col.endswith('_disc'):
                        parts = ldm_col.rsplit('.', 1)
                        if len(parts) == 2:
                            path_prefix = parts[0]
                            disc_name = parts[1]

                            # Try to find matching type attribute at the same path level
                            if path_prefix in path_to_type_attrs:
                                type_attrs = path_to_type_attrs[path_prefix]
                                # Prefer the first TYP attribute, or any available
                                for attr_name, il_col in type_attrs:
                                    il_mapping[ldm_col] = il_col
                                    break

    # Build complete attribute mapping for the attributes panel
    # This maps each entity path to all its attributes and their IL columns
    entity_attributes = {}
    if os.path.exists(full_path):
        with open(full_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if len(lines) >= 2:
                ldm_headers = _parse_csv_line(lines[0])
                il_headers = _parse_csv_line(lines[1])

                for i, ldm_col in enumerate(ldm_headers):
                    if i < len(il_headers):
                        il_col = il_headers[i].strip()
                        # Parse the path to get entity and attribute
                        parts = ldm_col.split('.')
                        if len(parts) >= 2:
                            # The entity path is everything except the last part (attribute)
                            entity_path = '.'.join(parts[:-1])
                            attr_name = parts[-1]

                            # Skip discriminator markers, we want actual attributes
                            if attr_name.endswith('_delegate') or attr_name.endswith('_disc'):
                                continue

                            if entity_path not in entity_attributes:
                                entity_attributes[entity_path] = []

                            entity_attributes[entity_path].append({
                                'ldm_attribute': attr_name,
                                'ldm_full_path': ldm_col,
                                'il_column': il_col if il_col and il_col != 'UNKNOWN' else None,
                            })

    # Escape for JavaScript
    csv_data_json = json.dumps(csv_data)
    il_mapping_json = json.dumps(il_mapping)
    entity_attributes_json = json.dumps(entity_attributes)

    context = {
        'entity_name': entity_name,
        'available_entities': available_entities,
        'csv_data': csv_data_json,
        'il_mapping': il_mapping_json,
        'entity_attributes': entity_attributes_json,
    }

    return render(request, 'pybirdai/visualizations/discriminator_tree.html', context)


def _parse_csv_line(line):
    """Parse a CSV line handling quoted values."""
    result = []
    current = ''
    in_quotes = False

    for char in line:
        if char == '"':
            in_quotes = not in_quotes
        elif char == ',' and not in_quotes:
            result.append(current.strip())
            current = ''
        else:
            current += char

    result.append(current.strip())
    return result


def discriminator_tree_api(request, entity_name):
    """
    API endpoint to get discriminator tree data as JSON.
    """
    base_dir = settings.BASE_DIR
    csv_dir = os.path.join(base_dir, 'results', 'csv')

    # Read summary CSV
    summary_path = os.path.join(csv_dir, f'{entity_name}_discrimitor_combinations_summary.csv')
    full_path = os.path.join(csv_dir, f'{entity_name}_discrimitor_combinations_full.csv')

    result = {
        'entity': entity_name,
        'summary': None,
        'full': None,
    }

    if os.path.exists(summary_path):
        with open(summary_path, 'r', encoding='utf-8') as f:
            result['summary'] = f.read()

    if os.path.exists(full_path):
        with open(full_path, 'r', encoding='utf-8') as f:
            result['full'] = f.read()

    if not result['summary'] and not result['full']:
        raise Http404(f"No discriminator data found for entity: {entity_name}")

    return JsonResponse(result)


def available_entities_api(request):
    """
    API endpoint to get list of available entities with discriminator data.
    """
    base_dir = settings.BASE_DIR
    csv_dir = os.path.join(base_dir, 'results', 'csv')

    pattern = os.path.join(csv_dir, '*_discrimitor_combinations_summary.csv')
    csv_files = glob_module.glob(pattern)

    entities = []
    for f in csv_files:
        filename = os.path.basename(f)
        entity = filename.replace('_discrimitor_combinations_summary.csv', '')
        if entity:
            # Get file stats for additional info
            stat = os.stat(f)
            entities.append({
                'name': entity,
                'file_size': stat.st_size,
                'modified': stat.st_mtime,
            })

    entities.sort(key=lambda x: x['name'])

    return JsonResponse({'entities': entities})
