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

    # Read the CSV data
    csv_path = os.path.join(csv_dir, f'{entity_name}_discrimitor_combinations_summary.csv')

    csv_data = ""
    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8') as f:
            csv_data = f.read()

    # Escape for JavaScript
    csv_data_json = json.dumps(csv_data)

    context = {
        'entity_name': entity_name,
        'available_entities': available_entities,
        'csv_data': csv_data_json,
    }

    return render(request, 'pybirdai/visualizations/discriminator_tree.html', context)


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
