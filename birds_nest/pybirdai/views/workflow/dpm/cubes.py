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
# Extracted from workflow_views.py

import logging

from django.http import JsonResponse
from django.conf import settings

logger = logging.getLogger(__name__)

def get_cubes_for_dpm_step3():
    """
    Get all cubes generated from the output layer mapping workflow.
    Filter by the pattern: table_code_REF_version
    """
    from pybirdai.models.bird_meta_data_model import (
        CUBE, CUBE_STRUCTURE_ITEM, CUBE_TO_COMBINATION, MAPPING_TO_CUBE
    )

    # Find all cubes with '_REF_' pattern (from output layer mapping workflow)
    cubes = CUBE.objects.filter(
        cube_id__icontains='_REF_'
    ).select_related('cube_structure_id', 'framework_id')

    # Enrich with metadata
    cube_data = []
    for cube in cubes:
        # Get combination count
        combination_count = CUBE_TO_COMBINATION.objects.filter(
            cube_id=cube
        ).count()

        # Get structure item count
        item_count = 0
        if cube.cube_structure_id:
            item_count = CUBE_STRUCTURE_ITEM.objects.filter(
                cube_structure_id=cube.cube_structure_id
            ).count()

        cube_data.append({
            'cube_id': cube.cube_id,
            'cube_name': cube.name or cube.cube_id,
            'cube_code': cube.code,
            'structure_id': cube.cube_structure_id.cube_structure_id if cube.cube_structure_id else None,
            'structure_name': cube.cube_structure_id.name if cube.cube_structure_id else 'N/A',
            'framework': cube.framework_id.framework_id if cube.framework_id else 'N/A',
            'combination_count': combination_count,
            'item_count': item_count,
        })

    return cube_data


def api_dpm_cubes(request):
    """
    API endpoint to list Output Layer Mapping Workflow cubes only.
    Returns JSON array of cubes created via the Output Layer Mapping Workflow.
    Supports filtering by framework via ?framework= parameter.
    """
    from pybirdai.models.bird_meta_data_model import CUBE, CUBE_STRUCTURE_ITEM
    from django.http import JsonResponse
    from django.db.models import Q, Count

    try:
        # Get framework filter from request
        framework = request.GET.get('framework', '')

        # Get Output Layer Mapping Workflow cubes only
        # Pattern: {table_code}_REF_{framework}_{version} (e.g., C_07_00_a_4_0_REF_COREP_4_0)
        cubes = CUBE.objects.filter(
            cube_structure_id__isnull=False,  # Has a structure (actual output layer cube)
            cube_id__icontains='_REF_'  # Output Layer Mapping Workflow pattern
        ).exclude(
            cube_id='MAPPING_TO_CUBE'  # Exclude metadata cube
        ).select_related('cube_structure_id', 'framework_id')

        # Apply framework filter if provided
        if framework:
            cubes = cubes.filter(framework_id=framework)
        else:
            # Default: exclude ANCRDT cubes if no framework specified (for backward compatibility)
            cubes = cubes.exclude(framework_id__framework_id__icontains='ANCRDT')

        cubes = cubes.order_by('name')

        # Use annotation to avoid N+1 query for item_count
        cube_list = []
        for cube in cubes:
            # Get structure item count
            item_count = 0
            if cube.cube_structure_id:
                item_count = CUBE_STRUCTURE_ITEM.objects.filter(
                    cube_structure_id=cube.cube_structure_id
                ).count()

            cube_list.append({
                'cube_id': cube.cube_id,
                'name': cube.name or cube.cube_id,
                'code': cube.code,
                'structure_id': cube.cube_structure_id.cube_structure_id if cube.cube_structure_id else None,
                'structure_name': cube.cube_structure_id.name if cube.cube_structure_id else None,
                'item_count': item_count,
            })

        return JsonResponse({'cubes': cube_list})

    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error fetching DPM cubes: {e}")
        return JsonResponse({'error': str(e)}, status=500)
