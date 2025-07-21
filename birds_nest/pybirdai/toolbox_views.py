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
from django.http import HttpResponse, JsonResponse, HttpResponseBadRequest
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from django.core.paginator import Paginator
from django.db import transaction, connection
from django.conf import settings
from django.views.decorators.http import require_http_methods
from django.core import serializers
from .bird_meta_data_model import (
    VARIABLE_MAPPING, MAINTENANCE_AGENCY,
    COMBINATION, COMBINATION_ITEM, CUBE, FRAMEWORK, CUBE_TO_COMBINATION
)
import json
from . import bird_meta_data_model
import os
import csv
from pathlib import Path
from .process_steps.upload_files.file_uploader import FileUploader
from .entry_points.delete_bird_metadata_database import RunDeleteBirdMetadataDatabase
from .entry_points.upload_joins_configuration import UploadJoinsConfiguration
from django.template.loader import render_to_string
from django.db.models import Count, F
from django.views.generic import ListView
from django.urls import reverse
from .context.sdd_context_django import SDDContext
from urllib.parse import unquote
import logging
import zipfile
from .context.csv_column_index_context import ColumnIndexes
from django.apps import apps
from django.db import models
import time
from datetime import datetime
from django.views.decorators.clickjacking import xframe_options_exempt
import traceback
from .entry_points.automode_database_setup import RunAutomodeDatabaseSetup
from typing import Dict, List, Set, Tuple, Any, Optional
from .entry_points.mapping_assistant_entry import RunMappingAssistant


def mapping_assistant_view(request):
    """Main view for the Mapping Assistant tool with mode selection."""
    if request.method == 'GET':
        # Get data for all modes
        maintenance_agency = MAINTENANCE_AGENCY.objects.get(pk="EBA")
        combinations = COMBINATION.objects.filter(maintenance_agency_id=maintenance_agency).order_by('combination_id')
        frameworks = FRAMEWORK.objects.filter(maintenance_agency_id=maintenance_agency).order_by('framework_id')
        cubes = CUBE.objects.filter(maintenance_agency_id=maintenance_agency).order_by('cube_id')
        variable_mappings = VARIABLE_MAPPING.objects.all().order_by('variable_mapping_id')

        context = {
            'combinations': combinations,
            'frameworks': frameworks,
            'cubes': cubes,
            'variable_mappings': variable_mappings,
            'page_title': 'Mapping Assistant'
        }
        return render(request, 'mapping_assistant.html', context)

    elif request.method == 'POST':
        # Handle form submission based on mode
        mode = request.POST.get('mode', 'combination')
        selected_variable_mappings = request.POST.getlist('variable_mappings')
        confidence_threshold = float(request.POST.get('confidence_threshold', 0.7))

        try:
            if mode == 'combination':
                selected_combinations = request.POST.getlist('combinations')
                if not selected_combinations:
                    messages.error(request, 'Please select at least one combination.')
                    return redirect('mapping_assistant')

                # Generate proposals for selected combinations
                proposals = RunMappingAssistant.generate_mapping_proposals(
                    combination_ids=selected_combinations,
                    variable_mapping_ids=selected_variable_mappings if selected_variable_mappings else None,
                    confidence_threshold=confidence_threshold
                )
                mode_description = f"{len(selected_combinations)} combinations"

            elif mode == 'template':
                selected_templates = request.POST.getlist('templates')
                if not selected_templates:
                    messages.error(request, 'Please select at least one template.')
                    return redirect('mapping_assistant')

                # Generate proposals for selected templates
                all_proposals = {'proposals': {}, 'summary': {'total_combinations': 0, 'combinations_with_proposals': 0, 'total_proposals': 0}}

                for template_id in selected_templates:
                    template_proposals = RunMappingAssistant.generate_proposals_for_template(
                        cube_id=template_id,
                        variable_mapping_ids=selected_variable_mappings if selected_variable_mappings else None,
                        confidence_threshold=confidence_threshold
                    )

                    # Merge proposals
                    all_proposals['proposals'].update(template_proposals['proposals'])
                    all_proposals['summary']['total_combinations'] += template_proposals['summary']['total_combinations']
                    all_proposals['summary']['combinations_with_proposals'] += template_proposals['summary']['combinations_with_proposals']
                    all_proposals['summary']['total_proposals'] += template_proposals['summary']['total_proposals']

                proposals = all_proposals
                mode_description = f"{len(selected_templates)} templates"

            elif mode == 'framework':
                selected_frameworks = request.POST.getlist('frameworks')
                if not selected_frameworks:
                    messages.error(request, 'Please select at least one framework.')
                    return redirect('mapping_assistant')

                ensure_consistency = request.POST.get('ensure_consistency', 'on') == 'on'

                # Generate proposals for selected frameworks
                all_proposals = {'proposals': {}, 'summary': {'total_combinations': 0, 'combinations_with_proposals': 0, 'total_proposals': 0}}

                for framework_id in selected_frameworks:
                    framework_proposals = RunMappingAssistant.generate_proposals_for_framework(
                        framework_id=framework_id,
                        variable_mapping_ids=selected_variable_mappings if selected_variable_mappings else None,
                        confidence_threshold=confidence_threshold,
                        ensure_consistency=ensure_consistency
                    )

                    # Merge proposals
                    all_proposals['proposals'].update(framework_proposals['proposals'])
                    all_proposals['summary']['total_combinations'] += framework_proposals['summary']['total_combinations']
                    all_proposals['summary']['combinations_with_proposals'] += framework_proposals['summary']['combinations_with_proposals']
                    all_proposals['summary']['total_proposals'] += framework_proposals['summary']['total_proposals']

                proposals = all_proposals
                mode_description = f"{len(selected_frameworks)} frameworks"

            else:
                messages.error(request, 'Invalid mode selected.')
                return redirect('mapping_assistant')

            # Store proposals in session for review
            request.session['mapping_proposals'] = proposals

            messages.success(
                request,
                f"Generated {proposals['summary']['total_proposals']} proposals for {proposals['summary']['combinations_with_proposals']} combinations across {mode_description}."
            )

            return redirect('mapping_assistant_review')

        except Exception as e:
            messages.error(request, f'Error generating proposals: {str(e)}')
            return redirect('mapping_assistant')


def mapping_assistant_review(request):
    """View for reviewing and accepting mapping proposals."""
    if request.method == 'GET':
        # Get proposals from session
        proposals = request.session.get('mapping_proposals')
        if not proposals:
            messages.error(request, 'No proposals found. Please generate proposals first.')
            return redirect('mapping_assistant')

        context = {
            'proposals': proposals,
            'page_title': 'Review Mapping Proposals'
        }
        return render(request, 'mapping_assistant_review.html', context)

    elif request.method == 'POST':
        # Handle proposal acceptance
        accepted_proposals = json.loads(request.POST.get('accepted_proposals', '[]'))

        if not accepted_proposals:
            messages.warning(request, 'No proposals selected.')
            return redirect('mapping_assistant')

        try:
            result = RunMappingAssistant.accept_proposals(accepted_proposals)

            if result['success']:
                messages.success(
                    request,
                    f"Successfully created {len(result['created_mappings'])} mapping items."
                )
            else:
                for error in result['errors']:
                    messages.error(request, error)

            # Clear proposals from session
            request.session.pop('mapping_proposals', None)

            return redirect('mapping_assistant')

        except Exception as e:
            messages.error(request, f'Error accepting proposals: {str(e)}')
            return redirect('mapping_assistant_review')


@require_http_methods(["POST"])
def generate_mapping_proposal_api(request):
    """API endpoint for generating mapping proposals."""
    try:
        data = json.loads(request.body)
        combination_ids = data.get('combination_ids', [])
        variable_mapping_ids = data.get('variable_mapping_ids', [])
        confidence_threshold = data.get('confidence_threshold', 0.7)

        if not combination_ids:
            return JsonResponse({
                'success': False,
                'error': 'No combination IDs provided'
            }, status=400)

        # Generate proposals
        proposals = RunMappingAssistant.generate_mapping_proposals(
            combination_ids=combination_ids,
            variable_mapping_ids=variable_mapping_ids if variable_mapping_ids else None,
            confidence_threshold=confidence_threshold
        )

        return JsonResponse({
            'success': True,
            'proposals': proposals
        })

    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def get_templates_for_framework_api(request, framework_id):
    """API endpoint to get templates/cubes for a specific framework."""
    try:
        templates = CUBE.objects.filter(
            framework_id__framework_id=framework_id
        ).order_by('cube_id')

        template_data = []
        for template in templates:
            combo_count = CUBE_TO_COMBINATION.objects.filter(cube_id=template).count()
            template_data.append({
                'cube_id': template.cube_id,
                'name': template.name or template.cube_id,
                'combination_count': combo_count
            })

        return JsonResponse({
            'success': True,
            'templates': template_data
        })

    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["GET"])
def get_framework_summary_api(request, framework_id):
    """API endpoint to get framework summary information."""
    try:
        summary = RunMappingAssistant.get_framework_summary(framework_id)

        if 'error' in summary:
            return JsonResponse({
                'success': False,
                'error': summary['error']
            }, status=404)

        return JsonResponse({
            'success': True,
            'summary': summary
        })

    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def accept_mapping_proposals_api(request):
    """API endpoint for accepting mapping proposals."""
    try:
        data = json.loads(request.body)
        accepted_proposals = data.get('accepted_proposals', [])

        if not accepted_proposals:
            return JsonResponse({
                'success': False,
                'error': 'No proposals to accept'
            }, status=400)

        result = RunMappingAssistant.accept_proposals(accepted_proposals)

        return JsonResponse(result)

    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON data'
        }, status=400)
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)
