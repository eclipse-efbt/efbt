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
#    Auto-generated for derived fields UI integration
"""
Views for derivation configuration in the setup workflow.

These views provide API endpoints for:
- Listing available derived fields (both auto-generated and manual)
- Getting current configuration
- Saving user selections
- Triggering the merge process
"""
import os
import ast
import csv
import json
import logging
from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt

from pybirdai.entry_points.generate_derived_fields import (
    run_list_available_rules,
    run_merge_derived_fields,
    export_available_rules_to_config,
)

logger = logging.getLogger(__name__)

# Path to derivation config CSV
DERIVATION_CONFIG_PATH = os.path.join(
    settings.BASE_DIR, 'resources', 'derivation_files', 'derivation_config.csv'
)

# Path to manual derivation file
MANUAL_DERIVATION_FILE = os.path.join(
    settings.BASE_DIR, 'resources', 'derivation_files', 'derived_field_configuration.py'
)


def _extract_manual_derivations() -> list:
    """
    Extract manually defined derivations from derived_field_configuration.py.

    Uses AST parsing to find classes with @lineage decorated properties.

    Returns:
        List of dicts with class_name and field_name for each manual derivation.
    """
    if not os.path.exists(MANUAL_DERIVATION_FILE):
        logger.info(f"Manual derivation file not found: {MANUAL_DERIVATION_FILE}")
        return []

    try:
        with open(MANUAL_DERIVATION_FILE, 'r', encoding='utf-8') as f:
            source = f.read()

        tree = ast.parse(source)
        derivations = []

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                class_name = node.name

                for item in node.body:
                    if isinstance(item, ast.FunctionDef):
                        # Check if this function has a @lineage or @property decorator
                        has_lineage = False
                        for decorator in item.decorator_list:
                            # Check for @lineage decorator
                            if isinstance(decorator, ast.Call):
                                if isinstance(decorator.func, ast.Name) and decorator.func.id == 'lineage':
                                    has_lineage = True
                                    break
                            elif isinstance(decorator, ast.Name):
                                if decorator.id == 'lineage':
                                    has_lineage = True
                                    break

                        if has_lineage:
                            derivations.append({
                                'class_name': class_name,
                                'field_name': item.name,
                            })

        logger.info(f"Found {len(derivations)} manual derivations")
        return derivations

    except Exception as e:
        logger.error(f"Error parsing manual derivation file: {e}")
        return []


def get_available_derivations(request):
    """
    Return all available derived fields from both auto-generated and manual sources.

    Returns JSON:
    {
        "success": true,
        "derivations": [
            {
                "class_name": "INSTRMNT",
                "field_name": "CRRNT_LTV_RT",
                "type": "auto",
                "enabled": false,
                "notes": ""
            },
            {
                "class_name": "INSTRMNT_RL",
                "field_name": "GRSS_CRRYNG_AMNT",
                "type": "manual",
                "enabled": true,
                "notes": ""
            },
            ...
        ],
        "classes": ["INSTRMNT", "INSTRMNT_RL", ...]  # unique class names
    }
    """
    try:
        derivations = []
        class_set = set()

        # 1. FIRST get MANUAL derivations (they take precedence over auto-generated)
        manual_derivations = _extract_manual_derivations()

        # Build set of manual keys to skip duplicates in auto-generated
        manual_keys = set()
        for manual in manual_derivations:
            class_name = manual['class_name']
            field_name = manual['field_name']
            key = f"{class_name}.{field_name}"

            manual_keys.add(key)
            class_set.add(class_name)

            derivations.append({
                'class_name': class_name,
                'field_name': field_name,
                'type': 'manual',
                'enabled': True,  # Manual derivations are always enabled
                'notes': 'Manually implemented',
            })

        # 2. THEN get AUTO-GENERATED derivations, skipping any that exist in manual
        transformation_rules_csv = os.path.join(
            settings.BASE_DIR,
            'resources',
            'technical_export',
            'logical_transformation_rule.csv'
        )

        if os.path.exists(transformation_rules_csv):
            available_rules = run_list_available_rules(transformation_rules_csv)

            # Load current config to get enabled states
            current_config = _load_derivation_config()

            # available_rules is a dict: {class_name: [field1, field2, ...]}
            for class_name, fields in available_rules.items():
                class_set.add(class_name)
                for field_name in fields:
                    config_key = f"{class_name}.{field_name}"

                    # Skip if manual version exists (manual takes precedence)
                    if config_key in manual_keys:
                        continue

                    enabled = current_config.get(config_key, {}).get('enabled', False)
                    notes = current_config.get(config_key, {}).get('notes', '')

                    derivations.append({
                        'class_name': class_name,
                        'field_name': field_name,
                        'type': 'auto',
                        'enabled': enabled,
                        'notes': notes,
                    })
        else:
            logger.info(f"Transformation rules CSV not found: {transformation_rules_csv}")

        # Count by type
        auto_count = sum(1 for d in derivations if d['type'] == 'auto')
        manual_count = sum(1 for d in derivations if d['type'] == 'manual')

        return JsonResponse({
            'success': True,
            'derivations': derivations,
            'classes': sorted(list(class_set)),
            'total_count': len(derivations),
            'auto_count': auto_count,
            'manual_count': manual_count,
        })

    except Exception as e:
        logger.error(f"Error getting available derivations: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e),
        }, status=500)


def get_current_derivation_config(request):
    """
    Return the current derivation configuration from CSV.

    Returns JSON with enabled/disabled status for each field.
    """
    try:
        config = _load_derivation_config()

        # Convert to list format
        config_list = []
        for key, value in config.items():
            parts = key.split('.', 1)
            if len(parts) == 2:
                config_list.append({
                    'class_name': parts[0],
                    'field_name': parts[1],
                    'enabled': value.get('enabled', False),
                    'notes': value.get('notes', ''),
                })

        return JsonResponse({
            'success': True,
            'config': config_list,
            'enabled_count': sum(1 for c in config_list if c['enabled']),
        })

    except Exception as e:
        logger.error(f"Error getting derivation config: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e),
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def save_derivation_config(request):
    """
    Save user selections to the derivation config CSV.

    Expects POST body:
    {
        "selections": [
            {"class_name": "INSTRMNT", "field_name": "CRRNT_LTV_RT", "enabled": true},
            ...
        ]
    }
    """
    try:
        data = json.loads(request.body)
        selections = data.get('selections', [])

        if not selections:
            return JsonResponse({
                'success': False,
                'error': 'No selections provided',
            }, status=400)

        # Load existing config to preserve notes and unspecified fields
        existing_config = _load_derivation_config()

        # Update with new selections
        for selection in selections:
            class_name = selection.get('class_name')
            field_name = selection.get('field_name')
            enabled = selection.get('enabled', False)

            if class_name and field_name:
                key = f"{class_name}.{field_name}"
                if key in existing_config:
                    existing_config[key]['enabled'] = enabled
                else:
                    existing_config[key] = {
                        'enabled': enabled,
                        'notes': selection.get('notes', ''),
                    }

        # Write back to CSV
        _save_derivation_config(existing_config)

        enabled_count = sum(1 for v in existing_config.values() if v.get('enabled', False))

        logger.info(f"Saved derivation config: {enabled_count} fields enabled")

        return JsonResponse({
            'success': True,
            'message': f'Configuration saved. {enabled_count} fields enabled.',
            'enabled_count': enabled_count,
        })

    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'error': 'Invalid JSON in request body',
        }, status=400)
    except Exception as e:
        logger.error(f"Error saving derivation config: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e),
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def merge_derived_fields(request):
    """
    Trigger the merge of enabled derived fields into bird_data_model.py.

    This calls run_merge_derived_fields() which reads the config CSV
    and merges enabled fields into the target model file.
    """
    try:
        # Run the merge
        result = run_merge_derived_fields()

        logger.info("Derived fields merge completed")

        return JsonResponse({
            'success': True,
            'message': 'Derived fields merged successfully',
            'result': result if isinstance(result, dict) else {'status': 'completed'},
        })

    except Exception as e:
        logger.error(f"Error merging derived fields: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e),
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def regenerate_derivation_config(request):
    """
    Regenerate the derivation config CSV from available rules.

    This updates the config with any new fields from transformation rules.
    Existing selections (enabled status and notes) are preserved.
    Only NEW fields are added with enabled=false.
    """
    try:
        # Use absolute path based on BASE_DIR
        transformation_rules_csv = os.path.join(
            settings.BASE_DIR,
            'resources',
            'technical_export',
            'logical_transformation_rule.csv'
        )

        config_csv = os.path.join(
            settings.BASE_DIR,
            'resources',
            'derivation_files',
            'derivation_config.csv'
        )

        # export_available_rules_to_config now preserves existing selections
        export_available_rules_to_config(
            transformation_rules_csv=transformation_rules_csv,
            config_csv=config_csv,
            enabled_by_default=False
        )

        # Reload to get the count
        new_config = _load_derivation_config()
        enabled_count = sum(1 for v in new_config.values() if v.get('enabled', False))

        logger.info(f"Regenerated derivation config: {len(new_config)} total, {enabled_count} enabled")

        return JsonResponse({
            'success': True,
            'message': f'Configuration updated. {len(new_config)} fields total, {enabled_count} enabled.',
            'total_count': len(new_config),
            'enabled_count': enabled_count,
        })

    except Exception as e:
        logger.error(f"Error regenerating derivation config: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e),
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def enable_all_derivations(request):
    """
    Enable all derived fields in the config.
    """
    try:
        config = _load_derivation_config()

        for key in config:
            config[key]['enabled'] = True

        _save_derivation_config(config)

        logger.info(f"Enabled all {len(config)} derivations")

        return JsonResponse({
            'success': True,
            'message': f'Enabled all {len(config)} fields',
            'enabled_count': len(config),
        })

    except Exception as e:
        logger.error(f"Error enabling all derivations: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e),
        }, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def disable_all_derivations(request):
    """
    Disable all derived fields in the config.
    """
    try:
        config = _load_derivation_config()

        for key in config:
            config[key]['enabled'] = False

        _save_derivation_config(config)

        logger.info("Disabled all derivations")

        return JsonResponse({
            'success': True,
            'message': 'Disabled all fields',
            'enabled_count': 0,
        })

    except Exception as e:
        logger.error(f"Error disabling all derivations: {e}")
        return JsonResponse({
            'success': False,
            'error': str(e),
        }, status=500)


def _load_derivation_config() -> dict:
    """
    Load derivation config from CSV file.

    Returns dict with keys as "class_name.field_name" and values as
    {"enabled": bool, "notes": str}
    """
    config = {}

    if not os.path.exists(DERIVATION_CONFIG_PATH):
        return config

    try:
        with open(DERIVATION_CONFIG_PATH, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                class_name = row.get('class_name', '').strip()
                field_name = row.get('field_name', '').strip()
                enabled_str = row.get('enabled', 'False').strip().lower()
                notes = row.get('notes', '').strip()

                if class_name and field_name:
                    key = f"{class_name}.{field_name}"
                    config[key] = {
                        'enabled': enabled_str in ('true', '1', 'yes'),
                        'notes': notes,
                    }
    except Exception as e:
        logger.error(f"Error loading derivation config: {e}")

    return config


def _save_derivation_config(config: dict):
    """
    Save derivation config to CSV file.

    Expects config dict with keys as "class_name.field_name" and values as
    {"enabled": bool, "notes": str}
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(DERIVATION_CONFIG_PATH), exist_ok=True)

    with open(DERIVATION_CONFIG_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['class_name', 'field_name', 'enabled', 'notes'])

        # Sort by class_name, then field_name
        sorted_keys = sorted(config.keys())
        for key in sorted_keys:
            parts = key.split('.', 1)
            if len(parts) == 2:
                class_name, field_name = parts
                value = config[key]
                writer.writerow([
                    class_name,
                    field_name,
                    str(value.get('enabled', False)),
                    value.get('notes', ''),
                ])
