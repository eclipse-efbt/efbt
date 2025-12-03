# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
"""
CSV import/export and viewing operations.
"""
import os
import csv
import json
import logging
from pathlib import Path
from django.conf import settings
from django.http import HttpResponse, HttpResponseBadRequest, JsonResponse
from django.shortcuts import render

from pybirdai.models.bird_meta_data_model import (
    MEMBER, VARIABLE, DOMAIN
)
from pybirdai.process_steps.website_to_sddmodel.constants import BULK_CREATE_BATCH_SIZE_DEFAULT
from pybirdai.context.sdd_context_django import SDDContext

logger = logging.getLogger(__name__)


def list_lineage_files(request):
    """List lineage CSV files."""
    lineage_dir = Path(settings.BASE_DIR) / 'results' / 'lineage_output'
    csv_files = list(lineage_dir.glob('*.csv'))
    file_names = [f.name for f in csv_files]
    return render(request, 'pybirdai/list_lineage_files.html', {'files': file_names})


def view_csv_file(request, filename):
    """Secure CSV viewer with path traversal protection."""
    # Prevent directory traversal attacks
    if '..' in filename or '/' in filename or '\\' in filename:
        return HttpResponseBadRequest('Invalid filename')

    # Build safe path
    lineage_dir = Path(settings.BASE_DIR) / 'results' / 'lineage_output'
    file_path = lineage_dir / filename

    # Verify the file path is within lineage_dir (additional security check)
    try:
        file_path = file_path.resolve()
        lineage_dir = lineage_dir.resolve()
        if not str(file_path).startswith(str(lineage_dir)):
            return HttpResponseBadRequest('Access denied')
    except (OSError, ValueError):
        return HttpResponseBadRequest('Invalid path')

    if not file_path.exists():
        return HttpResponseBadRequest('File not found')

    # Read CSV file with proper encoding and error handling
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            if rows:
                fieldnames = list(rows[0].keys())
            else:
                fieldnames = []
    except csv.Error as e:
        return HttpResponseBadRequest(f'Error reading CSV: {e}')
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        return SecureErrorHandler.secure_http_response(e, "CSV file reading", request)

    return render(request, 'pybirdai/view_csv_file.html', {
        'filename': filename,
        'fieldnames': fieldnames,
        'rows': rows
    })


def view_ldm_to_sdd_results(request):
    """View for displaying the LDM to SDD hierarchy conversion results."""
    results_dir = os.path.join(settings.BASE_DIR, 'results', 'ldm_to_sdd_hierarchies')

    # Read the CSV files
    csv_data = {}
    for filename in ['member_hierarchy.csv', 'member_hierarchy_node.csv', 'missing_members.csv']:
        filepath = os.path.join(results_dir, filename)
        if os.path.exists(filepath):
            with open(filepath, 'r', newline='') as f:
                reader = csv.reader(f)
                headers = next(reader)  # Get headers
                rows = list(reader)     # Get data rows
                csv_data[filename] = {'headers': headers, 'rows': rows}

    return render(request, 'pybirdai/view_ldm_to_sdd_results.html', {'csv_data': csv_data})


def import_members_from_csv(request):
    """Import members from CSV file."""
    if request.method == 'GET':
        return render(request, 'pybirdai/import_members.html')
    elif request.method == 'POST':
        try:
            csv_file = request.FILES.get('csvFile')
            if not csv_file:
                return HttpResponseBadRequest('No file was uploaded')

            if not csv_file.name.endswith('.csv'):
                return HttpResponseBadRequest('File must be a CSV')

            # Read the CSV file
            decoded_file = csv_file.read().decode('utf-8').splitlines()
            reader = csv.DictReader(decoded_file)

            # Validate headers
            required_fields = {'MEMBER_ID', 'CODE', 'NAME', 'DESCRIPTION', 'DOMAIN_ID'}
            headers = set(reader.fieldnames)
            if not required_fields.issubset(headers):
                missing = required_fields - headers
                return HttpResponseBadRequest(f'Missing required columns: {", ".join(missing)}')

            # Process each row
            members_to_create = []
            for row in reader:
                try:
                    # Look up the domain
                    domain = DOMAIN.objects.get(domain_id=row['DOMAIN_ID'])

                    member = MEMBER(
                        member_id=row['MEMBER_ID'],
                        code=row['CODE'],
                        name=row['NAME'],
                        description=row['DESCRIPTION'],
                        domain_id=domain
                    )
                    members_to_create.append(member)
                except DOMAIN.DoesNotExist:
                    return HttpResponseBadRequest(f'Domain with ID {row["DOMAIN_ID"]} not found')

            # Bulk create the members
            if members_to_create:
                MEMBER.objects.bulk_create(members_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT)

            return JsonResponse({'message': 'Import successful', 'count': len(members_to_create)})

        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            return SecureErrorHandler.secure_http_response(e, "CSV member import", request)


def import_variables_from_csv(request):
    """Import variables from CSV file."""
    if request.method == 'GET':
        return render(request, 'pybirdai/import_variables.html')
    elif request.method == 'POST':
        try:
            csv_file = request.FILES.get('csvFile')
            if not csv_file:
                return HttpResponseBadRequest('No file was uploaded')

            if not csv_file.name.endswith('.csv'):
                return HttpResponseBadRequest('File must be a CSV')

            # Read the CSV file
            decoded_file = csv_file.read().decode('utf-8').splitlines()
            reader = csv.DictReader(decoded_file)

            # Validate headers
            required_fields = {'VARIABLE_ID', 'CODE', 'NAME', 'DESCRIPTION', 'DOMAIN_ID'}
            headers = set(reader.fieldnames)
            if not required_fields.issubset(headers):
                missing = required_fields - headers
                return HttpResponseBadRequest(f'Missing required columns: {", ".join(missing)}')

            # Get SDDContext instance
            sdd_context = SDDContext()

            # Process each row
            variables_to_create = []
            for row in reader:
                try:
                    # Look up the domain
                    domain = DOMAIN.objects.get(domain_id=row['DOMAIN_ID'])

                    variable = VARIABLE(
                        variable_id=row['VARIABLE_ID'],
                        code=row['CODE'],
                        name=row['NAME'],
                        description=row['DESCRIPTION'],
                        domain_id=domain
                    )
                    variables_to_create.append(variable)
                except DOMAIN.DoesNotExist:
                    return HttpResponseBadRequest(f'Domain with ID {row["DOMAIN_ID"]} not found')

            # Bulk create the variables
            if variables_to_create:
                created_variables = VARIABLE.objects.bulk_create(variables_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT)

                # Update SDDContext variable dictionary
                for variable in created_variables:
                    sdd_context.variable_dictionary[variable.variable_id] = variable

            return JsonResponse({'message': 'Import successful', 'count': len(variables_to_create)})

        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            return SecureErrorHandler.secure_http_response(e, "CSV variable import", request)


def export_database_to_csv(request):
    """Export entire database to CSV zip."""
    from pybirdai.utils.export_db import _export_database_to_csv_logic

    if request.method == 'GET':
        return render(request, 'pybirdai/export_database.html')
    elif request.method == 'POST':
        zip_file_path, extract_dir = _export_database_to_csv_logic()
        with open(zip_file_path, 'rb') as f:
            response = HttpResponse(f.read(), content_type='application/zip')
            response['Content-Disposition'] = 'attachment; filename="database_export.zip"'
            return response


def import_bird_data_from_csv_export(request):
    """Django endpoint for importing metadata from CSV files."""
    from pybirdai.utils.clone_mode import import_from_metadata_export

    if request.method == 'GET':
        return render(request, 'pybirdai/import_database.html')

    files = json.loads(request.body.decode("utf-8"))
    # Use ordered import to maintain ID mappings across files
    results = import_from_metadata_export.CSVDataImporter().import_from_csv_strings_ordered(files["csv_files"])

    # Count successful imports
    successful_imports = sum(1 for result in results.values() if result.get('success', False))
    total_objects = sum(result.get('imported_count', 0) for result in results.values() if result.get('success', False))

    # Create JSON-serializable results (remove Django objects)
    serializable_results = {}
    for filename, result in results.items():
        serializable_results[filename] = {
            'success': result.get('success', False),
            'imported_count': result.get('imported_count', 0)
        }
        if 'error' in result:
            serializable_results[filename]['error'] = result['error']

    return JsonResponse({
        'message': f'Import successful: {successful_imports}/{len(results)} files imported, {total_objects} total objects',
        'results': serializable_results
    })


def load_variables_from_csv_file(csv_file_path):
    """
    Helper function to load variables from a CSV file.
    Used by run_full_setup to load extra variables.
    """
    try:
        if not os.path.exists(csv_file_path):
            logger.warning(f"Extra variables CSV file not found: {csv_file_path}")
            return 0

        logger.info(f"Loading extra variables from: {csv_file_path}")

        # Read the CSV file
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            # Validate headers
            required_fields = {'VARIABLE_ID', 'CODE', 'NAME', 'DESCRIPTION', 'DOMAIN_ID'}
            headers = set(reader.fieldnames)
            if not required_fields.issubset(headers):
                missing = required_fields - headers
                logger.error(f'Missing required columns in extra_variables.csv: {", ".join(missing)}')
                return 0

            # Get SDDContext instance
            sdd_context = SDDContext()

            # Process each row
            variables_to_create = []
            for row in reader:
                try:
                    # Look up the domain
                    domain = DOMAIN.objects.get(domain_id=row['DOMAIN_ID'])

                    variable = VARIABLE(
                        variable_id=row['VARIABLE_ID'],
                        code=row['CODE'],
                        name=row['NAME'],
                        description=row['DESCRIPTION'],
                        domain_id=domain
                    )
                    variables_to_create.append(variable)
                except DOMAIN.DoesNotExist:
                    logger.error(f'Domain with ID {row["DOMAIN_ID"]} not found in extra_variables.csv')
                    continue
                except Exception as e:
                    logger.error(f'Error processing variable row in extra_variables.csv: {str(e)}')
                    continue

            # Bulk create the variables
            if variables_to_create:
                created_variables = VARIABLE.objects.bulk_create(variables_to_create, batch_size=BULK_CREATE_BATCH_SIZE_DEFAULT)

                # Update SDDContext variable dictionary
                for variable in created_variables:
                    sdd_context.variable_dictionary[variable.variable_id] = variable

                logger.info(f"Successfully loaded {len(created_variables)} extra variables from CSV")
                return len(created_variables)
            else:
                logger.info("No extra variables to load from CSV")
                return 0

    except Exception as e:
        logger.error(f"Error loading extra variables from CSV: {str(e)}")
        return 0
