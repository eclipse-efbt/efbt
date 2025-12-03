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


# Mapping import/export functions

def export_mapping_template(request):
    """
    Export an empty mapping template CSV with example data.
    Allows users to download a template they can fill out to create new mappings.
    """
    try:
        from pybirdai.entry_points.template_mapping_definition import RunExportMappingTemplate

        # Generate CSV content
        csv_content = RunExportMappingTemplate.run_export_mapping_template()

        # Create HTTP response with CSV content
        response = HttpResponse(csv_content, content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="mapping_template_example.csv"'

        return response

    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        return SecureErrorHandler.secure_http_response(e, "Export mapping template", request)


def export_mapping_data(request, mapping_id):
    """
    Export an existing mapping definition to business-friendly CSV format.
    Allows users to download, edit, and re-import mapping data.
    """
    try:
        from pybirdai.entry_points.template_mapping_definition import RunExportMappingData
        from datetime import datetime
        from pybirdai.models.bird_meta_data_model import MAPPING_DEFINITION

        # Generate CSV content
        csv_content = RunExportMappingData.run_export_mapping_data(mapping_id)

        # Get mapping code for filename
        try:
            mapping_def = MAPPING_DEFINITION.objects.get(mapping_id=mapping_id)
            mapping_code = mapping_def.code or mapping_id
        except MAPPING_DEFINITION.DoesNotExist:
            mapping_code = mapping_id

        # Create filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"mapping_{mapping_code}_{timestamp}.csv"

        # Create HTTP response with CSV content
        response = HttpResponse(csv_content, content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'

        return response

    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        return SecureErrorHandler.secure_http_response(e, "Export mapping data", request)


def import_mapping_from_csv(request):
    """
    Import mapping data from CSV file.
    GET: Display upload form
    POST: Process uploaded CSV file and show import form with metadata fields
    """
    from django.http import HttpResponseBadRequest
    from pybirdai.models.bird_meta_data_model import CUBE, MAINTENANCE_AGENCY

    if request.method == 'GET':
        # Get all cubes for the cube selection dropdown
        cubes = CUBE.objects.all().order_by('name')
        agencies = MAINTENANCE_AGENCY.objects.all().order_by('maintenance_agency_id')

        return render(request, 'pybirdai/import_mapping_from_csv.html', {
            'cubes': cubes,
            'agencies': agencies
        })

    elif request.method == 'POST':
        try:
            from pybirdai.entry_points.template_mapping_definition import RunImportMappingData

            # Check if this is file upload or final import submission
            if 'csvFile' in request.FILES:
                # Step 1: Parse and validate CSV file
                csv_file = request.FILES['csvFile']

                if not csv_file.name.endswith('.csv'):
                    return HttpResponseBadRequest('File must be a CSV')

                # Parse CSV
                parsed_data = RunImportMappingData.run_parse_mapping_csv(csv_file)

                # Validate data
                validation_report = RunImportMappingData.run_validate_mapping_csv(parsed_data)

                # If validation fails, return errors
                if not validation_report['is_valid']:
                    return JsonResponse({
                        'success': False,
                        'errors': validation_report['errors'],
                        'warnings': validation_report['warnings']
                    })

                # Return parsed data and validation results for preview
                return JsonResponse({
                    'success': True,
                    'parsed_data': parsed_data,
                    'validation_report': validation_report,
                    'row_count': len(parsed_data['rows'])
                })

            else:
                # Step 2: Final import submission
                # Get form data
                mapping_name = request.POST.get('mapping_name', '').strip()
                mapping_code = request.POST.get('mapping_code', '').strip()
                mapping_type = request.POST.get('mapping_type', '').strip()
                algorithm = request.POST.get('algorithm', '').strip()
                cube_ids = request.POST.getlist('cube_ids')
                maintenance_agency_id = request.POST.get('maintenance_agency_id', '').strip()
                overwrite = request.POST.get('overwrite') == 'true'
                parsed_data_json = request.POST.get('parsed_data')

                # Validate required fields
                if not mapping_name:
                    return JsonResponse({'success': False, 'error': 'Mapping name is required'})
                if not mapping_code:
                    return JsonResponse({'success': False, 'error': 'Mapping code is required'})
                if not parsed_data_json:
                    return JsonResponse({'success': False, 'error': 'No CSV data found. Please upload file again.'})

                # Parse the JSON data
                parsed_data = json.loads(parsed_data_json)

                # Import data
                mapping_id = RunImportMappingData.run_import_mapping_data(
                    parsed_data=parsed_data,
                    mapping_name=mapping_name,
                    mapping_code=mapping_code,
                    mapping_type=mapping_type,
                    algorithm=algorithm,
                    cube_ids=cube_ids,
                    maintenance_agency_id=maintenance_agency_id,
                    overwrite=overwrite
                )

                return JsonResponse({
                    'success': True,
                    'message': f'Mapping "{mapping_name}" imported successfully',
                    'mapping_id': mapping_id,
                    'redirect_url': '/pybirdai/edit-mapping-definitions/'
                })

        except Exception as e:
            from pybirdai.utils.secure_error_handling import SecureErrorHandler
            error_msg = str(e)
            return JsonResponse({'success': False, 'error': error_msg})


def delete_mapping_row(request):
    """View function for handling the deletion of a mapping row."""
    from django.db import transaction
    from pybirdai.models.bird_meta_data_model import MAPPING_DEFINITION, MEMBER_MAPPING_ITEM

    logger.info("Handling delete mapping row request")
    if request.method != 'POST':
        logger.warning("Invalid request method for delete_mapping_row")
        return JsonResponse({'success': False, 'error': 'Invalid request method'})

    try:
        data = json.loads(request.body)
        logger.debug(f"Received data for deletion: {data}")
        mapping_id = data.get('mapping_id')
        row_index = data.get('row_index')

        logger.info(f"Deleting row {row_index} from mapping {mapping_id}")

        # Use atomic transaction to ensure all operations succeed or fail together
        with transaction.atomic():
            # Get the mapping definition
            mapping_def = MAPPING_DEFINITION.objects.get(mapping_id=mapping_id)
            logger.debug(f"Found mapping definition: {mapping_def.name}")

            # Find all member mapping items in the specified row
            member_mapping_items = MEMBER_MAPPING_ITEM.objects.filter(
                member_mapping_id=mapping_def.member_mapping_id,
                member_mapping_row=row_index
            )
            logger.debug(f"Found {member_mapping_items.count()} items to delete in row {row_index}")

            # Delete the items within the atomic transaction
            member_mapping_items.delete()
            logger.info(f"Successfully deleted {row_index} from mapping {mapping_id}")

        return JsonResponse({'success': True})
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error deleting mapping row: {str(e)}", exc_info=True)
        error_data = SecureErrorHandler.handle_exception(e, 'mapping row deletion', request)
        return JsonResponse({'success': False, 'error': error_data['message']})


def duplicate_mapping(request):
    """View function for duplicating an existing mapping."""
    from datetime import datetime
    from pybirdai.models.bird_meta_data_model import (
        VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM, MEMBER_MAPPING, MEMBER_MAPPING_ITEM,
        MAPPING_TO_CUBE, MAPPING_DEFINITION
    )

    sdd_context = SDDContext()
    logger.info("Handling duplicate mapping request")
    if request.method != 'POST':
        logger.warning("Invalid request method for duplicate_mapping")
        return JsonResponse({'success': False, 'error': 'Invalid request method'})

    try:
        data = json.loads(request.body)
        logger.debug(f"Received data for duplication: {data}")
        source_mapping_id = data.get('source_mapping_id')
        new_mapping_name = data.get('new_mapping_name')
        logger.info(f"Duplicating mapping {source_mapping_id} with new name: {new_mapping_name}")

        # Get timestamp for new instances
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Get source mapping
        source_mapping = MAPPING_DEFINITION.objects.get(mapping_id=source_mapping_id)
        logger.debug(f"Found source mapping: {source_mapping.name}")

        # Extract shortened mapping name for new IDs
        shortened_name = new_mapping_name

        # Copy member mapping
        new_member_mapping = MEMBER_MAPPING.objects.create(
            member_mapping_id=f"MM_{shortened_name}__{timestamp}",
            code=f"MM_{shortened_name}__{timestamp}",
            name=f"{new_mapping_name} - Members"
        )
        sdd_context.member_mapping_dictionary[new_member_mapping.member_mapping_id] = new_member_mapping
        logger.debug(f"Created new member mapping: {new_member_mapping.member_mapping_id}")

        # Copy variable mapping
        new_variable_mapping = VARIABLE_MAPPING.objects.create(
            variable_mapping_id=f"VM_{shortened_name}__{timestamp}",
            code=f"VM_{shortened_name}__{timestamp}",
            name=f"{new_mapping_name} - Variables"
        )
        sdd_context.variable_mapping_dictionary[new_variable_mapping.variable_mapping_id] = new_variable_mapping
        logger.debug(f"Created new variable mapping: {new_variable_mapping.variable_mapping_id}")

        # Create new mapping definition
        new_mapping = MAPPING_DEFINITION.objects.create(
            mapping_id=f"MAP_{shortened_name}__{timestamp}",
            code=f"MAP_{shortened_name}__{timestamp}",
            name=new_mapping_name,
            member_mapping_id=new_member_mapping,
            variable_mapping_id=new_variable_mapping
            )
        sdd_context.mapping_definition_dictionary[new_mapping.mapping_id] = new_mapping
        logger.debug(f"Created new mapping definition: {new_mapping.mapping_id}")

        # Copy variable mapping items
        var_items = VARIABLE_MAPPING_ITEM.objects.filter(variable_mapping_id=source_mapping.variable_mapping_id)
        logger.debug(f"Copying {var_items.count()} variable mapping items")
        for item in var_items:
            new_variable_item = VARIABLE_MAPPING_ITEM.objects.create(
                variable_mapping_id=new_variable_mapping,
                variable_id=item.variable_id,
                is_source=item.is_source
            )
            try:
                variable_mapping_list = sdd_context.variable_mapping_item_dictionary[
                new_variable_item.variable_mapping_id.variable_mapping_id]
                variable_mapping_list.append(new_variable_item)
            except KeyError:
                sdd_context.variable_mapping_item_dictionary[
                    new_variable_item.variable_mapping_id.variable_mapping_id] = [new_variable_item]

        # Copy member mapping items
        member_items = MEMBER_MAPPING_ITEM.objects.filter(member_mapping_id=source_mapping.member_mapping_id)
        logger.debug(f"Copying {member_items.count()} member mapping items")
        for item in member_items:
            new_mm_item=MEMBER_MAPPING_ITEM.objects.create(
                member_mapping_id=new_member_mapping,
                member_mapping_row=item.member_mapping_row,
                variable_id=item.variable_id,
                member_id=item.member_id,
                is_source=item.is_source
            )
            try:
                member_mapping_list = sdd_context.member_mapping_items_dictionary[
                    new_mm_item.member_mapping_id.member_mapping_id]
                member_mapping_list.append(new_mm_item)
            except KeyError:
                sdd_context.member_mapping_items_dictionary[
                    new_mm_item.member_mapping_id.member_mapping_id] = [new_mm_item]

        # Create mapping to cube with version suffix
        mapping_to_cube = MAPPING_TO_CUBE.objects.create(
            mapping_id=new_mapping,
            cube_mapping_id=f"{new_mapping.code}_v1"
        )
        logger.debug(f"Created new mapping to cube: {mapping_to_cube.cube_mapping_id}")

        logger.info(f"Successfully duplicated mapping {source_mapping_id} to {new_mapping.mapping_id}")
        return JsonResponse({'success': True})
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error duplicating mapping: {str(e)}", exc_info=True)
        error_data = SecureErrorHandler.handle_exception(e, 'mapping duplication', request)
        return JsonResponse({'success': False, 'error': error_data['message']})


def update_mapping_row(request):
    """View function for updating a mapping row."""
    from django.db import transaction
    from pybirdai.models.bird_meta_data_model import (
        MAPPING_DEFINITION, MEMBER_MAPPING_ITEM, VARIABLE, MEMBER
    )

    logger.info("Handling update mapping row request")
    sdd_context = SDDContext()
    if request.method != 'POST':
        logger.warning("Invalid request method for update_mapping_row")
        return JsonResponse({'success': False, 'error': 'Invalid request method'})

    try:
        data = json.loads(request.body)
        logger.debug(f"Received data for row update: {data}")
        mapping_id = data.get('mapping_id')
        row_index = data.get('row_index')
        source_data = data.get('source_data', {})
        target_data = data.get('target_data', {})

        logger.info(f"Updating row {row_index} in mapping {mapping_id}")

        # Use atomic transaction to ensure all operations succeed or fail together
        with transaction.atomic():
            # Get mapping definition
            mapping_def = MAPPING_DEFINITION.objects.get(mapping_id=mapping_id)
            logger.debug(f"Found mapping definition: {mapping_def.name}")

            # Delete existing row items
            existing_items = MEMBER_MAPPING_ITEM.objects.filter(
                member_mapping_id=mapping_def.member_mapping_id,
                member_mapping_row=row_index
            )
            logger.debug(f"Deleting {existing_items.count()} existing items from row {row_index}")

            try:
                # delete existing items if they are in this list
                for mm_item in existing_items:
                    member_mapping_list = sdd_context.member_mapping_items_dictionary[
                    mm_item.member_mapping_id.member_mapping_id]
                    for item in member_mapping_list:
                        if item.member_mapping_row == row_index:
                            member_mapping_list.remove(item)
            except KeyError:
                pass
            existing_items.delete()

            # Add new source items
            logger.debug(f"Adding {len(source_data.get('variabless', []))} source items")
            for variable, member in zip(source_data.get('variabless', []), source_data.get('members', [])):
                if member:
                    logger.debug(f"Variable code: {variable}, Member: {member}")
                    variable_name, variable_code = variable.split("(")[0][:-1], variable.split("(")[1].rstrip(")")
                    logger.debug(f"Variable code: {variable_code}, Variable name: {variable_name}")
                    variable_obj = VARIABLE.objects.filter(code=variable_code,name=variable_name).first()
                    member_obj = MEMBER.objects.get(member_id=member)
                    logger.debug(f"Adding source mapping: Variable {variable_obj.code} -> Member {member_obj.code}")

                    new_mm_item = MEMBER_MAPPING_ITEM.objects.create(
                        member_mapping_id=mapping_def.member_mapping_id,
                        member_mapping_row=row_index,
                        variable_id=variable_obj,
                        member_id=member_obj,
                        is_source='true'
                    )
                    try:
                        member_mapping_list = sdd_context.member_mapping_items_dictionary[
                            new_mm_item.member_mapping_id.member_mapping_id]
                        member_mapping_list.append(new_mm_item)
                    except KeyError:
                        sdd_context.member_mapping_items_dictionary[
                            new_mm_item.member_mapping_id.member_mapping_id] = [new_mm_item]
            # Add new target items
            logger.debug(f"Adding {len(target_data.get('variablses', []))} target items")
            for variable, member in zip(target_data.get('variablses', []), target_data.get('members', [])):
                if member:
                    logger.debug(f"Variable code: {variable}, Member: {member}")
                    variable_name, variable_code = variable.split(" ")[0], variable.split(" ")[1].strip("(").rstrip(")")
                    variable_obj = VARIABLE.objects.filter(code=variable_code,name=variable_name).first()
                    if not( member == "None"):
                        member_obj = MEMBER.objects.get(member_id=member)
                        logger.debug(f"Adding target mapping: Variable {variable_obj.code} -> Member {member_obj.code}")

                        new_mm_item = MEMBER_MAPPING_ITEM.objects.create(
                            member_mapping_id=mapping_def.member_mapping_id,
                            member_mapping_row=row_index,
                            variable_id=variable_obj,
                            member_id=member_obj,
                            is_source='false'
                        )
                        try:
                            member_mapping_list = sdd_context.member_mapping_items_dictionary[
                                new_mm_item.member_mapping_id.member_mapping_id]
                            member_mapping_list.append(new_mm_item)
                        except KeyError:
                            sdd_context.member_mapping_items_dictionary[
                                new_mm_item.member_mapping_id.member_mapping_id] = [new_mm_item]
        logger.info(f"Successfully updated row {row_index} in mapping {mapping_id}")
        return JsonResponse({'success': True})
    except Exception as e:
        from pybirdai.utils.secure_error_handling import SecureErrorHandler
        logger.error(f"Error updating mapping row: {str(e)}", exc_info=True)
        error_data = SecureErrorHandler.handle_exception(e, 'mapping row update', request)
        return JsonResponse({'success': False, 'error': error_data['message']})
