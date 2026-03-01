"""
Test Data Template Views for generating Excel templates.

Provides an API endpoint to generate Excel workbooks with:
- One worksheet per BIRD data model table
- Column headers matching field names
- Dropdown validation for domain fields
"""

import io
import logging
import os
from datetime import datetime

from django.http import HttpResponse, JsonResponse
from django.views.decorators.http import require_http_methods

logger = logging.getLogger(__name__)


# Default tables to include in the template
DEFAULT_TEST_TABLES = [
    'PRTY',
    'FNNCL_CNTRCT',
    'CRDT_FCLTY',
    'INSTRMNT',
    'ENTTY_RL',
    'INSTRMNT_RL',
    'INSTRMNT_ENTTY_RL_ASSGNMNT',
    'CLLTRL',
    'CLLTRL_RL',
    'PRTCTN_ARRNGMNT',
    'PRTCTN_RCVD',
]


@require_http_methods(["GET"])
def export_bird_excel_template(request):
    """
    Generate an Excel template for BIRD test data entry.

    Creates an Excel workbook with one worksheet per table, including:
    - Header row with field names
    - Dropdown validation for domain fields (showing "code: description")
    - A reference sheet with all domain values

    Query Parameters:
        tables: Comma-separated list of table names (e.g., "prty,instrmnt")
        include_all: If "true", include all BIRD tables (default: false)

    Returns:
        Excel file download
    """
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
        from openpyxl.worksheet.datavalidation import DataValidation
        from openpyxl.utils import get_column_letter
    except ImportError:
        return HttpResponse(
            "openpyxl is required for Excel export. Install with: pip install openpyxl",
            status=500,
            content_type='text/plain'
        )

    try:
        from pybirdai.utils.datapoint_test_run.test_data_template_utils import (
            get_bird_model_classes,
            extract_domain_dictionaries,
            get_model_fields_metadata,
            format_domain_value_for_dropdown,
        )

        # Parse query parameters
        tables_param = request.GET.get('tables', '')
        include_all = request.GET.get('include_all', 'false').lower() == 'true'

        # Get model classes
        all_models = get_bird_model_classes()
        if not all_models:
            candidate_paths = [
                os.path.join(os.getcwd(), 'pybirdai', 'models', 'bird_data_model.py'),
            ]
            try:
                from django.conf import settings
                candidate_paths.append(
                    os.path.join(str(settings.BASE_DIR), 'pybirdai', 'models', 'bird_data_model.py')
                )
            except Exception:
                pass

            # Preserve order while removing duplicates
            seen = set()
            deduped_candidates = []
            for path in candidate_paths:
                if path not in seen:
                    seen.add(path)
                    deduped_candidates.append(path)

            return JsonResponse(
                {
                    'error': (
                        'No BIRD data model tables were discovered. '
                        'Expected generated model file: pybirdai/models/bird_data_model.py'
                    ),
                    'hint': (
                        'Run the database setup/automode pipeline to generate bird_data_model.py, '
                        'then retry the template export.'
                    ),
                    'checked_paths': deduped_candidates,
                },
                status=500
            )

        if tables_param:
            # Use requested tables
            requested_tables = [t.strip().upper() for t in tables_param.split(',') if t.strip()]
            models_to_include = {
                name: model for name, model in all_models.items()
                if name.upper() in requested_tables
            }
            if not models_to_include:
                return JsonResponse(
                    {'error': f'No valid tables found. Available: {list(all_models.keys())[:10]}...'},
                    status=400
                )
        elif include_all:
            models_to_include = all_models
        else:
            # Use default tables
            models_to_include = {
                name: model for name, model in all_models.items()
                if name.upper() in [t.upper() for t in DEFAULT_TEST_TABLES]
            }

        # Create workbook
        wb = openpyxl.Workbook()
        wb.remove(wb.active)  # Remove default sheet

        # Define styles
        header_fill = PatternFill(start_color='0D6EFD', end_color='0D6EFD', fill_type='solid')
        header_font = Font(color='FFFFFF', bold=True, size=10)
        cell_font = Font(size=10)
        thin_border = Border(
            left=Side(style='thin', color='CCCCCC'),
            right=Side(style='thin', color='CCCCCC'),
            top=Side(style='thin', color='CCCCCC'),
            bottom=Side(style='thin', color='CCCCCC')
        )
        header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)

        # Track all domains for reference sheet
        all_domains_data = []

        # Create worksheet for each model
        for model_name in sorted(models_to_include.keys()):
            model_class = models_to_include[model_name]

            # Create worksheet (Excel sheet names limited to 31 chars)
            ws_name = model_name.lower()[:31]
            ws = wb.create_sheet(title=ws_name)

            # Get field metadata and domains
            fields_metadata = get_model_fields_metadata(model_class)
            domain_dicts = extract_domain_dictionaries(model_class)

            if not fields_metadata:
                continue

            # Write header row
            for col_idx, field_meta in enumerate(fields_metadata, start=1):
                cell = ws.cell(row=1, column=col_idx, value=field_meta['name'])
                cell.fill = header_fill
                cell.font = header_font
                cell.border = thin_border
                cell.alignment = header_alignment

                # Set column width
                col_letter = get_column_letter(col_idx)
                # Width based on header length, min 12, max 30
                width = min(30, max(12, len(field_meta['name']) + 2))
                ws.column_dimensions[col_letter].width = width

                # Add dropdown validation for domain fields
                field_name = field_meta['name']
                if field_name in domain_dicts:
                    domain = domain_dicts[field_name]

                    # Record domain for reference sheet
                    for code, description in domain.items():
                        all_domains_data.append({
                            'table': model_name,
                            'field': field_name,
                            'code': code,
                            'description': description
                        })

                    # Create dropdown values in "code: description" format
                    dropdown_values = []
                    for code, description in sorted(domain.items(), key=lambda x: str(x[0])):
                        formatted = format_domain_value_for_dropdown(code, description, max_length=100)
                        dropdown_values.append(formatted)

                    # Excel has a 255 character limit for formula-based validation
                    validation_string = ','.join(dropdown_values)
                    if len(validation_string) <= 255:
                        dv = DataValidation(
                            type="list",
                            formula1=f'"{validation_string}"',
                            showDropDown=False,  # False = show dropdown arrow
                            allow_blank=True,
                            showErrorMessage=True,
                            errorStyle='warning',
                            errorTitle='Invalid value',
                            error='Please select a value from the dropdown or enter a valid code.'
                        )
                        dv.add(f"{col_letter}2:{col_letter}1000")
                        ws.add_data_validation(dv)
                    else:
                        # Too many values for inline validation
                        # Add a comment instead
                        logger.debug(
                            f"Domain {field_name} in {model_name} has too many values for dropdown "
                            f"({len(dropdown_values)} values). Adding as comment."
                        )

            # Add data type hints in row 2 (commented out for clean template)
            # for col_idx, field_meta in enumerate(fields_metadata, start=1):
            #     hint = field_meta['python_type']
            #     if field_meta['is_foreign_key']:
            #         hint = f"FK -> {field_meta['related_model']}"
            #     ws.cell(row=2, column=col_idx, value=hint)

            # Freeze header row
            ws.freeze_panes = 'A2'

            # Set row height for header
            ws.row_dimensions[1].height = 25

        # Create Domains Reference sheet
        if all_domains_data:
            ref_ws = wb.create_sheet(title='_Domains_Reference')

            # Headers
            ref_headers = ['Table', 'Field', 'Code', 'Description']
            for col_idx, header in enumerate(ref_headers, start=1):
                cell = ref_ws.cell(row=1, column=col_idx, value=header)
                cell.fill = PatternFill(start_color='6C757D', end_color='6C757D', fill_type='solid')
                cell.font = Font(color='FFFFFF', bold=True)
                cell.border = thin_border

            # Set column widths
            ref_ws.column_dimensions['A'].width = 25
            ref_ws.column_dimensions['B'].width = 30
            ref_ws.column_dimensions['C'].width = 15
            ref_ws.column_dimensions['D'].width = 60

            # Data rows
            for row_idx, domain_item in enumerate(all_domains_data, start=2):
                ref_ws.cell(row=row_idx, column=1, value=domain_item['table'])
                ref_ws.cell(row=row_idx, column=2, value=domain_item['field'])
                ref_ws.cell(row=row_idx, column=3, value=domain_item['code'])
                # Clean up description
                desc = domain_item['description'].replace('_', ' ')
                ref_ws.cell(row=row_idx, column=4, value=desc)

            # Freeze header
            ref_ws.freeze_panes = 'A2'

        # Create Instructions sheet
        instr_ws = wb.create_sheet(title='_Instructions', index=0)
        instructions = [
            ['BIRD Test Data Template'],
            [''],
            ['How to use this template:'],
            ['1. Each worksheet (tab) represents a BIRD data model table'],
            ['2. The header row contains the field names - do not modify'],
            ['3. Enter your test data in rows 2 and below'],
            ['4. Fields with dropdowns have enumerated values - select from the list'],
            ['5. For dropdown fields, enter just the CODE part (before the colon)'],
            ['6. Date fields should use ISO format: YYYY-MM-DD'],
            ['7. Leave cells empty for NULL values'],
            ['8. Foreign key fields (starting with "the") should contain the unique ID of the referenced record'],
            [''],
            ['To create CSV files for test fixtures:'],
            ['1. Complete your data entry in each table worksheet'],
            ['2. For each worksheet, Save As -> CSV (Comma delimited)'],
            ['3. Name the CSV file after the table (e.g., prty.csv, instrmnt.csv)'],
            ['4. Place CSV files in the scenario folder alongside the existing sql_inserts.sql'],
            [''],
            ['See the _Domains_Reference sheet for a complete list of valid domain values.'],
            [''],
            [f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'],
            [f'Tables included: {len(models_to_include)}'],
        ]

        for row_idx, row_data in enumerate(instructions, start=1):
            for col_idx, value in enumerate(row_data, start=1):
                cell = instr_ws.cell(row=row_idx, column=col_idx, value=value)
                if row_idx == 1:
                    cell.font = Font(bold=True, size=14)
                elif row_idx in [3, 13]:
                    cell.font = Font(bold=True, size=11)

        instr_ws.column_dimensions['A'].width = 100

        # Generate response
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"bird_test_data_template_{timestamp}.xlsx"

        response = HttpResponse(
            output.read(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = f'attachment; filename="{filename}"'

        logger.info(f"Generated Excel template with {len(models_to_include)} tables")
        return response

    except Exception as e:
        logger.exception("Error generating Excel template")
        return HttpResponse(
            f"Error generating Excel template: {str(e)}",
            status=500,
            content_type='text/plain'
        )


@require_http_methods(["GET"])
def list_available_tables(request):
    """
    List all available BIRD tables that can be included in the template.

    Returns:
        JSON response with list of table names
    """
    try:
        from pybirdai.utils.datapoint_test_run.test_data_template_utils import (
            get_bird_model_classes,
            DEFAULT_TEST_TABLES,
        )

        all_models = get_bird_model_classes()
        if not all_models:
            return JsonResponse(
                {
                    'error': (
                        'No BIRD data model tables were discovered. '
                        'Expected generated model file: pybirdai/models/bird_data_model.py'
                    ),
                    'tables': [],
                    'total': 0,
                    'default_tables': DEFAULT_TEST_TABLES,
                },
                status=500
            )

        return JsonResponse({
            'tables': sorted(all_models.keys()),
            'total': len(all_models),
            'default_tables': DEFAULT_TEST_TABLES,
        })

    except Exception as e:
        logger.exception("Error listing tables")
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["POST"])
def convert_sql_to_csv(request):
    """
    Convert an existing sql_inserts.sql file to CSV files.

    Request Body (JSON):
        scenario_path: Path to scenario directory containing sql_inserts.sql

    Returns:
        JSON response with conversion results
    """
    try:
        import json
        data = json.loads(request.body)
        scenario_path = data.get('scenario_path')

        if not scenario_path:
            return JsonResponse({'error': 'scenario_path is required'}, status=400)

        from pybirdai.utils.datapoint_test_run.sql_to_csv_converter import SQLToCSVConverter

        converter = SQLToCSVConverter()
        output_files = converter.convert_scenario_in_place(scenario_path)

        return JsonResponse({
            'success': True,
            'files': output_files,
            'message': f'Converted {len(output_files)} tables to CSV'
        })

    except FileNotFoundError as e:
        return JsonResponse({'error': str(e)}, status=404)
    except Exception as e:
        logger.exception("Error converting SQL to CSV")
        return JsonResponse({'error': str(e)}, status=500)
