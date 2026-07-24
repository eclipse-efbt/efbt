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
import re
from datetime import datetime

from django.conf import settings
from django.core.exceptions import SuspiciousFileOperation
from django.http import HttpResponse, JsonResponse
from django.utils._os import safe_join
from django.views.decorators.http import require_http_methods
from pybirdai.utils.secure_error_handling import SecureErrorHandler

logger = logging.getLogger(__name__)

EXCEL_MAX_SHEET_TITLE_LENGTH = 31
EXCEL_INVALID_SHEET_TITLE_CHARS = re.compile(r"[\\/*?:\[\]]")


# Default tables to include in the template
DEFAULT_TEST_TABLES = [
    "PRTY",
    "FNNCL_CNTRCT",
    "CRDT_FCLTY",
    "INSTRMNT",
    "ENTTY_RL",
    "INSTRMNT_RL",
    "INSTRMNT_ENTTY_RL_ASSGNMNT",
    "CLLTRL",
    "CLLTRL_RL",
    "PRTCTN_ARRNGMNT",
    "PRTCTN_RCVD",
]


def _make_unique_excel_sheet_title(raw_title: str, used_titles: set[str]) -> str:
    """
    Return an Excel-safe, case-insensitively unique worksheet title.

    Excel limits worksheet titles to 31 characters. Letting openpyxl resolve
    collisions after truncation can produce a 32+ character title because it
    appends a numeric suffix to an already 31-character title.
    """
    base_title = EXCEL_INVALID_SHEET_TITLE_CHARS.sub("_", str(raw_title))
    base_title = base_title.strip().strip("'") or "Sheet"

    candidate = base_title[:EXCEL_MAX_SHEET_TITLE_LENGTH]
    suffix_number = 2
    while candidate.casefold() in used_titles:
        suffix = f"_{suffix_number}"
        prefix_length = EXCEL_MAX_SHEET_TITLE_LENGTH - len(suffix)
        candidate = f"{base_title[:prefix_length]}{suffix}"
        suffix_number += 1

    used_titles.add(candidate.casefold())
    return candidate


def _internal_json_error_response(exception: Exception, context: str, request, status: int = 500):
    """Hide implementation details from JSON error responses."""
    error_data = SecureErrorHandler.handle_exception(exception, context, request)
    return JsonResponse({"error": error_data["message"]}, status=status)


def _internal_http_error_response(exception: Exception, context: str, request, status: int = 500):
    """Hide implementation details from plain text responses."""
    error_data = SecureErrorHandler.handle_exception(exception, context, request)
    return HttpResponse(error_data["message"], status=status, content_type="text/plain")


def _resolve_project_scenario_path(scenario_path: str) -> str:
    """Keep fixture conversion requests inside the project tree."""
    try:
        return safe_join(str(settings.BASE_DIR), scenario_path)
    except SuspiciousFileOperation as exc:
        raise ValueError("Invalid fixture scenario path.") from exc


@require_http_methods(["GET"])
def export_bird_excel_template(request):
    """
    Generate an Excel template for BIRD test data entry.

    Creates an Excel workbook with one worksheet per table, including:
    - Header row with field names
    - Dropdown validation for domain fields (showing "code: description")
    - A reference sheet with all domain values
    - A table index mapping full model names to Excel-safe worksheet names

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
        from openpyxl.utils import get_column_letter, quote_sheetname
        from openpyxl.workbook.defined_name import DefinedName
    except ImportError:
        return HttpResponse(
            "openpyxl is required for Excel export. Install with: pip install openpyxl",
            status=500,
            content_type="text/plain",
        )

    try:
        from pybirdai.utils.datapoint_test_run.test_data_template_utils import (
            get_bird_model_classes,
            extract_domain_dictionaries,
            get_model_fields_metadata,
            format_domain_value_for_dropdown,
        )

        # Parse query parameters
        tables_param = request.GET.get("tables", "")
        include_all = request.GET.get("include_all", "false").lower() == "true"

        # Get model classes
        all_models = get_bird_model_classes()
        if not all_models:
            candidate_paths = [
                os.path.join(os.getcwd(), "pybirdai", "models", "bird_data_model.py"),
            ]
            try:
                from django.conf import settings

                candidate_paths.append(os.path.join(str(settings.BASE_DIR), "pybirdai", "models", "bird_data_model.py"))
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
                    "error": (
                        "No BIRD data model tables were discovered. "
                        "Expected generated model file: pybirdai/models/bird_data_model.py"
                    ),
                    "hint": (
                        "Run the database setup/automode pipeline to generate bird_data_model.py, "
                        "then retry the template export."
                    ),
                    "checked_paths": deduped_candidates,
                },
                status=500,
            )

        if tables_param:
            # Use requested tables
            requested_tables = [t.strip().upper() for t in tables_param.split(",") if t.strip()]
            models_to_include = {name: model for name, model in all_models.items() if name.upper() in requested_tables}
            if not models_to_include:
                return JsonResponse(
                    {"error": f"No valid tables found. Available: {list(all_models.keys())[:10]}..."}, status=400
                )
        elif include_all:
            models_to_include = all_models
        else:
            # Use default tables
            models_to_include = {
                name: model
                for name, model in all_models.items()
                if name.upper() in [t.upper() for t in DEFAULT_TEST_TABLES]
            }

        # Create workbook
        wb = openpyxl.Workbook()
        wb.remove(wb.active)  # Remove default sheet
        reserved_sheet_titles = {
            "_Instructions",
            "_Table_Index",
            "_Domains_Reference",
            "_Validation_Lists",
        }
        used_sheet_titles = {title.casefold() for title in reserved_sheet_titles}

        # Define styles
        header_fill = PatternFill(start_color="0D6EFD", end_color="0D6EFD", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True, size=10)
        cell_font = Font(size=10)
        thin_border = Border(
            left=Side(style="thin", color="CCCCCC"),
            right=Side(style="thin", color="CCCCCC"),
            top=Side(style="thin", color="CCCCCC"),
            bottom=Side(style="thin", color="CCCCCC"),
        )
        header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        # Track all domains for reference sheet
        all_domains_data = []
        table_sheet_index = []
        validation_ws = None
        validation_range_names = {}

        # Create worksheet for each model
        for model_name in sorted(models_to_include.keys()):
            model_class = models_to_include[model_name]

            # Keep every title within Excel's 31-character limit, including
            # suffixes needed when long model names share the same prefix.
            ws_name = _make_unique_excel_sheet_title(
                model_name.lower(),
                used_sheet_titles,
            )
            ws = wb.create_sheet(title=ws_name)
            table_sheet_index.append((model_name, ws_name))

            # Get field metadata and domains
            fields_metadata = get_model_fields_metadata(model_class)
            domain_dicts = extract_domain_dictionaries(model_class)

            if not fields_metadata:
                continue

            # Write header row
            for col_idx, field_meta in enumerate(fields_metadata, start=1):
                cell = ws.cell(row=1, column=col_idx, value=field_meta["name"])
                cell.fill = header_fill
                cell.font = header_font
                cell.border = thin_border
                cell.alignment = header_alignment

                # Set column width
                col_letter = get_column_letter(col_idx)
                # Width based on header length, min 12, max 30
                width = min(30, max(12, len(field_meta["name"]) + 2))
                ws.column_dimensions[col_letter].width = width

                # Add dropdown validation for domain fields
                field_name = field_meta["name"]
                if field_name in domain_dicts:
                    domain = domain_dicts[field_name]

                    # Record domain for reference sheet
                    for code, description in domain.items():
                        all_domains_data.append(
                            {"table": model_name, "field": field_name, "code": code, "description": description}
                        )

                    sorted_domain_items = sorted(domain.items(), key=lambda x: str(x[0]))

                    # Create dropdown values in "code: description" format.
                    # Store them in a hidden sheet and refer to a workbook-level
                    # defined name. Inline validation lists are fragile because
                    # Excel limits the formula to 255 characters and treats
                    # commas and quotes in labels as syntax.
                    dropdown_values = []
                    for code, description in sorted_domain_items:
                        formatted = format_domain_value_for_dropdown(code, description, max_length=100)
                        dropdown_values.append(formatted)

                    if dropdown_values:
                        domain_key = tuple((str(code), str(description)) for code, description in sorted_domain_items)
                        range_name = validation_range_names.get(domain_key)
                        if range_name is None:
                            if validation_ws is None:
                                validation_ws = wb.create_sheet(title="_Validation_Lists")

                            validation_column = len(validation_range_names) + 1
                            validation_column_letter = get_column_letter(validation_column)
                            range_name = f"BIRD_Domain_{validation_column}"
                            validation_range_names[domain_key] = range_name

                            validation_ws.cell(
                                row=1,
                                column=validation_column,
                                value=range_name,
                            )
                            for row_idx, dropdown_value in enumerate(dropdown_values, start=2):
                                validation_ws.cell(
                                    row=row_idx,
                                    column=validation_column,
                                    value=dropdown_value,
                                )

                            range_reference = (
                                f"{quote_sheetname(validation_ws.title)}!"
                                f"${validation_column_letter}$2:"
                                f"${validation_column_letter}${len(dropdown_values) + 1}"
                            )
                            wb.defined_names.add(DefinedName(range_name, attr_text=range_reference))

                        dv = DataValidation(
                            type="list",
                            formula1=f"={range_name}",
                            showDropDown=False,  # False = show dropdown arrow
                            allow_blank=True,
                            showErrorMessage=True,
                            errorStyle="warning",
                            errorTitle="Invalid value",
                            error="Please select a value from the dropdown or enter a valid code.",
                        )
                        dv.add(f"{col_letter}2:{col_letter}1000")
                        ws.add_data_validation(dv)

            # Add data type hints in row 2 (commented out for clean template)
            # for col_idx, field_meta in enumerate(fields_metadata, start=1):
            #     hint = field_meta['python_type']
            #     if field_meta['is_foreign_key']:
            #         hint = f"FK -> {field_meta['related_model']}"
            #     ws.cell(row=2, column=col_idx, value=hint)

            # Freeze header row
            ws.freeze_panes = "A2"

            # Set row height for header
            ws.row_dimensions[1].height = 25

        # Create Domains Reference sheet
        if all_domains_data:
            ref_ws = wb.create_sheet(title="_Domains_Reference")

            # Headers
            ref_headers = ["Table", "Field", "Code", "Description"]
            for col_idx, header in enumerate(ref_headers, start=1):
                cell = ref_ws.cell(row=1, column=col_idx, value=header)
                cell.fill = PatternFill(start_color="6C757D", end_color="6C757D", fill_type="solid")
                cell.font = Font(color="FFFFFF", bold=True)
                cell.border = thin_border

            # Set column widths
            ref_ws.column_dimensions["A"].width = 25
            ref_ws.column_dimensions["B"].width = 30
            ref_ws.column_dimensions["C"].width = 15
            ref_ws.column_dimensions["D"].width = 60

            # Data rows
            for row_idx, domain_item in enumerate(all_domains_data, start=2):
                ref_ws.cell(row=row_idx, column=1, value=domain_item["table"])
                ref_ws.cell(row=row_idx, column=2, value=domain_item["field"])
                ref_ws.cell(row=row_idx, column=3, value=domain_item["code"])
                # Clean up description
                desc = domain_item["description"].replace("_", " ")
                ref_ws.cell(row=row_idx, column=4, value=desc)

            # Freeze header
            ref_ws.freeze_panes = "A2"

        # Create a stable mapping from full BIRD model names to the necessarily
        # shortened worksheet titles.
        index_ws = wb.create_sheet(title="_Table_Index", index=0)
        index_headers = ["BIRD model/table", "Worksheet"]
        for col_idx, header in enumerate(index_headers, start=1):
            cell = index_ws.cell(row=1, column=col_idx, value=header)
            cell.fill = PatternFill(start_color="6C757D", end_color="6C757D", fill_type="solid")
            cell.font = Font(color="FFFFFF", bold=True)
            cell.border = thin_border

        for row_idx, (model_name, sheet_name) in enumerate(table_sheet_index, start=2):
            index_ws.cell(row=row_idx, column=1, value=model_name)
            index_ws.cell(row=row_idx, column=2, value=sheet_name)

        index_ws.column_dimensions["A"].width = 70
        index_ws.column_dimensions["B"].width = 35
        index_ws.freeze_panes = "A2"
        index_ws.auto_filter.ref = f"A1:B{len(table_sheet_index) + 1}"

        if validation_ws is not None:
            # Users have the readable _Domains_Reference sheet; the raw range
            # source should not be edited or accidentally deleted.
            validation_ws.sheet_state = "veryHidden"

        # Create Instructions sheet
        instr_ws = wb.create_sheet(title="_Instructions", index=0)
        instructions = [
            ["BIRD Test Data Template"],
            [""],
            ["How to use this template:"],
            ["1. Each worksheet (tab) represents a BIRD data model table"],
            ["2. The header row contains the field names - do not modify"],
            ["3. Enter your test data in rows 2 and below"],
            ["4. Fields with dropdowns have enumerated values - select from the list"],
            ["5. For dropdown fields, enter just the CODE part (before the colon)"],
            ["6. Date fields should use ISO format: YYYY-MM-DD"],
            ["7. Leave cells empty for NULL values"],
            ['8. Foreign key fields (starting with "the") should contain the unique ID of the referenced record'],
            ["9. See _Table_Index for full BIRD table names when worksheet names are shortened"],
            [""],
            ["To create CSV files for test fixtures:"],
            ["1. Complete your data entry in each table worksheet"],
            ["2. For each worksheet, Save As -> CSV (Comma delimited)"],
            ["3. Name the CSV file after the table (e.g., prty.csv, instrmnt.csv)"],
            ["4. Place CSV files in the scenario folder alongside the existing sql_inserts.sql"],
            [""],
            ["See the _Domains_Reference sheet for a complete list of valid domain values."],
            [""],
            [f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'],
            [f"Tables included: {len(models_to_include)}"],
        ]

        for row_idx, row_data in enumerate(instructions, start=1):
            for col_idx, value in enumerate(row_data, start=1):
                cell = instr_ws.cell(row=row_idx, column=col_idx, value=value)
                if row_idx == 1:
                    cell.font = Font(bold=True, size=14)
                elif value in ["How to use this template:", "To create CSV files for test fixtures:"]:
                    cell.font = Font(bold=True, size=11)

        instr_ws.column_dimensions["A"].width = 100

        # Generate response
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bird_test_data_template_{timestamp}.xlsx"

        response = HttpResponse(
            output.read(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        response["Content-Disposition"] = f'attachment; filename="{filename}"'

        logger.info(f"Generated Excel template with {len(models_to_include)} tables")
        return response

    except Exception as e:
        return _internal_http_error_response(e, "generating Excel template", request)


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
                    "error": (
                        "No BIRD data model tables were discovered. "
                        "Expected generated model file: pybirdai/models/bird_data_model.py"
                    ),
                    "tables": [],
                    "total": 0,
                    "default_tables": DEFAULT_TEST_TABLES,
                },
                status=500,
            )

        return JsonResponse(
            {
                "tables": sorted(all_models.keys()),
                "total": len(all_models),
                "default_tables": DEFAULT_TEST_TABLES,
            }
        )

    except Exception as e:
        return _internal_json_error_response(e, "listing test data template tables", request)


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
        scenario_path = data.get("scenario_path")

        if not scenario_path:
            return JsonResponse({"error": "scenario_path is required"}, status=400)
        try:
            scenario_path = _resolve_project_scenario_path(scenario_path)
        except ValueError:
            return JsonResponse({"error": "Invalid fixture scenario path."}, status=400)

        from pybirdai.utils.datapoint_test_run.sql_to_csv_converter import SQLToCSVConverter

        converter = SQLToCSVConverter(allowed_root=str(settings.BASE_DIR))
        output_files = converter.convert_scenario_in_place(scenario_path)

        return JsonResponse(
            {"success": True, "files": output_files, "message": f"Converted {len(output_files)} tables to CSV"}
        )

    except FileNotFoundError as e:
        logger.info("Scenario SQL fixture not found for CSV conversion: %s", e)
        return JsonResponse({"error": "Scenario SQL fixture file was not found."}, status=404)
    except Exception as e:
        SecureErrorHandler.handle_exception(e, "converting SQL fixtures to CSV", request)
        return JsonResponse({"error": "Failed to convert SQL fixtures to CSV."}, status=500)
