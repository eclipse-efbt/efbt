import io
import re
import unittest
import zipfile
from unittest.mock import patch
from xml.etree import ElementTree

from django.test import RequestFactory, SimpleTestCase
from openpyxl import load_workbook

from pybirdai.views.test_data_template_views import (
    EXCEL_MAX_SHEET_TITLE_LENGTH,
    _make_unique_excel_sheet_title,
    export_bird_excel_template,
)


class ExcelSheetTitleTests(unittest.TestCase):
    def test_sheet_titles_remain_unique_after_truncation(self):
        used_titles = set()

        first = _make_unique_excel_sheet_title("a" * 40, used_titles)
        second = _make_unique_excel_sheet_title("A" * 40, used_titles)
        third = _make_unique_excel_sheet_title("bad/name:*?[]\\'title", used_titles)

        self.assertEqual(len(first), EXCEL_MAX_SHEET_TITLE_LENGTH)
        self.assertLessEqual(len(second), EXCEL_MAX_SHEET_TITLE_LENGTH)
        self.assertNotEqual(first.casefold(), second.casefold())
        self.assertNotRegex(third, r"[\\/*?:\[\]]")


class ExcelTestDataTemplateTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    @patch("pybirdai.utils.datapoint_test_run.test_data_template_utils.extract_domain_dictionaries")
    @patch("pybirdai.utils.datapoint_test_run.test_data_template_utils.get_model_fields_metadata")
    @patch("pybirdai.utils.datapoint_test_run.test_data_template_utils.get_bird_model_classes")
    def test_export_uses_excel_safe_titles_and_range_backed_validations(
        self,
        get_models,
        get_fields,
        get_domains,
    ):
        model_names = [
            "LONG_MODEL_NAME_WITH_A_COLLIDING_PREFIX_ALPHA",
            "LONG_MODEL_NAME_WITH_A_COLLIDING_PREFIX_BETA",
        ]
        get_models.return_value = {name: object() for name in model_names}
        get_fields.return_value = [
            {
                "name": "STATUS",
                "python_type": "str",
                "is_foreign_key": False,
                "related_model": None,
            }
        ]
        get_domains.return_value = {
            "STATUS": {str(code): f'Description_with_a_comma,_quote_"_and_value_{code}' for code in range(1, 25)}
        }

        request = self.factory.get(
            "/api/test-data/excel-template/",
            {"include_all": "true"},
        )
        response = export_bird_excel_template(request)

        self.assertEqual(response.status_code, 200)
        workbook = load_workbook(io.BytesIO(response.content))

        self.assertEqual(
            len(workbook.sheetnames),
            len({title.casefold() for title in workbook.sheetnames}),
        )
        for title in workbook.sheetnames:
            self.assertLessEqual(len(title), EXCEL_MAX_SHEET_TITLE_LENGTH)
            self.assertIsNone(re.search(r"[\\/*?:\[\]]", title))

        table_index = workbook["_Table_Index"]
        indexed_rows = list(
            table_index.iter_rows(
                min_row=2,
                max_row=1 + len(model_names),
                values_only=True,
            )
        )
        self.assertEqual([row[0] for row in indexed_rows], model_names)
        self.assertNotEqual(indexed_rows[0][1], indexed_rows[1][1])
        self.assertTrue(all(len(row[1]) <= EXCEL_MAX_SHEET_TITLE_LENGTH for row in indexed_rows))

        validation_sheet = workbook["_Validation_Lists"]
        self.assertEqual(validation_sheet.sheet_state, "veryHidden")
        self.assertIn("BIRD_Domain_1", workbook.defined_names)
        self.assertIn(
            "'_Validation_Lists'!",
            workbook.defined_names["BIRD_Domain_1"].attr_text,
        )

        for _, sheet_name in indexed_rows:
            validations = list(workbook[sheet_name].data_validations.dataValidation)
            self.assertEqual(len(validations), 1)
            self.assertEqual(validations[0].formula1, "=BIRD_Domain_1")

        # Every XML relationship and content part should remain well-formed.
        with zipfile.ZipFile(io.BytesIO(response.content)) as package:
            xml_parts = [name for name in package.namelist() if name.endswith((".xml", ".rels"))]
            self.assertTrue(xml_parts)
            for part_name in xml_parts:
                ElementTree.fromstring(package.read(part_name))
