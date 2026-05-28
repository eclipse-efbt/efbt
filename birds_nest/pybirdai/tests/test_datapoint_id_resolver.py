from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from pybirdai.services.cell_execution_service import CellExecutionService
from pybirdai.services.datapoint_id_resolver import (
    build_datapoint_id,
    build_datapoint_id_candidates,
)
from pybirdai.services.table_rendering_service import TableRenderingService


class DatapointIdResolverTests(SimpleTestCase):
    def test_corep_cell_id_datapoint_is_preferred_over_generated_combination(self):
        table = SimpleNamespace(
            table_id="EBA_COREP_C_07_00_a_4_0_0__eba_qEC_qx1",
            code="C_07.00.a__eba_qEC_qx1",
            version="4_0_0",
        )
        cell = SimpleNamespace(
            cell_id="EBA_343332__eba_qEC_qx1",
            table_cell_combination_id=(
                "EBA_COREP_C_07_00_a_4_0_0__eba_qEC_qx1_COMB_20260523103550_0129"
            ),
            table_id=table,
        )

        expected = (
            "EBA_COREP_C_07_00_a_4_0_0__eba_qEC_qx1"
            "_EBA_343332__eba_qEC_qx1"
        )

        self.assertEqual(next(iter(build_datapoint_id_candidates(cell))), expected)
        with patch(
            "pybirdai.services.datapoint_id_resolver._report_cell_class_names",
            return_value=frozenset({f"Cell_{expected}"}),
        ):
            self.assertEqual(build_datapoint_id(cell, validate_class=True), expected)
            self.assertEqual(CellExecutionService._build_datapoint_id(cell), expected)
            self.assertEqual(TableRenderingService._build_datapoint_id(cell, table), expected)

    def test_legacy_reference_suffix_can_follow_eba_combination(self):
        table = SimpleNamespace(
            table_id="F_04_01_REF_FINREP_3_0",
            code="F_04.01_REF",
            version="FINREP 3.0",
        )
        cell = SimpleNamespace(
            cell_id="CELL_1",
            table_cell_combination_id="EBA_11112_REF",
            table_id=table,
        )

        self.assertEqual(
            next(iter(build_datapoint_id_candidates(cell))),
            "F_04_01_REF_FINREP_3_0_11112_REF",
        )

    def test_legacy_finrep_datapoint_id_still_resolves_first(self):
        table = SimpleNamespace(
            table_id="F_04_01_REF_FINREP_3_0",
            code="F_04.01_REF",
            version="FINREP 3.0",
        )
        cell = SimpleNamespace(
            cell_id="CELL_1",
            table_cell_combination_id="EBA_11112",
            table_id=table,
        )

        expected = "F_04_01_REF_FINREP_3_0_11112_REF"

        self.assertEqual(next(iter(build_datapoint_id_candidates(cell))), expected)
        with patch(
            "pybirdai.services.datapoint_id_resolver._report_cell_class_names",
            return_value=frozenset({f"Cell_{expected}"}),
        ):
            self.assertEqual(build_datapoint_id(cell, validate_class=True), expected)
