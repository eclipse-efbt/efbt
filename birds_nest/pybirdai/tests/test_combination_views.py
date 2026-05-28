from types import SimpleNamespace

from django.test import SimpleTestCase

from pybirdai.views.core.combination_views import (
    _build_non_reference_display_id,
)


class CombinationViewHelperTests(SimpleTestCase):
    def test_non_reference_display_id_prefers_reference_datapoint_id(self):
        table = SimpleNamespace(code='F_05.01', version='FINREP 3.0-Ind')
        row_ordinate = SimpleNamespace(code='0010', axis_ordinate_id='TABLE_Y_0010')
        column_ordinate = SimpleNamespace(code='0005', axis_ordinate_id='TABLE_X_0005')
        reference_lookup = {
            (('0010',), ('0005',)): 'F_05_01_REF_FINREP_3_0_152586_REF',
        }

        display_id = _build_non_reference_display_id(
            table,
            row_ordinate,
            column_ordinate,
            reference_lookup,
        )

        self.assertEqual(display_id, 'F_05_01_REF_FINREP_3_0_152586_NONREF')

    def test_non_reference_display_id_falls_back_to_readable_coordinates(self):
        table = SimpleNamespace(code='F_05.01', version='FINREP 3.0-Ind')
        row_ordinate = SimpleNamespace(code='0010', axis_ordinate_id='TABLE_Y_0010')
        column_ordinate = SimpleNamespace(code='0005', axis_ordinate_id='TABLE_X_0005')

        display_id = _build_non_reference_display_id(
            table,
            row_ordinate,
            column_ordinate,
            {},
        )

        self.assertEqual(display_id, 'F_05_01_FINREP_3_0_Ind_R0010_C0005_NONREF')
