from types import SimpleNamespace

from django.test import SimpleTestCase

from pybirdai.views.core.combination_views import (
    _get_mapping_cube_candidates,
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

    def test_mapping_cube_candidates_strip_eba_framework_prefixes(self):
        cube = SimpleNamespace(
            cube_id='EBA_COREP_C_07_00_a_4_0_0__eba_qEC_qx1',
            name='C_07.00.a - Z axis : Equity exposures',
            code=None,
        )
        table = SimpleNamespace(
            table_id='EBA_COREP_C_07_00_a_4_0_0__eba_qEC_qx1',
            code='C_07.00.a__eba_qEC_qx1',
            version='4_0_0',
        )

        candidates = _get_mapping_cube_candidates(cube, table, table)

        self.assertIn('M_C_07_00_a_4_0_0__eba_qEC_qx1', candidates)
