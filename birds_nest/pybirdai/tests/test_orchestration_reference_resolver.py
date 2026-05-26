from django.test import SimpleTestCase

from pybirdai.process_steps.pybird.orchestration import (
    OrchestrationWithLineage,
    _get_logic_prefix_candidates,
)


class OrchestrationReferenceResolverTests(SimpleTestCase):
    def test_corep_logic_prefix_is_discovered_from_filter_code_file(self):
        reference = (
            "EBA_COREP_C_07_00_a_4_0_0__eba_qEC_qx1_"
            "Credit_or_Counterparty_Risk_Exposure_Data_Table"
        )

        self.assertEqual(
            _get_logic_prefix_candidates(reference)[0],
            "EBA_COREP_C_07_00_a_4_0_0__eba_qEC_qx1",
        )

    def test_corep_reference_instantiates_from_discovered_logic_module(self):
        reference = (
            "EBA_COREP_C_07_00_a_4_0_0__eba_qEC_qx1_"
            "Credit_or_Counterparty_Risk_Exposure_Data_Table"
        )

        table = OrchestrationWithLineage.createObjectFromReferenceType(reference)

        self.assertIsNotNone(table)
        self.assertEqual(table.__class__.__name__, reference)
