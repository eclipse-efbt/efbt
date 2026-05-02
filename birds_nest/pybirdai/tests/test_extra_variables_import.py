import tempfile
from pathlib import Path

from django.test import TestCase

from pybirdai.models.bird_meta_data_model import DOMAIN, MAINTENANCE_AGENCY, VARIABLE
from pybirdai.views.core.csv_views import load_variables_from_csv_file


class ExtraVariablesImportTests(TestCase):
    def test_existing_extra_variable_does_not_block_missing_variables(self):
        agency = MAINTENANCE_AGENCY.objects.create(
            maintenance_agency_id="REF",
            code="REF",
            name="REF",
        )
        existing_domain = DOMAIN.objects.create(
            domain_id="ENTTY_RL_TYP",
            name="ENTTY_RL_TYP",
            description="ENTTY_RL_TYP",
            data_type="String",
            maintenance_agency_id=agency,
        )
        VARIABLE.objects.create(
            maintenance_agency_id=agency,
            variable_id="PRTY_RL_TYP",
            code="PRTY_RL_TYP",
            name="Old name",
            description="Old description",
            domain_id=existing_domain,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "extra_variables.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "MAINTENANCE_AGENCY_ID,VARIABLE_ID,CODE,NAME,DOMAIN_ID,DESCRIPTION,PRIMARY_CONCEPT,IS_DECOMPOSED",
                        "REF,TYP_INSTRMNT,TYP_INSTRMNT,TYP_INSTRMNT,TYP_INSTRMNT,Type of instrument,,",
                        "REF,PRTY_RL_TYP,PRTY_RL_TYP,PRTY_RL_TYP,ENTTY_RL_TYP,Party role type,,",
                    ]
                ),
                encoding="utf-8",
            )

            loaded_count = load_variables_from_csv_file(str(csv_path))

        self.assertEqual(loaded_count, 2)
        self.assertTrue(VARIABLE.objects.filter(variable_id="TYP_INSTRMNT").exists())
        party_role = VARIABLE.objects.get(variable_id="PRTY_RL_TYP")
        self.assertEqual(party_role.name, "PRTY_RL_TYP")
        self.assertEqual(party_role.description, "Party role type")
        self.assertEqual(party_role.maintenance_agency_id_id, "REF")

    def test_variable_like_agency_values_default_to_ref(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "extra_variables.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "MAINTENANCE_AGENCY_ID,VARIABLE_ID,CODE,NAME,DOMAIN_ID,DESCRIPTION,PRIMARY_CONCEPT,IS_DECOMPOSED",
                        "TYP_CLLTRL,TYP_CLLTRL,TYP_CLLTRL,TYP_CLLTRL,TYP_PRTCTN,Collateral type,,",
                    ]
                ),
                encoding="utf-8",
            )

            load_variables_from_csv_file(str(csv_path))

        variable = VARIABLE.objects.get(variable_id="TYP_CLLTRL")
        self.assertEqual(variable.maintenance_agency_id_id, "REF")
        self.assertFalse(
            MAINTENANCE_AGENCY.objects.filter(maintenance_agency_id="TYP_CLLTRL").exists()
        )
