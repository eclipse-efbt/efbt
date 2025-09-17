# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#

import json
import sqlite3
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, date

@dataclass
class EntityData:
    """Represents data for a specific entity"""
    entity_type: str
    entity_id: str
    attributes: Dict[str, Any] = field(default_factory=dict)
    relationships: Dict[str, str] = field(default_factory=dict)

@dataclass
class FixtureTemplate:
    """Template for generating SQL fixtures"""
    template_id: str
    cell_suffix: str
    scenario_name: str
    expected_value: int
    entities: List[EntityData] = field(default_factory=list)
    custom_sql: List[str] = field(default_factory=list)

class SQLFixtureBuilder:
    """
    Advanced SQL fixture builder that understands the BIRD database schema
    and can generate complex relational data
    """

    def __init__(self, db_path: str = None):
        self.db_path = db_path or "db.sqlite3"
        self.logger = logging.getLogger(self.__class__.__name__)

        # Common default values based on existing fixtures
        self.default_values = {
            'test_id': '1',
            'ACCNTNG_CNSLDTN_LVL': '1',
            'ACCNTNG_STNDRD': '2',
            'RFRNC_DT': '2018-09-30 00:00:00',
            'RPRTNG_AGNT_ID': 'BLZ10',
            'DT_RFRNC': '2018-09-30 00:00:00',
            'INSTTTNL_SCTR': 'S11',
            'ECNMC_ACTVTY': '23_32',
            'PRTY_TYP': '31',
            'PRTY_TYP_ADDRS': '7'
        }

    def get_next_available_row_id(self) -> int:
        """
        Query the database to find the next available row ID across all tables.
        This prevents UNIQUE constraint violations when inserting fixtures.

        Returns:
            int: The next safe row ID to use
        """
        try:
            import sqlite3

            # List of tables that use rowid in our fixtures
            tables_with_rowid = [
                'pybirdai_balance_sheet_recognised_financial_asset_instrument_type',
                'pybirdai_blnc_sht_rcgnsd_fnncl_asst_instrmnt',
                'pybirdai_blnc_sht_rcgnsd_fnncl_asst_instrmnt_ifrs',
                'pybirdai_crdtr',
                'pybirdai_entty_rl',
                'pybirdai_entty_trnsctn_rl',
                'pybirdai_financial_asset_instrument_type',
                'pybirdai_fnncl_asst_instrmnt',
                'pybirdai_fnncl_asst_instrmnt_drvd_dt',
                'pybirdai_fnncl_cntrct',
                'pybirdai_instrmnt',
                'pybirdai_instrmnt_entty_rl_assgnmnt',
                'pybirdai_instrmnt_rl',
                'pybirdai_instrmnt_rsltng_drctly_fnncl_cntrct',
                'pybirdai_instrument_type_by_origin',
                'pybirdai_instrument_type_by_product',
                'pybirdai_lgl_prsn',
                'pybirdai_ln_excldng_rprchs_agrmnt',
                'pybirdai_party_type',
                'pybirdai_prty',
                'pybirdai_prty_rl'
            ]

            max_row_id = 0

            with sqlite3.connect(self.db_path) as connection:
                cursor = connection.cursor()

                for table in tables_with_rowid:
                    try:
                        # Check if table exists first
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
                        if cursor.fetchone() is None:
                            continue  # Skip non-existent tables

                        # Get max rowid from this table
                        cursor.execute(f"SELECT COALESCE(MAX(rowid), 0) FROM {table}")
                        table_max = cursor.fetchone()[0] or 0
                        max_row_id = max(max_row_id, table_max)

                    except sqlite3.Error as e:
                        # Table might not exist or have rowid, continue
                        self.logger.warning(f"Could not query max rowid from table {table}: {e}")
                        continue

            # Start from next available ID, with a buffer of 1000 to be safe
            next_id = max_row_id + 1000
            self.logger.debug(f"Next available row ID: {next_id} (max found: {max_row_id})")
            return next_id

        except Exception as e:
            self.logger.error(f"Error querying next available row ID: {e}")
            # Fallback to timestamp-based ID if database query fails
            import time
            fallback_id = int(time.time()) % 1000000  # Use last 6 digits of timestamp
            self.logger.warning(f"Using fallback row ID: {fallback_id}")
            return fallback_id

    def generate_fixture_template_for_cell(self, template_id: str, cell_suffix: str, expected_value: int) -> FixtureTemplate:
        """Generate a fixture template based on cell requirements"""

        # Generate unique IDs for this fixture
        instrument_id = f"123321_{datetime.now().strftime('%Y-%m-%d')}_BLZ10"
        party_ids = [
            f"78451209_{datetime.now().strftime('%Y-%m-%d')}_BLZ10",
            f"BLZ10_{datetime.now().strftime('%Y-%m-%d')}_BLZ10",
            f"63829150_{datetime.now().strftime('%Y-%m-%d')}_BLZ10"
        ]

        entities = []

        # Financial Asset Instrument
        entities.append(EntityData(
            entity_type="financial_asset_instrument",
            entity_id=instrument_id,
            attributes={
                "CRRYNG_AMNT": expected_value,
                "ACCNTNG_CLSSFCTN": "6",
                "ACCRD_INTRST": 191200,
                "ACCMLTD_IMPRMNT": 0,
                "RCGNTN_STTS": None,
                "ACCMLTD_NGTV_VL_ADJSTMNT_CR": 0,
                "FV_CHNGS_HDG_ACCNTNG": 0,
                "GRSS_CRRYNG_AMNT_E_INTRST": 83300000,
                "IMPRMNT_STTS": "23",
                "INTL_IMPRMNT_STTS": "23",
                "PRDNTL_PRTFL_TYP": "2"
            }
        ))

        # Loan excluding repurchase agreement
        entities.append(EntityData(
            entity_type="loan_excluding_repurchase_agreement",
            entity_id=instrument_id,
            attributes={
                "CRRNCY": "EUR",
                "LN_TYP": "1022",
                "NMNL_AMNT": 850000,
                "SNDCTN_SB_PRTCPTN_MMBR_INSTRMNT_INDCTR": "2",
                "ELGBL_CNTRL_BNK_FNDNG_INDCTR": "2",
                "DT_LGL_FNL_MTRTY": "2022-12-01 00:00:00",
                "LTGTN_STTS": "3",
                "LN_AND_ADVNC_TYP": "28"
            }
        ))

        # Financial Asset Instrument Data
        entities.append(EntityData(
            entity_type="financial_asset_instrument_data",
            entity_id=instrument_id,
            attributes={
                "ACCMLTD_PRTL_WRTFFS": 0,
                "ACCMLTD_TTL_WRTFFS": 0,
                "DFLT_STTS_DRVD": "14",
                "ENCMBRD_ASST_INDCTR": "1",
                "FNNCL_ASST_INSTRMNT_ID": "123321",
                "FNNCL_ASST_INSTRMNT_RL_TYP": "3",
                "PRFRMNG_STTS": "11"
            }
        ))

        # Parties (debtor, creditor, protection provider)
        party_types = ["debtor", "creditor", "protection_provider"]
        party_roles = ["28", "17", "24"]

        for i, (party_id, party_type, role) in enumerate(zip(party_ids, party_types, party_roles)):
            entities.append(EntityData(
                entity_type="party",
                entity_id=party_id,
                attributes={
                    "INSTTTNL_SCTR": "S11" if i > 0 else "S122_A",
                    "ECNMC_ACTVTY": "23_32" if i > 0 else "64_1",
                    "PRTY_RL_TYP": role,
                    "LGL_PRSN_TYP": "13",
                    "PLLNG_EFFCT_INDCTR": "2"
                }
            ))

        # Add relationships
        entities.append(EntityData(
            entity_type="instrument_entity_role_assignment",
            entity_id=f"{instrument_id}_{party_ids[0]}",
            attributes={
                "INSTRMNT_ENTTY_RL_ASSGNMNT_TYP": "6"
            },
            relationships={
                "instrument": instrument_id,
                "party": party_ids[0]
            }
        ))

        return FixtureTemplate(
            template_id=template_id,
            cell_suffix=cell_suffix,
            scenario_name="loan_and_guarantee_scenario_1",
            expected_value=expected_value,
            entities=entities
        )

    def build_sql_from_template(self, template: FixtureTemplate) -> List[str]:
        """Convert fixture template to SQL INSERT statements"""
        sql_statements = [
            "-- Generated fixture data",
            f"-- Template: {template.template_id}",
            f"-- Cell: {template.cell_suffix}",
            f"-- Scenario: {template.scenario_name}",
            f"-- Expected Value: {template.expected_value}",
            ""
        ]

        # Start from next available row ID to prevent UNIQUE constraint violations
        row_id_counter = self.get_next_available_row_id()

        # Process entities in dependency order
        for entity in template.entities:
            if entity.entity_type == "financial_asset_instrument":
                sql_statements.extend(self._build_financial_asset_instrument_sql(entity, row_id_counter))
                row_id_counter += 5  # Reserve space for related records

            elif entity.entity_type == "loan_excluding_repurchase_agreement":
                sql_statements.extend(self._build_loan_sql(entity, row_id_counter))
                row_id_counter += 3

            elif entity.entity_type == "financial_asset_instrument_data":
                sql_statements.extend(self._build_instrument_data_sql(entity, row_id_counter))
                row_id_counter += 1

            elif entity.entity_type == "party":
                sql_statements.extend(self._build_party_sql(entity, row_id_counter))
                row_id_counter += 4

            elif entity.entity_type == "instrument_entity_role_assignment":
                sql_statements.extend(self._build_relationship_sql(entity, row_id_counter))
                row_id_counter += 1

        # Add custom SQL
        sql_statements.extend(template.custom_sql)

        return sql_statements

    def _build_financial_asset_instrument_sql(self, entity: EntityData, row_id: int) -> List[str]:
        """Build SQL for financial asset instrument and related records"""
        statements = []

        # Balance sheet recognised financial asset instrument type
        statements.append(
            f"INSERT INTO pybirdai_balance_sheet_recognised_financial_asset_instrument_type"
            f"(rowid,test_id,Balance_sheet_recognised_financial_asset_instrument_type_uniqueID) "
            f"VALUES({row_id},'{self.default_values['test_id']}','{entity.entity_id}');"
        )

        # Main instrument record
        crryng_amnt = entity.attributes.get('CRRYNG_AMNT', 83491250)
        accntng_clssfctn = entity.attributes.get('ACCNTNG_CLSSFCTN', '6')

        statements.append(
            f"INSERT INTO pybirdai_blnc_sht_rcgnsd_fnncl_asst_instrmnt"
            f"(rowid,financial_asset_instrument_type_ptr_id,ACCNTNG_CLSSFCTN,ACCRD_INTRST,"
            f"ACCMLTD_IMPRMNT,BLNC_SHT_RCGNSD_FFNCL_ASST_INSTRMNT_FR_VL_TYP,"
            f"BLNC_SHT_RCGNSD_FNNCL_ASST_INSTRMNT_TYP,RCGNTN_STTS,CRRYNG_AMNT,"
            f"ACCMLTD_NGTV_VL_ADJSTMNT_CR,FV_CHNGS_HDG_ACCNTNG,GRSS_CRRYNG_AMNT_E_INTRST,"
            f"IMPRMNT_STTS,INTL_IMPRMNT_STTS,ACCMLTD_NGTV_VL_ADJSTMNT_MR,PRDNTL_PRTFL_TYP) "
            f"VALUES({row_id + 1},'{entity.entity_id}','{accntng_clssfctn}',"
            f"{entity.attributes.get('ACCRD_INTRST', 191200)},{entity.attributes.get('ACCMLTD_IMPRMNT', 0)},"
            f"1,NULL,NULL,{crryng_amnt},{entity.attributes.get('ACCMLTD_NGTV_VL_ADJSTMNT_CR', 0)},"
            f"{entity.attributes.get('FV_CHNGS_HDG_ACCNTNG', 0)},{entity.attributes.get('GRSS_CRRYNG_AMNT_E_INTRST', 83300000)},"
            f"'{entity.attributes.get('IMPRMNT_STTS', '23')}','{entity.attributes.get('INTL_IMPRMNT_STTS', '23')}',"
            f"NULL,'{entity.attributes.get('PRDNTL_PRTFL_TYP', '2')}');"
        )

        # IFRS data
        statements.append(
            f"INSERT INTO pybirdai_blnc_sht_rcgnsd_fnncl_asst_instrmnt_ifrs"
            f"(rowid,balance_sheet_recognised_financial_asset_instrument_type_ptr_id,"
            f"FVO_DSGNTN,HLD_SL_INDCTR,LW_CRDT_RSK_INDCTR) "
            f"VALUES({row_id + 2},'{entity.entity_id}','2','2','1');"
        )

        return statements

    def _build_loan_sql(self, entity: EntityData, row_id: int) -> List[str]:
        """Build SQL for loan records"""
        statements = []

        # Loan excluding repurchase agreement
        statements.append(
            f"INSERT INTO pybirdai_ln_excldng_rprchs_agrmnt"
            f"(rowid,ln_excldng_rprchs_agrmnt_and_advnce_ptr_id,CRRNCY,LN_TYP,NMNL_AMNT,"
            f"SNDCTN_SB_PRTCPTN_MMBR_INSTRMNT_INDCTR) "
            f"VALUES({row_id},'{entity.entity_id}','{entity.attributes.get('CRRNCY', 'EUR')}',"
            f"'{entity.attributes.get('LN_TYP', '1022')}',"
            f"{entity.attributes.get('NMNL_AMNT', 850000)},"
            f"'{entity.attributes.get('SNDCTN_SB_PRTCPTN_MMBR_INSTRMNT_INDCTR', '2')}');"
        )

        return statements

    def _build_instrument_data_sql(self, entity: EntityData, row_id: int) -> List[str]:
        """Build SQL for financial asset instrument derived data"""
        statements = []

        statements.append(
            f"INSERT INTO pybirdai_fnncl_asst_instrmnt_drvd_dt"
            f"(rowid,test_id,FNNCL_ASST_INSTRMNT_DRVD_DT_uniqueID,ACCMLTD_PRTL_WRTFFS,"
            f"ACCMLTD_TTL_WRTFFS,DFLT_STTS_DRVD,ENCMBRD_ASST_INDCTR,"
            f"FNNCL_ASST_INSTRMNT_ACCNTNG_CNSLDTN_LVL,FNNCL_ASST_INSTRMNT_ACCNTNG_STNDRD,"
            f"FNNCL_ASST_INSTRMNT_ID,FNNCL_ASST_INSTRMNT_RFRNC_DT,"
            f"FNNCL_ASST_INSTRMNT_RPRTNG_AGNT_ID,FNNCL_ASST_INSTRMNT_RL_TYP,PRFRMNG_STTS) "
            f"VALUES({row_id},'{self.default_values['test_id']}','{entity.entity_id}',"
            f"{entity.attributes.get('ACCMLTD_PRTL_WRTFFS', 0)},"
            f"{entity.attributes.get('ACCMLTD_TTL_WRTFFS', 0)},"
            f"'{entity.attributes.get('DFLT_STTS_DRVD', '14')}',"
            f"'{entity.attributes.get('ENCMBRD_ASST_INDCTR', '1')}',"
            f"'{self.default_values['ACCNTNG_CNSLDTN_LVL']}',"
            f"'{self.default_values['ACCNTNG_STNDRD']}',"
            f"'{entity.attributes.get('FNNCL_ASST_INSTRMNT_ID', '123321')}',"
            f"'{self.default_values['RFRNC_DT']}',"
            f"'{self.default_values['RPRTNG_AGNT_ID']}',"
            f"'{entity.attributes.get('FNNCL_ASST_INSTRMNT_RL_TYP', '3')}',"
            f"'{entity.attributes.get('PRFRMNG_STTS', '11')}');"
        )

        return statements

    def _build_party_sql(self, entity: EntityData, row_id: int) -> List[str]:
        """Build SQL for party records"""
        statements = []

        # Party type
        statements.append(
            f"INSERT INTO pybirdai_party_type(rowid,test_id,Party_type_uniqueID) "
            f"VALUES({row_id},'{self.default_values['test_id']}','{entity.entity_id}');"
        )

        # Main party record
        statements.append(
            f"INSERT INTO pybirdai_prty"
            f"(rowid,test_id,PRTY_uniqueID,INSTTTNL_SCTR,PRTY_ACCNTNG_CNSLDTN_LVL,"
            f"PRTY_ACCNTNG_STNDRD,PRTY_ID,PRTY_RFRNC_DT,PRTY_RPRTNG_AGNT_ID,PRTY_TYP,PRTY_TYP_ADDRS,ECNMC_ACTVTY) "
            f"VALUES({row_id + 1},'{self.default_values['test_id']}','{entity.entity_id}',"
            f"'{entity.attributes.get('INSTTTNL_SCTR', 'S11')}',"
            f"'{self.default_values['ACCNTNG_CNSLDTN_LVL']}',"
            f"'{self.default_values['ACCNTNG_STNDRD']}',"
            f"'{entity.entity_id.split('_')[0]}',"
            f"'{self.default_values['RFRNC_DT']}',"
            f"'{self.default_values['RPRTNG_AGNT_ID']}',"
            f"'{self.default_values['PRTY_TYP']}',"
            f"'{self.default_values['PRTY_TYP_ADDRS']}',"
            f"'{entity.attributes.get('ECNMC_ACTVTY', '23_32')}');"
        )

        # Legal person
        statements.append(
            f"INSERT INTO pybirdai_lgl_prsn(rowid,party_type_ptr_id,LGL_PRSN_TYP,PLLNG_EFFCT_INDCTR) "
            f"VALUES({row_id + 2},'{entity.entity_id}','{entity.attributes.get('LGL_PRSN_TYP', '13')}',"
            f"'{entity.attributes.get('PLLNG_EFFCT_INDCTR', '2')}');"
        )

        return statements

    def _build_relationship_sql(self, entity: EntityData, row_id: int) -> List[str]:
        """Build SQL for relationship records"""
        statements = []

        statements.append(
            f"INSERT INTO pybirdai_instrmnt_entty_rl_assgnmnt"
            f"(rowid,test_id,INSTRMNT_ENTTY_RL_ASSGNMNT_uniqueID,INSTRMNT_ENTTY_RL_ASSGNMNT_TYP) "
            f"VALUES({row_id},'{self.default_values['test_id']}','{entity.entity_id}',"
            f"'{entity.attributes.get('INSTRMNT_ENTTY_RL_ASSGNMNT_TYP', '6')}');"
        )

        return statements

    def load_configuration_from_json(self, config_file: str) -> Optional[FixtureTemplate]:
        """Load fixture configuration from JSON file"""
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)

            # Convert JSON to FixtureTemplate
            entities = []
            for entity_data in config_data.get('entities', {}).get('data', []):
                entities.append(EntityData(
                    entity_type=entity_data['entity_type'],
                    entity_id=entity_data['entity_id'],
                    attributes=entity_data.get('attributes', {}),
                    relationships=entity_data.get('relationships', {})
                ))

            return FixtureTemplate(
                template_id=config_data['template_id'],
                cell_suffix=config_data['cell_suffix'],
                scenario_name=config_data['scenario_name'],
                expected_value=config_data['expected_value'],
                entities=entities,
                custom_sql=config_data.get('custom_sql', [])
            )

        except Exception as e:
            self.logger.error(f"Failed to load configuration from {config_file}: {e}")
            return None