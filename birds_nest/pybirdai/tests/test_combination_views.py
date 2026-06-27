from types import SimpleNamespace

from django.test import SimpleTestCase

from pybirdai.views.core.combination_views import (
    _apply_mapped_non_reference_ordinate_items_to_reference_layout,
    _build_non_reference_display_id,
    _build_reference_combination_display_id,
    _get_mapping_cube_candidates,
    _map_non_reference_ordinate_items_to_reference,
)


class CombinationViewHelperTests(SimpleTestCase):
    def test_non_reference_display_id_shortens_reference_datapoint_id(self):
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

        self.assertEqual(display_id, '152586_NONREF')

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

        self.assertEqual(display_id, 'R0010_C0005_NONREF')

    def test_non_reference_display_id_prefers_table_cell_label(self):
        table = SimpleNamespace(code='F_05.01', version='FINREP 3.0-Ind')
        row_ordinate = SimpleNamespace(code='0010', axis_ordinate_id='TABLE_Y_0010')
        column_ordinate = SimpleNamespace(code='0005', axis_ordinate_id='TABLE_X_0005')
        table_cell_lookup = {
            (('0010',), ('0005',)): '0010_0005',
        }

        display_id = _build_non_reference_display_id(
            table,
            row_ordinate,
            column_ordinate,
            {},
            table_cell_lookup,
        )

        self.assertEqual(display_id, '0010_0005_NONREF')

    def test_reference_combination_display_id_prefers_cell_name(self):
        cell = SimpleNamespace(name='0010_0005', cell_id='long_cell_id')

        display_id = _build_reference_combination_display_id(
            'F_05_01_REF_FINREP_3_0_152586_REF',
            cell=cell,
        )

        self.assertEqual(display_id, '0010_0005')

    def test_reference_combination_display_id_shortens_known_patterns(self):
        self.assertEqual(
            _build_reference_combination_display_id(
                'CUBE_PREFIX_EBA_12345__eba_qEC_qx1_REF'
            ),
            'EBA_12345__eba_qEC_qx1_REF',
        )
        self.assertEqual(
            _build_reference_combination_display_id(
                'F_05_01_REF_FINREP_3_0_152586_REF'
            ),
            '152586_REF',
        )

    def test_mapping_cube_candidates_include_reference_table_version_form(self):
        cube = SimpleNamespace(
            cube_id='C_07_00_a__eba_qEC_qx1_EBA_COREP_4_0_0_CUBE',
            name='Reference cube for C_07.00.a__eba_qEC_qx1',
            code='C_07_00_a__eba_qEC_qx1_CUBE',
        )
        reference_table = SimpleNamespace(
            table_id='EBA_COREP_C_07_00_a_4_0_0__eba_qEC_qx1',
            code='C_07.00.a__eba_qEC_qx1',
            version='4_0_0',
        )

        candidates = _get_mapping_cube_candidates(cube, reference_table=reference_table)

        self.assertIn('M_C_07_00_a__eba_qEC_qx1_REF_4_0_0', candidates)

    def test_reference_layout_ordinate_items_are_mapped_from_non_reference_layout(self):
        reference_layout = {
            'row_headers': [
                {'ordinate_id': 'REF_TABLE_Y_0010', 'ordinate_items': []},
            ],
            'column_headers': {
                'levels': [[
                    {'ordinate_id': 'REF_TABLE_X_0020', 'ordinate_items': []},
                ]],
            },
        }
        non_reference_layout = {
            'row_headers': [
                {
                    'ordinate_id': 'EBA_TABLE_Y_0010',
                    'ordinate_items': [
                        {
                            'variable_id': 'EBA_qEEF',
                            'variable_name': 'Prudential portfolio',
                            'member_id': 'eba_qPL_qx2000',
                            'member_name': 'Banking and trading book',
                        },
                    ],
                },
            ],
            'column_headers': {
                'levels': [[
                    {
                        'ordinate_id': 'EBA_TABLE_X_0020',
                        'ordinate_items': [
                            {
                                'variable_id': 'EBA_qIJJ',
                                'variable_name': 'Exposure value',
                                'member_id': None,
                                'member_name': None,
                            },
                        ],
                    },
                ]],
            },
        }
        mapping_lineage = {
            'mappings': [
                {
                    'mapping_id': 'MAP_PORTFOLIO',
                    'name': 'Prudential portfolio mapping',
                    'source_variables': [{'variable_id': 'EBA_qEEF'}],
                    'target_variables': [{'variable_id': 'PRUDENTIAL_PORTFOLIO', 'friendly_label': 'Prudential portfolio'}],
                    'member_rows': [
                        {
                            'source_items': [
                                {
                                    'variable': {'variable_id': 'EBA_qEEF'},
                                    'member': {'member_id': 'eba_qPL_qx2000'},
                                },
                            ],
                            'target_items': [
                                {
                                    'variable': {
                                        'variable_id': 'PRUDENTIAL_PORTFOLIO',
                                        'friendly_label': 'Prudential portfolio',
                                    },
                                    'member': {
                                        'member_id': 'PRUDENTIAL_PORTFOLIO_QX2000',
                                        'friendly_label': 'Banking and trading book',
                                    },
                                },
                            ],
                        },
                    ],
                },
                {
                    'mapping_id': 'MAP_EXPOSURE',
                    'name': 'Exposure value mapping',
                    'source_variables': [{'variable_id': 'EBA_qIJJ'}],
                    'target_variables': [{'variable_id': 'EXPOSURE_VALUE', 'friendly_label': 'Exposure value'}],
                    'member_rows': [],
                },
            ],
        }

        mapped_layout = _apply_mapped_non_reference_ordinate_items_to_reference_layout(
            reference_layout,
            non_reference_layout,
            mapping_lineage,
        )

        self.assertEqual(
            mapped_layout['row_headers'][0]['ordinate_items'][0]['variable_id'],
            'PRUDENTIAL_PORTFOLIO',
        )
        self.assertEqual(
            mapped_layout['row_headers'][0]['ordinate_items'][0]['member_id'],
            'PRUDENTIAL_PORTFOLIO_QX2000',
        )
        self.assertEqual(
            mapped_layout['column_headers']['levels'][0][0]['ordinate_items'][0]['variable_id'],
            'EXPOSURE_VALUE',
        )

    def test_reference_layout_ordinate_items_use_specific_multi_source_mapping_once(self):
        reference_layout = {
            'row_headers': [
                {'ordinate_id': 'REF_TABLE_Y_0010', 'ordinate_items': []},
            ],
            'column_headers': {'levels': []},
        }
        non_reference_layout = {
            'row_headers': [
                {
                    'ordinate_id': 'EBA_TABLE_Y_0010',
                    'ordinate_items': [
                        {
                            'variable_id': 'EBA_APL',
                            'variable_name': 'Accounting portfolio',
                            'member_id': 'EBA_PL_x20',
                            'member_name': 'Portfolio member',
                        },
                        {
                            'variable_id': 'EBA_BAS',
                            'variable_name': 'Base',
                            'member_id': 'EBA_BA_x6',
                            'member_name': 'Assets',
                        },
                    ],
                },
            ],
            'column_headers': {'levels': []},
        }
        mapping_lineage = {
            'mappings': [
                {
                    'mapping_id': 'MAP_APL',
                    'name': 'Accounting portfolio only',
                    'source_variables': [{'variable_id': 'EBA_APL'}],
                    'target_variables': [
                        {'variable_id': 'ACCNTNG_CLSSFCTN', 'is_output_layer_variable': True},
                        {'variable_id': 'HLD_SL_INDCTR', 'is_output_layer_variable': True},
                        {'variable_id': 'SBJCT_IMPRMNT_INDCTR', 'is_output_layer_variable': True},
                    ],
                    'member_rows': [
                        {
                            'source_items': [
                                {
                                    'variable': {'variable_id': 'EBA_APL'},
                                    'member': {'member_id': 'EBA_PL_x20'},
                                },
                            ],
                            'target_items': [
                                {
                                    'variable': {
                                        'variable_id': 'ACCNTNG_CLSSFCTN',
                                        'is_output_layer_variable': True,
                                    },
                                    'member': {'member_id': 'ACCNTNG_CLSSFCTN_20'},
                                },
                                {
                                    'variable': {
                                        'variable_id': 'HLD_SL_INDCTR',
                                        'is_output_layer_variable': True,
                                    },
                                    'member': {'member_id': 'HLD_SL_INDCTR_2'},
                                },
                                {
                                    'variable': {
                                        'variable_id': 'SBJCT_IMPRMNT_INDCTR',
                                        'is_output_layer_variable': True,
                                    },
                                    'member': {'member_id': 'SBJCT_IMPRMNT_INDCTR_0'},
                                },
                            ],
                        },
                    ],
                },
                {
                    'mapping_id': 'MAP_BAS_APL',
                    'name': 'Accounting portfolio and base',
                    'source_variables': [
                        {'variable_id': 'EBA_APL'},
                        {'variable_id': 'EBA_BAS'},
                    ],
                    'target_variables': [
                        {'variable_id': 'ACCNTNG_CLSSFCTN', 'is_output_layer_variable': True},
                        {'variable_id': 'HLD_SL_INDCTR', 'is_output_layer_variable': True},
                        {'variable_id': 'TYP_ACCNTNG_ITM', 'is_output_layer_variable': True},
                        {'variable_id': 'NOT_IN_THIS_OUTPUT_LAYER', 'is_output_layer_variable': False},
                    ],
                    'member_rows': [
                        {
                            'source_items': [
                                {
                                    'variable': {'variable_id': 'EBA_APL'},
                                    'member': {'member_id': 'EBA_PL_x20'},
                                },
                                {
                                    'variable': {'variable_id': 'EBA_BAS'},
                                    'member': {'member_id': 'EBA_BA_x6'},
                                },
                            ],
                            'target_items': [
                                {
                                    'variable': {
                                        'variable_id': 'ACCNTNG_CLSSFCTN',
                                        'is_output_layer_variable': True,
                                    },
                                    'member': {'member_id': 'ACCNTNG_CLSSFCTN_20'},
                                },
                                {
                                    'variable': {
                                        'variable_id': 'HLD_SL_INDCTR',
                                        'is_output_layer_variable': True,
                                    },
                                    'member': {'member_id': 'HLD_SL_INDCTR_2'},
                                },
                                {
                                    'variable': {
                                        'variable_id': 'TYP_ACCNTNG_ITM',
                                        'is_output_layer_variable': True,
                                    },
                                    'member': {'member_id': 'TYP_ACCNTNG_ITM_6'},
                                },
                                {
                                    'variable': {
                                        'variable_id': 'NOT_IN_THIS_OUTPUT_LAYER',
                                        'is_output_layer_variable': False,
                                    },
                                    'member': {'member_id': 'NOT_IN_THIS_OUTPUT_LAYER_1'},
                                },
                            ],
                        },
                    ],
                },
            ],
        }

        mapped_layout = _apply_mapped_non_reference_ordinate_items_to_reference_layout(
            reference_layout,
            non_reference_layout,
            mapping_lineage,
        )

        mapped_items = mapped_layout['row_headers'][0]['ordinate_items']
        self.assertEqual(
            [item['variable_id'] for item in mapped_items],
            ['ACCNTNG_CLSSFCTN', 'HLD_SL_INDCTR', 'TYP_ACCNTNG_ITM'],
        )
        self.assertEqual(
            {item['mapping_id'] for item in mapped_items},
            {'MAP_BAS_APL'},
        )

    def test_reference_layout_does_not_leak_unmapped_non_reference_source_variables(self):
        reference_layout = {
            'row_headers': [
                {'ordinate_id': 'REF_TABLE_Y_0010', 'ordinate_items': []},
            ],
            'column_headers': {'levels': []},
        }
        non_reference_layout = {
            'row_headers': [
                {
                    'ordinate_id': 'EBA_TABLE_Y_0010',
                    'ordinate_items': [
                        {
                            'variable_id': 'EBA_BAS',
                            'variable_name': 'Base',
                            'member_id': 'EBA_BA_x6',
                            'member_name': 'Assets',
                        },
                        {
                            'variable_id': 'EBA_CPS',
                            'variable_name': 'Counterparty sector',
                            'member_id': 'EBA_CT_x10',
                            'member_name': 'Credit institutions',
                        },
                    ],
                },
            ],
            'column_headers': {'levels': []},
        }
        mapping_lineage = {
            'reference_variable_rows': [
                {'variable': {'variable_id': 'INSTTTNL_SCTR'}, 'links': []},
            ],
            'mappings': [
                {
                    'mapping_id': 'MAP_CPS',
                    'name': 'Counterparty sector',
                    'source_variables': [{'variable_id': 'EBA_CPS'}],
                    'target_variables': [
                        {'variable_id': 'INSTTTNL_SCTR', 'is_output_layer_variable': True},
                    ],
                    'member_rows': [
                        {
                            'source_items': [
                                {
                                    'variable': {'variable_id': 'EBA_CPS'},
                                    'member': {'member_id': 'EBA_CT_x10'},
                                },
                            ],
                            'target_items': [
                                {
                                    'variable': {
                                        'variable_id': 'INSTTTNL_SCTR',
                                        'is_output_layer_variable': True,
                                    },
                                    'member': {'member_id': 'INSTTTNL_SCTR_10'},
                                },
                            ],
                        },
                    ],
                },
            ],
        }

        mapped_layout = _apply_mapped_non_reference_ordinate_items_to_reference_layout(
            reference_layout,
            non_reference_layout,
            mapping_lineage,
        )

        mapped_items = mapped_layout['row_headers'][0]['ordinate_items']
        self.assertEqual(
            [item['variable_id'] for item in mapped_items],
            ['INSTTTNL_SCTR'],
        )

    def test_non_reference_metric_items_use_metric_mapping_lookup(self):
        mapped_items = _map_non_reference_ordinate_items_to_reference(
            [
                {
                    'variable_id': 'EBA_mi53',
                    'variable_name': 'Carrying amount',
                    'member_id': None,
                    'member_name': None,
                },
            ],
            [],
            frozenset({'CRRYNG_AMNT'}),
            {
                'EBA_mi53': [
                    {
                        'variable_id': 'CRRYNG_AMNT',
                        'variable_name': 'Carrying amount',
                        'member_id': None,
                        'member_name': None,
                        'mapping_id': 'DPM_mi53',
                    },
                ],
            },
        )

        self.assertEqual(
            [item['variable_id'] for item in mapped_items],
            ['CRRYNG_AMNT'],
        )
