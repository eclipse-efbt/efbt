# coding=UTF-8#
# Copyright (c) 2020 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#

from birdai.sdd_models import *
from django.apps import apps

from django.db.models.fields import CharField,DateTimeField,BooleanField,FloatField,BigIntegerField
import math
from django.db.models import Sum
from birds_nest.birdai.ldm_models_ldm import (
    DLT_SNSTVTY,
    VG_SNSTVTY,
    CRVTR,
    RSK_FAC_SA,
    FX_RSK_FACTR_SA,
    GIRR_RSK_FCTR,
    EQTY_RSK_FCTR,
    CMMDTY_RSK_FCTR_FR_STNDRDSD_APPRCH,
    CRDT_SPRD_RSK_NN_SCRTSTN_RSK_FCTR_STNDRDSD_APPRCH,
    SCRTSTN_NN_CRRLTN_TRDNG_PRTFL_RSK_FCTR_FR_STNDRDSD_APPRCH,
    SCRTSTN_CRRLTN_TRDNG_PRTFL_RSK_FCTR_STNDRDSD_APPRCH,
    FNDMNTL_RVW_TRDNG_BK_STNDRD_APPRCH_RSK_MSR,
)

class ImportInputModel(object):
    """
    A class for creating reference domains, variables, and cubes in the SDD model.
    """

    # Define risk weights and correlations based on FRTB guidelines.
# These are placeholder values; replace them with actual figures from the regulation.
    RISK_WEIGHTS = {
        'GIRR': {
            'delta': 0.005,
            'vega': 0.21,
            'curvature': 0.005,
        },
        'FX': {
            'delta': 0.04,
            'vega': 0.18,
            'curvature': 0.04,
        },
        'Equity': {
            'delta': 0.12,
            'vega': 0.21,
            'curvature': 0.12,
        },
        'Commodity': {
            'delta': 0.15,
            'vega': 0.18,
            'curvature': 0.15,
        },
        # Add other risk classes as needed.
    }

    # Correlations within buckets (placeholder values)
    INTRA_BUCKET_CORRELATIONS = {
        'GIRR': 0.99,
        'FX': 0.5,
        'Equity': 0.8,
        'Commodity': 0.6,
        # Add other risk classes as needed.
    }

    # Inter-bucket correlations (placeholder values)
    INTER_BUCKET_CORRELATIONS = {
        ('GIRR', 'FX'): 0.2,
        ('GIRR', 'Equity'): 0.15,
        ('GIRR', 'Commodity'): 0.1,
        # Add other pairs as needed.
    }
    def import_input_model(sdd_context, context):
        """
        Create reference domains, variables, and cubes in the SDD model.

        Args:
            sdd_context: The SDD context object containing dictionaries for
                         storing created elements.
            context: The context object containing configuration settings.
        """
        ImportInputModel.calculate_total_capital()

    def get_sensitivities(risk_class):
        # Fetch sensitivities based on risk class
        delta_sensitivities = DLT_SNSTVTY.objects.filter(
            Risk_measure_type__Fundamental_review_of_the_trading_book_standard_approach_risk_measure__RSK_CLSS=risk_class
        ).values('RSK_FCTR_ID', 'DLT_SNSTVTY')

        vega_sensitivities = VG_SNSTVTY.objects.filter(
            Risk_measure_type__Fundamental_review_of_the_trading_book_standard_approach_risk_measure__RSK_CLSS=risk_class
        ).values('RSK_FCTR_ID', 'VG_SNSTVTY')

        curvature_sensitivities = CRVTR.objects.filter(
            Risk_measure_type__Fundamental_review_of_the_trading_book_standard_approach_risk_measure__RSK_CLSS=risk_class
        ).values('RSK_FCTR_ID', 'CRVTR_VLTN_DFFRNC')

        return delta_sensitivities, vega_sensitivities, curvature_sensitivities

    def apply_risk_weights(sensitivities, risk_weight):
        # Apply risk weights to sensitivities
        weighted_sensitivities = {}
        for s in sensitivities:
            risk_factor_id = s['RSK_FCTR_ID']
            sensitivity_value = s.get('DLT_SNSTVTY') or s.get('VG_SNSTVTY') or s.get('CRVTR_VLTN_DFFRNC')
            weighted_value = sensitivity_value * risk_weight
            weighted_sensitivities[risk_factor_id] = weighted_value
        return weighted_sensitivities

    def aggregate_within_bucket(weighted_sensitivities, intra_bucket_correlation):
        # Aggregate sensitivities within a bucket using intra-bucket correlations
        weighted_values = list(weighted_sensitivities.values())
        aggregate = 0.0
        for i in range(len(weighted_values)):
            for j in range(len(weighted_values)):
                aggregate += (weighted_values[i] * weighted_values[j] * (intra_bucket_correlation if i != j else 1))
        return math.sqrt(aggregate)

    def calculate_risk_class_capital(risk_class):
        delta_sensitivities, vega_sensitivities, curvature_sensitivities = get_sensitivities(risk_class)

        # Apply risk weights
        delta_weighted = ImportInputModel.apply_risk_weights(delta_sensitivities, ImportInputModel.RISK_WEIGHTS[risk_class]['delta'])
        vega_weighted = ImportInputModel.apply_risk_weights(vega_sensitivities, ImportInputModel.RISK_WEIGHTS[risk_class]['vega'])
        curvature_weighted = ImportInputModel.apply_risk_weights(curvature_sensitivities, ImportInputModel.RISK_WEIGHTS[risk_class]['curvature'])

        # Aggregate within buckets
        delta_aggregate = ImportInputModel.aggregate_within_bucket(delta_weighted, ImportInputModel.INTRA_BUCKET_CORRELATIONS[risk_class])
        vega_aggregate = ImportInputModel.aggregate_within_bucket(vega_weighted, ImportInputModel.INTRA_BUCKET_CORRELATIONS[risk_class])
        curvature_aggregate = ImportInputModel.aggregate_within_bucket(curvature_weighted, ImportInputModel.INTRA_BUCKET_CORRELATIONS[risk_class])

        # Total capital charge for the risk class
        capital_charge = delta_aggregate + vega_aggregate + curvature_aggregate
        print ("capital charge = " + capital_charge)
        return capital_charge

    def calculate_total_capital():
        risk_classes = ['GIRR', 'FX', 'Equity', 'Commodity']  # Add other risk classes as needed
        risk_class_capitals = {}

        for rc in risk_classes:
            capital = ImportInputModel.calculate_risk_class_capital(rc)
            risk_class_capitals[rc] = capital
            print(f"Capital charge for {rc}: {capital:.2f}")

        # Aggregate across risk classes using inter-bucket correlations
        total_capital = 0.0
        for rc1 in risk_classes:
            for rc2 in risk_classes:
                correlation = ImportInputModel.INTER_BUCKET_CORRELATIONS.get((rc1, rc2), 0.0)
                if rc1 == rc2:
                    correlation = 1.0
                total_capital += (risk_class_capitals[rc1] * risk_class_capitals[rc2] * correlation)

        total_capital = math.sqrt(total_capital)
        print(f"Total capital requirement: {total_capital:.2f}")
        return total_capital

    if __name__ == "__main__":
        calculate_total_capital()


        

