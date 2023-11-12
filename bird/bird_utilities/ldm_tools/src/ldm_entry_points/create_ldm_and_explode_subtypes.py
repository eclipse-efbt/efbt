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
'''
Created on 25 April 2022
'''

from ldm_context.context  import Context
from ldm_processing.import_sqldev_ldm_to_regdna import SQLDevLDMImport
from ldm_processing.traverser import SubtypeExploder
from ldm_processing.enrich_ldm_with_il_links_from_fe import InputLayerLinkEnricher
from ldm_persister.persist_to_file import PersistToFile

if __name__ == '__main__':
    context = Context()

    context.persist_to_ecore4reg = True
    context.file_directory = '/workspaces/efbt/bird/bird_utilities/ldm_tools/resources'
    context.output_directory = '/workspaces/efbt/bird/bird_utilities/ldm_tools/results' 
    
    SQLDevLDMImport().do_import(context)
    InputLayerLinkEnricher().enrich_with_links_to_input_layer_columns(context)
    persister = PersistToFile()
    persister.save_model_as_regdna_file(context)
    traverser = SubtypeExploder()
    # for all the root types get the full and summary breakdowns
    traverser.traverse(context,'Security_and_exchange_tradable_derivative',False)
    traverser.traverse(context,'Security_and_exchange_tradable_derivative',True)
    traverser.traverse(context,'Credit_facility',False)
    traverser.traverse(context,'Credit_facility',True)
    traverser.traverse(context,'Credit_facility_Entity_role_assignment',False)
    traverser.traverse(context,'Credit_facility_Entity_role_assignment',True)
    traverser.traverse(context,'Instrument',True)
    traverser.traverse(context,'Instrument',False)
    traverser.traverse(context,'Instrument_role',False)
    traverser.traverse(context,'Instrument_role',True)
    traverser.traverse(context,'Non_financial_asset_and_non_financial_liability',False)
    traverser.traverse(context,'Non_financial_asset_and_non_financial_liability',True)
    traverser.traverse(context,'Cash_on_hand',False)
    traverser.traverse(context,'Cash_on_hand',True)
    traverser.traverse(context,'Party',False)
    traverser.traverse(context,'Party',True)
    traverser.traverse(context,'Security_or_exchange_tradable_derivative_position',False)
    traverser.traverse(context,'Security_or_exchange_tradable_derivative_position',True)
    traverser.traverse(context,'Long_security_position_Prudential_portfolio_assignment',False)
    traverser.traverse(context,'Long_security_position_Prudential_portfolio_assignment',True)
    traverser.traverse(context,'Long_security_position_Prudential_Portfolio_assignment_Accounting_classification_for_financial_assets_assignment',False)
    traverser.traverse(context,'Long_security_position_Prudential_Portfolio_assignment_Accounting_classification_for_financial_assets_assignment',True)


    
    
