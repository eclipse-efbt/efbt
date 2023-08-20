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

from context.context  import Context

from importers.import_sqldev_ldm_to_ecore4reg import SQLDevLDMImport
from persister.persist_to_file import PersistToFile
from utils.ecore4reg_to_ecore_converter import Ecore4regToEcoreConverter
from utils.traverser import SubtypeExploder

if __name__ == '__main__':
    context = Context()

    context.persist_to_ecore4reg = True
    context.use_subdomains_in_rol = False
    context.file_directory = 'C:\\Users\\LENOVO\\freebirdtools-develop-jun23\\git\\efbt\\ecore4reg\\python\\resources'
    context.output_directory = 'C:\\Users\\LENOVO\\freebirdtools-develop-jun23\\git\\efbt\\ecore4reg\\python\\results'
    
    SQLDevLDMImport().do_import(context)

    Ecore4regToEcoreConverter().convert_packages_in_context(context)
    persister = PersistToFile()
    persister.save_model_as_ecore_file(context)
    persister.save_model_as_ecore4reg_file(context)
    persister.save_model_as_json_files(context)
    traverser = SubtypeExploder()
    # for all the root types get the full and summary breakdowns
    traverser.traverse(context,'Security_and_exchange_tradable_derivative',False)
    traverser.traverse(context,'Security_and_exchange_tradable_derivative',True)
    traverser.traverse(context,'Instrument',False)
    traverser.traverse(context,'Instrument',True)
    traverser.traverse(context,'Instrument_role',False)
    traverser.traverse(context,'Instrument_role',True)
    traverser.traverse(context,'Non_financial_asset_and_non_financial_liability',False)
    traverser.traverse(context,'Non_financial_asset_and_non_financial_liability',True)
    traverser.traverse(context,'Cash_on_hand',False)
    traverser.traverse(context,'Cash_on_hand',True)
    traverser.traverse(context,'Security_or_exchange_tradable_derivative_position',False)
    traverser.traverse(context,'Security_or_exchange_tradable_derivative_position',True)
    traverser.traverse(context,'Long_security_position_Prudential_portfolio_assignment',False)
    traverser.traverse(context,'Long_security_position_Prudential_portfolio_assignment',True)
    traverser.traverse(context,'Long_security_position_Prudential_Portfolio_assignment_Accounting_classification_for_financial_assets_assignment',False)
    traverser.traverse(context,'Long_security_position_Prudential_Portfolio_assignment_Accounting_classification_for_financial_assets_assignment',True)


    
    
