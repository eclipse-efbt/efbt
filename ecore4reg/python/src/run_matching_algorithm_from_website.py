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

from importers.context import Context
from importers.import_sqldev_il_to_ecore4reg import SQLDeveloperILImport
from importers.import_sdd_to_ecore4reg import SDDImport
from importers.persist_to_file import PersistToFile
from importers.ecore4reg_to_ecore_converter import Ecore4regToEcoreConverter
from importers.standard_matching_queries import StandardMatchingQueries

if __name__ == '__main__':
    context = Context()
    context.persist_to_ecore4reg = True
    context.use_subdomains_in_rol = True
    context.input_from_website = True
    context.set_up_csv_indexes()
    context.file_directory = '/workspaces/efbt/ecore4reg/python/resources_website'
    context.output_directory = '/workspaces/efbt/ecore4reg/python/results_website'
    

    SQLDeveloperILImport().do_import(context)
    SDDImport().doImport(context)
    Ecore4regToEcoreConverter().convert_packages_in_context(context)
    persister = PersistToFile()
    persister.save_model_as_ecore_file(context)
    persister.save_model_as_ecore4reg_file(context)
    persister.save_model_as_xcore_file(context)
    
    persister.save_model_as_json_files(context)

    context.in_scope_file_directory = '/workspaces/efbt/ecore4reg/python/resources_website'
    standardMatchingQueries = StandardMatchingQueries()
    standardMatchingQueries.create_variable_name_to_code_map(context)
    standardMatchingQueries.create_derivable_list(context)
    standardMatchingQueries.query1(context)
    