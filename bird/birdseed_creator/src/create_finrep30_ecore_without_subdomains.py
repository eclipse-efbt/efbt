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


from context.context import Context
from importers.import_sdd_to_regdna import SDDImport
from persister.persist_to_file import PersistToFile
from utils.regdna_to_ecore_converter import regdnaToEcoreConverter



if __name__ == '__main__':
    context = Context()
    context.persist_to_regdna = True
    context.use_codes = False
    context.use_subdomains = False
    context.file_directory = '/workspaces/efbt/regdna/python/resources'
    context.output_directory = '/workspaces/efbt/regdna/python/results'
    SDDImport().do_import(context)
    regdnaToEcoreConverter().convert_packages_in_context(context)
    persister = PersistToFile()
    persister.save_model_as_ecore_file(context)
    persister.save_model_as_regdna_file(context)
    persister.save_model_as_json_files(context)

    