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

from importers.context  import Context

from importers.import_sqldev_ldm_to_ecore4reg import SQLDevLDMImport
from importers.persist_to_file import PersistToFile
from importers.ecore4reg_to_ecore_converter import Ecore4regToEcoreConverter
from importers.traverser import SubtypeExploder

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
    traverser.traverse(context)
    
    
