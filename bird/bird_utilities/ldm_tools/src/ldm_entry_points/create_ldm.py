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
from ldm_persister.persist_to_file import PersistToFile


if __name__ == '__main__':
    context = Context()

    context.persist_to_regdna = True
    context.file_directory = 'C:\\Users\\LENOVO\\git\\efbt_the_directories2\\bird\\bird_utilities\\ldm_tools\\resources'
    context.output_directory = 'C:\\Users\\LENOVO\\git\\efbt_the_directories2\\bird\\bird_utilities\\ldm_tools\\results' 
    
    SQLDevLDMImport().do_import(context)
    persister = PersistToFile()
    persister.save_model_as_regdna_file(context)

