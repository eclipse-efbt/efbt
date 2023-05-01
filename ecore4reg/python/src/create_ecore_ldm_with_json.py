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

from context import Context

from import_ldm_to_ecore4reg import LDMImport
from persist_to_file import PersistToFile
from ecore4reg_to_ecore_converter import Ecore4regToEcoreConverter

if __name__ == '__main__':
    context = Context()

    context.persist_to_ecore4reg = True
    context.use_subdomains = False
    context.file_directory = '/workspaces/efbt/ecore4reg/python/resources'
    context.output_directory = '/workspaces/efbt/ecore4reg/python/results'

    LDMImport().do_import(context)

    Ecore4regToEcoreConverter().convert_packages_in_ontext(context)
    persister = PersistToFile()
    persister.saveModelAsEcoreFile(context)
    persister.saveModelAsEcore4RegFile(context)
    persister.saveModelAsJSONFiles(context)
