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
Created on 22 Jan 2022

@author: Neil
'''


from context import Context

from import_ldm_to_ecore4reg import LDMImport
from import_input_layer_to_ecore4reg import InputLayerImport
from import_output_layer_to_ecore4reg import ROLImport
from import_finrep_vtl import ImportFinrepVTL
from persistToFile import PersistToFile
from ecore4regToEcoreConverter import Ecore4regToEcoreConverter

if __name__ == '__main__':
    context = Context()
    importLDM = False
    importIL = True
    importROL = True
    importFinrepVTL = False
    persistToXCoreL = True
    context.persistVTLComments = True
    context.useSubDomains = False
    context.fileDirectory = 'C:\\Users\\LENOVO\\freebirdtools-develop-mar23\\git\\efbt\\ecore4reg\\python\\resources'
    context.outputDirectory = 'C:\\Users\\LENOVO\\freebirdtools-develop-mar23\\git\\efbt\\ecore4reg\\python\\results'
    context.addExecutableStubs = True
    context.addLogicPackages = True

    if importLDM:
        LDMImport().doImport(context)
    if importIL:
        InputLayerImport().doImport(context)
    if importROL:
        ROLImport().doImport(context)
    if importFinrepVTL:
        ImportFinrepVTL().doImport(context)

    Ecore4regToEcoreConverter().convertPackagesInContext(context)

    persister = PersistToFile()
    persister.saveModelAsEcoreFile(context)
    persister.saveModelAsEcore4RegFile(context)

    if importFinrepVTL:
        persister.saveModelAsXMIFile(context)
    else:
        persister.saveModelAsJSONFiles(context)
