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
from pickle import TRUE, FALSE
'''
Created on 22 Jan 2022

@author: Neil
'''
from open_reg_specs import *

from context import Context

from import_ldm_to_xcorel import LDMImport
from import_input_layer_to_xcorel import InputLayerImport
from import_output_layer_to_xcorel import ROLImport
from import_finrep_vtl import ImportFinrepVTL
from persistToFile import PersistToFile 

if __name__ == '__main__':
    context = Context()
    importLDM = False
    importIL=True
    importROL=True
    importFinrepVTL=True
    persistToXML = True
    persistToXCoreL = True
    context.persistVTLComments=True
    context.useSubDomains = False
    context.fileDirectory = '/workspaces/efbt/openregspecs/python/resourcesfinrep28'
    context.outputDirectory = '/workspaces/efbt/openregspecs/python/resultsfinrep28'
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
        
    persister = PersistToFile()
    persister.saveModelAsXMIFile(context)
    persister.saveModelAsXCoreLFile(context)
 