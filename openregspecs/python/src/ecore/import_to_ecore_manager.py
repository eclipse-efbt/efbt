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
from pyecore.ecore import *

from context import Context

from import_ldm_to_ecore import LDMImport
from import_input_layer_to_ecore import InputLayerImport
from import_output_layer_to_ecore import ROLImport
from persistToFile import PersistToFile 

if __name__ == '__main__':
    context = Context()
    importLDM = False
    importIL=True
    importROL=True
    persistToXML = True
    persistToRPMN = True
    context.useSubDomains = True
    context.fileDirectory = 'C:\\Users\\LENOVO\\freebirdtools-develop-dec22\\git\\efbt\\openregspecs\\python\\resourcesfinrep30'
    context.outputDirectory = 'C:\\Users\\LENOVO\\freebirdtools-develop-dec22\\git\\efbt\\openregspecs\\python\\results_finrep_30'
    
    if importLDM:
        LDMImport().doImport(context)
    if importIL:
        InputLayerImport().doImport(context)
    if importROL:
        ROLImport().doImport(context)

        
    persister = PersistToFile()
    persister.saveModelAsXMIFile(context)
    persister.saveModelAsHumanReadable(context)
    #persister.saveModelAsRPMNFile(context)
 