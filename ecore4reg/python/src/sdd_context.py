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

from pyecore.ecore import EPackage
from ecore4reg import ELDataType, ELPackage, ModuleList, ViewModule, VTLModule, WorkflowModule


class SDDContext(object):
    '''
    Documentation for Context
    '''
    # variables to configure the behaviour


    # the directory where we get our input files
    file_directory = ""
    # the directory where we save our outputs.
    output_directory = ""


    variableToDomainMap = {}
    variableToLongNamesMap = {}
    # For the reference output layers we record a map between domains
    # and domain names
    domainToDomainNameMap = {}
    # For the reference output layers we record a map between members ids
    # andtheir containing domains
    memberIDToDomainMap = {}
    # For the reference output layers we record a map between members ids
    # and their names
    memberIDToMemberNameMap = {}
    # For the reference output layers we record a map between members ids
    # and their codes
    memberIDToMemberCodeMap = {}

    subDomainToMemberListMap = {}
    subDomainIDToDomainID = {}

    variableSetToVariableMap = {}


