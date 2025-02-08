# coding=UTF-8#
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDE-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#
'''
Created on 22 Jan 2022

@author: Neil
'''

import os
import csv
from pybirdai.utils.utils import Utils

from pybirdai.regdna import ELAttribute, ELClass, ELEnum
from pybirdai.regdna import ELEnumLiteral, ELReference



class BIRDMetaDataModelToRegDNA(object):
    '''
    Documentation for SQLDeveloperILImport
    '''

    