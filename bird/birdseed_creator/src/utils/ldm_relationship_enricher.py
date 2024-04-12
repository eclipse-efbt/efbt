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
from regdna import ELAttribute, ELClass, ELReference,ELAnnotation,ELStringToStringMapEntry
import csv
from utils.utils import Utils
import os

class LDMRelationshipEnricher(object):
    '''
    Enrich a model which can from the SDD website,
    Create references for each relationship, mark the references
    with the separated list of target keys and separated list of source keys
    For each foreign key, annotate that attribute as FK.
    For each primary key, annotate that as private key.
    foreign cubes should have the relationship to primary cube
    '''
    def enrich(self, context):
        
        pass