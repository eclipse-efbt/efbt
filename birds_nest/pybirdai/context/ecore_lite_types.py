# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
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
from pybirdai.regdna import ELDataType



class EcoreLiteTypes(object):
    '''
    Set up the main types for ecore lite
    '''
    # we create the main 5 'primitive' data types
    e_string = ELDataType()
    e_string.name = "String"

    e_double = ELDataType()
    e_double.name = "double"

    e_int = ELDataType()
    e_int.name = "int"

    e_date = ELDataType()
    e_date.name = "Date"

    e_boolean = ELDataType()
    e_boolean.name = "boolean"
