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

class EcoreLiteTypes(object):
   # we create the main 5 'primitive' data types
    xString = ELDataType()
    xString.name = "String"

    xDouble = ELDataType()
    xDouble.name = "double"

    xInt = ELDataType()
    xInt.name = "int"

    xDate = ELDataType()
    xDate.name = "Date"

    xBoolean = ELDataType()
    xBoolean.name = "boolean"