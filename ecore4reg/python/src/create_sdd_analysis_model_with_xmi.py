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

from context.sdd_context import SDDContext
from importers.import_sdd_to_analysis_model import ImportSDD
from persister.persist_to_file import PersistToFile

if __name__ == '__main__':
    sdd_context = SDDContext()
    sdd_context.file_directory = '/workspaces/efbt/ecore4reg/python/resources'
    sdd_context.output_directory = '/workspaces/efbt/ecore4reg/python/results'
    ImportSDD().import_sdd(sdd_context)
    persister = PersistToFile()
    persister.save_analysis_model_as_csv(sdd_context)
