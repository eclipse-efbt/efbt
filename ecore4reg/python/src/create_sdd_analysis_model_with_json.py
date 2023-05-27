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

from importers.sdd_context import SDDContext
from importers.import_sdd_to_analysis_model import ImportSDD


if __name__ == '__main__':
    sdd_context = SDDContext()

    sdd_context.file_directory = 'C:\\Users\\LENOVO\\git\\efbt_ecore4reg\\ecore4reg\\python\\resources_website'
    sdd_context.output_directory = 'C:\\Users\\LENOVO\\git\\efbt_ecore4reg\\ecore4reg\\python\\results_website\\sdd'
    sdd_context.input_from_website = True
    sdd_context.set_up_csv_indexes()
    ImportSDD().import_sdd(sdd_context)

    #Ecore4regToEcoreConverter().convertPackagesInContext(context)
    persister = PersistToFile()
    persister.save_analysis_model_as_json_files(context)
