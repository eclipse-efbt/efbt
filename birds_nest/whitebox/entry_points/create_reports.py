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

import django
from django.apps import AppConfig




class RunCreateReports(AppConfig):
    
    path = '/workspaces/efbt/bird/birdseed_creator/birds_nest'
    def ready(self):
        from whitebox.sdd_models import MAINTENANCE_AGENCY
        
       
        from whitebox.process_steps.database_to_sdd_model.import_database_to_sdd_model import ImportDatabaseToSDDModel
        from whitebox.process_steps.website_to_sddmodel.import_website_to_sdd_model_django import ImportWebsiteToSDDModel
        from whitebox.context.sdd_context_django import SDDContext
        from whitebox.context.context import Context
        from whitebox.persister.persist_to_file import PersistToFile
        from whitebox.process_steps.sqldeveloper_import.import_sqldev_ldm_to_regdna import SQLDevLDMImport
        from whitebox.process_steps.output_layers.create_output_layers import CreateOutputLayers
        from whitebox.process_steps.report_filters.create_report_filters import CreateReportFilters
        sdd_context = SDDContext()
        sdd_context.file_directory = '/workspaces/efbt/birdsnest/resources'
        sdd_context.output_directory = '/workspaces/efbt/birdsnest/results'
        context = Context()
        context.file_directory = '/workspaces/efbt/birdsnest/resources'
        context.output_directory = '/workspaces/efbt/birdsnest/results'
        if context.load_sdd_from_website:            
            ImportWebsiteToSDDModel().import_sdd(sdd_context)
        else:
            ImportDatabaseToSDDModel().import_sdd(sdd_context)

        SQLDevLDMImport.do_import(self,context)
        CreateOutputLayers().create_output_layers( context,sdd_context,"FINREP_REF","3.0")
        CreateReportFilters().create_report_filters(context,sdd_context,"FINREP_REF","BIRD","ELDM","3.0")
        persister = PersistToFile()
        persister.save_model_as_regdna_file(context)
        persister.persist_cell_based_reports(context)
        

if __name__ == '__main__':
    django.setup()
    RunCreateReports('whitebox','birds_nest').ready()
    
    

    
    

 

    