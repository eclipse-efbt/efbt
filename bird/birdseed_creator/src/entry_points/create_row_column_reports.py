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
# This script creates an analysis model from an SDD file and saves it as a CSV filegit add 


from entry_points.create_reports_info import RunCreateReports
from entry_points.website_to_sddmodel import RunWebsiteToSDDModel
from sddmodel_to_datamodel import RunSDDModelToDataModel 
from context.context import Context
from context.sdd_context import SDDContext
from persister.persist_to_file import PersistToFile
from process_steps.report_filters.translate_to_row_column_reports import TranslateToRowColumnReports


class RunCreateRowColumnReports:
    
    def run(self,context,sdd_context):
        TranslateToRowColumnReports().translate_to_row_column_reports(context,sdd_context,"FINREP_REF","RC","RC")
        TranslateToRowColumnReports().translate_to_row_column_reports(context,sdd_context,"FINREP_REF","RC","EIL")
        TranslateToRowColumnReports().translate_to_row_column_reports(context,sdd_context,"FINREP_REF","RC","ELDM")
        TranslateToRowColumnReports().translate_to_row_column_reports(context,sdd_context,"AE_REF","RC","RC")
        TranslateToRowColumnReports().translate_to_row_column_reports(context,sdd_context,"AE_REF","RC","EIL")
        TranslateToRowColumnReports().translate_to_row_column_reports(context,sdd_context,"AE_REF","RC","ELDM")
                                                                      
        
        
if __name__ == '__main__':
    sdd_context = SDDContext()
    context = Context()
    context.file_directory = '/workspaces/efbt/bird/birdseed_creator/resources'
    context.output_directory = '/workspaces/efbt/bird/birdseed_creator/results'  
    sdd_context.file_directory = '/workspaces/efbt/bird/birdseed_creator/resources'
    sdd_context.output_directory = '/workspaces/efbt/bird/birdseed_creator/results'            
    RunWebsiteToSDDModel().run(sdd_context)
    RunSDDModelToDataModel().run(context,sdd_context)
    RunCreateReports().run(context,sdd_context)
    RunCreateRowColumnReports().run(context,sdd_context)
    persister = PersistToFile()
    persister.save_model_as_regdna_file(context)
    persister.save_model_as_xmi_file(context)
    persister.persist_cell_based_reports(context)
    persister.persist_row_column_based_reports(context)