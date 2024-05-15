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



from entry_points.website_to_sddmodel import RunWebsiteToSDDModel
from entry_points.sddmodel_to_datamodel import RunSDDModelToDataModel
from context.context import Context
from context.sdd_context import SDDContext
from persister.persist_to_file import PersistToFile
from process_steps.report_filters.translate_combinations_to_report_filters import CombinationsToReportFilters


class RunCreateReports:
    
    def run(self,context,sdd_context):
        # temporary commnets to speed up testing
        CombinationsToReportFilters().translate_combinations_to_report_filters(context,sdd_context,"FINREP_REF","RC","RC")
        #CombinationsToReportFilters().translate_combinations_to_report_filters(context,sdd_context,"FINREP_REF","RC","EIL")
        CombinationsToReportFilters().translate_combinations_to_report_filters(context,sdd_context,"FINREP_REF","RC","LDM")
        #CombinationsToReportFilters().translate_combinations_to_report_filters(context,sdd_context,"AE_REF","RC","RC")
        #CombinationsToReportFilters().translate_combinations_to_report_filters(context,sdd_context,"AE_REF","RC","EIL")
        #CombinationsToReportFilters().translate_combinations_to_report_filters(context,sdd_context,"AE_REF","RC","LDM")
        
if __name__ == '__main__':
    sdd_context = SDDContext()
    context = Context()
    context.file_directory = 'C:\\Users\\neil\\freebirdtools-develop_may\\git\\efbt\\bird\\birdseed_creator\\resources_ae'
    context.output_directory = 'C:\\Users\\neil\\freebirdtools-develop_may\\git\\efbt\\bird\\birdseed_creator\\results'  
    sdd_context.file_directory = 'C:\\Users\\neil\\freebirdtools-develop_may\\git\\efbt\\bird\\birdseed_creator\\resources_ae'
    sdd_context.output_directory = 'C:\\Users\\neil\\freebirdtools-develop_may\\git\\efbt\\bird\\birdseed_creator\\results'     
    RunWebsiteToSDDModel().run(sdd_context)
    RunSDDModelToDataModel().run(context,sdd_context)
    RunCreateReports().run(context,sdd_context)
    persister = PersistToFile()
    persister.save_model_as_regdna_file(context)
    persister.save_model_as_xmi_file(context)
    persister.persist_cell_based_reports(context)
    #persister.create_example_reports(context)