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
from context.context import Context
from context.sdd_context import SDDContext
from entry_points.website_to_sddmodel import RunWebsiteToSDDModel
from entry_points.sddmodel_to_datamodel import RunSDDModelToDataModel
from entry_points.create_reports_info import RunCreateReports
from persister.persist_to_file import PersistToFile
from process_steps.sddmodel_plus_datamodel_to_outline_transformations.main_catagory_finder import MainCatagoryFinder
from process_steps.sddmodel_plus_datamodel_to_outline_transformations.generation_rule_creator import GenerationRuleCreator


class GenerateGenerationRulesILTestEnv:
    
    def run(self,context,sdd_context):
        
        # we will create an ecore4reg representation of BIRD also
        # use codes not long names for columns
        context.use_codes = True
        # when finding  source columns form the input layer, related
        # to items in the output layer we find columns with matching
        # variables not matching domains
        context.find_variable_with_same_domain = False
        # find the main catagories related with reports.
        MainCatagoryFinder().create_report_to_main_catogory_maps(context,sdd_context,"FINREP_REF", ["3","3.0-Ind","FINREP 3.0-Ind"])
        # find the main catagories related with reports.
        MainCatagoryFinder().create_report_to_main_catogory_maps(context,sdd_context,"AE_REF",["AE 3.2"])
        # create the generation rules
        GenerationRuleCreator().generate_generation_rules(context,sdd_context,"FINREP_REF","EIL")
        GenerationRuleCreator().generate_generation_rules(context,sdd_context,"AE_REF","EIL")
        # save the generated  files in useful formats

if __name__ == '__main__':
    
    sdd_context = SDDContext()
    context = Context()
    context.file_directory = '/workspaces/efbt/bird/birdseed_creator/resources'
    context.output_directory = '/workspaces/efbt/bird/birdseed_creator/results'  
    sdd_context.file_directory = '/workspaces/efbt/bird/birdseed_creator/resources'
    sdd_context.output_directory = '/workspaces/efbt/bird/birdseed_creator/results'    
    context.input_layer_code_has_EIL_postfix = True  
    RunWebsiteToSDDModel().run(sdd_context)
    RunSDDModelToDataModel().run(context,sdd_context)
    GenerateGenerationRulesILTestEnv().run(context,sdd_context)
    persister = PersistToFile()
    persister.save_model_as_regdna_file(context)
    persister.persist_generation_transformations(context)
    persister.persist_generation_transformations_to_csv(context)
    
