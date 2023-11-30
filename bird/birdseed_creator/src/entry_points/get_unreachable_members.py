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
from context.sdd_context import SDDContext
from process_steps.website_to_sddmodel.import_website_to_sdd_model import ImportWebsiteToSDDModel
from utils.unavailable_input_layer_members import MemberFinder
from entry_points.website_to_sddmodel import RunWebsiteToSDDModel 

class RunMemberFinder:

    def run(self,sdd_context,reporting_framework,reporting_framework_version):
        MemberFinder().get_unreachable_members(sdd_context, 
                                                reporting_framework,
                                                reporting_framework_version )

if __name__ == '__main__':
    sdd_context = SDDContext()
    sdd_context.file_directory = '/workspaces/efbt/bird/birdseed_creator/resources'
    sdd_context.output_directory = '/workspaces/efbt/bird/birdseed_creator/results'      
    reporting_framework = "FINREP"
    reporting_framework_version = ["FINREP 3.0","3.0-Ind"]
    RunWebsiteToSDDModel().run(sdd_context )
    RunMemberFinder().run(sdd_context, reporting_framework,reporting_framework_version )
