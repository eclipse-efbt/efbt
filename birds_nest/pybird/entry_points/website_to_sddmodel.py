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

import django
from django.apps import AppConfig




class RunWebsiteToSDDModel(AppConfig):

    path = 'C:\\Users\\neil\\freebirdtools-develop-July11\\git\\efbt\\birds_nest'
    def ready(self):
        from pybird.models import MAINTENANCE_AGENCY
        
        
        from pybird.process_steps.import_website_to_sdd_model_django import ImportWebsiteToSDDModel
        ImportWebsiteToSDDModel().import_sdd()
        

if __name__ == '__main__':
    django.setup()
    RunWebsiteToSDDModel('pybird','birds_nest').ready()


      
    
