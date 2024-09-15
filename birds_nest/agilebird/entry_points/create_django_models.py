# coding=UTF-8
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

import os
import django
from django.apps import AppConfig
from agilebird.context.sdd_context_django import SDDContext
from agilebird.context.context import Context
from agilebird.process_steps.sqldeveloper_import.import_sqldev_ldm_to_regdna import (
    SQLDevLDMImport,
)
from agilebird.process_steps.sqldeveloper_import.import_sqldev_il_to_regdna import (
    SQLDeveloperILImport,
)
from agilebird.process_steps.sqldeveloper_import.import_sqldev_ldm_to_django import (
    RegDNAToDJango,
)

class RunCreateDjangoModels(AppConfig):
    """AppConfig for creating Django models from SQL Developer Logical Data Model."""

    path = '/workspaces/efbt/birds_nest/birds_nest'


    def ready(self):
        """Prepare the context and run the import and conversion processes."""
        base_dir = '/workspaces/efbt/birds_nest/' 
        
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        
        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory
        if context.ldm_or_il == 'ldm':
            SQLDevLDMImport.do_import(self, context)
        else:
            SQLDeveloperILImport.do_import(self, context)
        RegDNAToDJango.convert(self, context)

if __name__ == '__main__':
    django.setup()
    RunCreateDjangoModels('agilebird', 'birds_nest').ready()

