# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
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
from django.conf import settings
from pybirdai.context.sdd_context_django import SDDContext
from pybirdai.context.context import Context
from pybirdai.process_steps.sqldeveloper_import.import_sqldev_ldm_to_regdna import (
    SQLDevLDMImport,
)
from pybirdai.process_steps.sqldeveloper_import.import_sqldev_il_to_regdna import (
    SQLDeveloperILImport,
)
from pybirdai.process_steps.sqldeveloper_import.import_sqldev_ldm_to_django import (
    RegDNAToDJango,
)
from pybirdai.process_steps.sqldeveloper_import.convert_regdna_to_xcore import (
    convert_context_packages_to_xcore,
)

class RunCreateDjangoModels(AppConfig):
    """AppConfig for creating Django models from SQL Developer Logical Data Model."""

    path = os.path.join(settings.BASE_DIR, 'birds_nest')


    def ready(self):
        """Prepare the context and run the import and conversion processes."""
        base_dir = settings.BASE_DIR
        
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

        output_directory = "results" + os.sep + "xcore_output"
        convert_context_packages_to_xcore(context, output_directory)
        RegDNAToDJango.convert(self, context)

if __name__ == '__main__':
    django.setup()
    RunCreateDjangoModels('pybirdai', 'birds_nest').ready()

