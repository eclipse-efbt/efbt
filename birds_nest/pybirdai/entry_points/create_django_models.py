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
from pybirdai.process_steps.sqldeveloper_import.import_sqldev_ldm_to_blueprint import (
    SQLDevLDMImport,
)
from pybirdai.process_steps.sqldeveloper_import.import_sqldev_il_to_blueprint import (
    SQLDeveloperILImport,
)
from pybirdai.process_steps.sqldeveloper_import.emit_django_from_blueprint import (
    BlueprintToDjango,
)

class RunCreateDjangoModels(AppConfig):
    """AppConfig for creating Django models from SQL Developer Logical Data Model.

    The import runs in two stages: the SQL Developer CSVs are first read into
    the model blueprint (see pybirdai/model_blueprint), and the finished
    blueprint is then emitted as Django source.
    """

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
        context.generate_etl = getattr(self, "generate_etl", True)

        # Stage one: build the model blueprint from the SQL Developer CSVs.
        if context.ldm_or_il == 'ldm':
            SQLDevLDMImport.do_import(self, context)
        else:
            SQLDeveloperILImport.do_import(self, context)

        # Stage two: emit Django source from the finished blueprint.
        BlueprintToDjango.convert(self, context)

        print("Django models created successfully.")

if __name__ == '__main__':
    django.setup()
    RunCreateDjangoModels('pybirdai', 'birds_nest').ready()
