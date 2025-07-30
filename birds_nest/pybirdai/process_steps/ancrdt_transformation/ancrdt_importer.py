"""
Copyright 2025 Arfa Digital Consulting

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import django
import os
import sys
from django.apps import AppConfig
from django.conf import settings
import logging

# Create a logger
logger = logging.getLogger(__name__)

class DjangoSetup:
    _initialized = False

    @classmethod
    def configure_django(cls):
        """Configure Django settings without starting the application"""
        if cls._initialized:
            return

        try:
            # Set up Django settings module for birds_nest in parent directory
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
            sys.path.insert(0, project_root)
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'birds_nest.settings')

            # This allows us to use Django models without running the server
            django.setup()

            logger.info("Django configured successfully with settings module: %s",
                       os.environ['DJANGO_SETTINGS_MODULE'])
            cls._initialized = True
        except Exception as e:
            logger.error(f"Django configuration failed: {str(e)}")
            raise

class RunANCRDTImport(AppConfig):
    """
    Django AppConfig for running the website to SDD model conversion process.

    This class sets up the necessary context and runs the import process
    to convert website data into an SDD  model.
    """

    # path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_import():
        DjangoSetup.configure_django()
        from pybirdai.context.context_ancrdt import Context
        from pybirdai.context.sdd_context_django_ancrdt import SDDContext
        from pybirdai.process_steps.website_to_sddmodel.import_website_to_sdd_model_django_ancrdt import (
            ImportWebsiteToSDDModel
        )
        # Move the content of the ready() method here
        path = os.path.join(settings.BASE_DIR, 'birds_nest')

        base_dir = settings.BASE_DIR
        sdd_context = SDDContext()
        sdd_context.file_directory = os.path.join(base_dir, 'resources')
        sdd_context.output_directory = os.path.join(base_dir, 'results')
        sdd_context.save_sdd_to_db = True

        context = Context()
        context.file_directory = sdd_context.file_directory
        context.output_directory = sdd_context.output_directory

        import_anacrdt_path = f"..{os.sep}results{os.sep}ancrdt_csv"
        ancrdt_include = True
        if not sdd_context.exclude_reference_info_from_website:
            ImportWebsiteToSDDModel().import_report_templates_from_sdd(sdd_context,import_anacrdt_path,ancrdt_include)

    def ready(self):
        # This method is still needed for Django's AppConfig
        pass

def main():
    DjangoSetup.configure_django()
    RunANCRDTImport.run_import()

if __name__ == "__main__":
    main()
