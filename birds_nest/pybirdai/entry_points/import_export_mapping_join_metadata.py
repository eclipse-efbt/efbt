
import os
from pathlib import Path

import django
from django.apps import AppConfig
from django.conf import settings
from pybirdai.process_steps.import_export_join_metadata.export_join_metadata import ExporterJoins
from pybirdai.process_steps.import_export_join_metadata.import_join_metadata import ImporterJoins
from pybirdai.process_steps.mapping_join_metadata_eil_ldm.mapping_join_eil_ldm import LinkProcessor

class RunExporterJoins(AppConfig):
    """Django AppConfig for running the creation of generation rules."""

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_export_joins_meta_data():
        ExporterJoins.handle()

class RunImporterJoins(AppConfig):
    """Django AppConfig for running the creation of generation rules."""

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_import_joins_meta_data():
        ImporterJoins.handle()

class RunMappingJoinsEIL_LDM(AppConfig):
    """Django AppConfig for running the creation of generation rules."""

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_mapping_joins_meta_data():
        LinkProcessor.handle()
