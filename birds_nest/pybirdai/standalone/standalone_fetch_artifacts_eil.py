import django
import os
import sys
from django.apps import AppConfig
from django.conf import settings
import logging
from importlib import metadata
import ast

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
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
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

if __name__ == "__main__":
    DjangoSetup.configure_django()
    from pybirdai import workflow_views
    import json
    with open("automode_config.json","w") as f:
        json.dump({
          "data_model_type": "EIL",
          "clone_mode": "false",
          "technical_export_source": "GITHUB",
          "technical_export_github_url": "https://github.com/regcommunity/FreeBIRD_EIL",
          "config_files_source": "GITHUB",
          "config_files_github_url": "https://github.com/regcommunity/FreeBIRD_EIL",
          "github_branch": "main",
          "when_to_stop": "RESOURCE_DOWNLOAD",
          "enable_lineage_tracking": False
        }, f)

    workflow_views._run_database_setup_async()
