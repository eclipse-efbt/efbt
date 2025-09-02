# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""Entry point for ANCRDT transformation process."""

import os
import sys
import django
from django.apps import AppConfig
from django.conf import settings
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("log.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class RunANCRDTTransformation(AppConfig):
    """
    Django AppConfig for running the complete ANCRDT transformation process.

    This class orchestrates the three-step ANCRDT process:
    1. Import ANCRDT data
    2. Create joins metadata
    3. Create executable joins
    """

    path = os.path.join(settings.BASE_DIR, 'birds_nest')

    @staticmethod
    def run_step_0_fetch_ancrdt_csv():
        """Step 0: Fetch ANCRDT CSV data from ECB website"""
        logger.info("Starting ANCRDT Step 0: Fetch CSV data from ECB website")

        from pybirdai.utils.bird_ecb_website_fetcher import BirdEcbWebsiteClient

        try:
            client = BirdEcbWebsiteClient()
            output_dir = client.request_and_save(
                tree_root_ids="ANCRDT",
                tree_root_type="FRAMEWORK",
                output_dir="results/ancrdt_csv",
                format_type="csv",
                include_mapping_content=False,
                include_rendering_content=False,
                include_transformation_content=False,
                only_currently_valid_metadata=False
            )
            logger.info(f"ANCRDT Step 0 completed successfully. Data saved to: {output_dir}")
            return True
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"ANCRDT Step 0 failed: {str(e)}\nDetails: {error_detail}")
            raise Exception(f"ANCRDT Step 0 failed: {str(e) or 'Unknown error occurred'}")

    @staticmethod
    def run_step_1_import():
        """Step 1: Import ANCRDT data"""
        logger.info("Starting ANCRDT Step 1: Import data")

        from pybirdai.process_steps.ancrdt_transformation.ancrdt_importer import RunANCRDTImport

        try:
            RunANCRDTImport.run_import()
            logger.info("ANCRDT Step 1 completed successfully")
            return True
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"ANCRDT Step 1 failed: {str(e)}\nDetails: {error_detail}")
            raise Exception(f"ANCRDT Step 1 failed: {str(e) or 'Unknown error occurred'}")

    @staticmethod
    def run_step_2_joins_metadata():
        """Step 2: Create joins metadata"""
        logger.info("Starting ANCRDT Step 2: Create joins metadata")

        from pybirdai.process_steps.ancrdt_transformation.create_joins_meta_data_ancrdt import (
            JoinsMetaDataCreatorANCRDT
        )

        try:
            creator = JoinsMetaDataCreatorANCRDT()
            creator.generate_joins_meta_data()
            logger.info("ANCRDT Step 2 completed successfully")
            return True
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"ANCRDT Step 2 failed: {str(e)}\nDetails: {error_detail}")
            raise Exception(f"ANCRDT Step 2 failed: {str(e) or 'Unknown error occurred'}")

    @staticmethod
    def run_step_3_executable_joins():
        """Step 3: Create executable joins"""
        logger.info("Starting ANCRDT Step 3: Create executable joins")

        from pybirdai.process_steps.ancrdt_transformation.create_executable_joins_ancrdt import (
            RunCreateExecutableJoins
        )

        try:
            RunCreateExecutableJoins.create_python_joins_from_db(logger=logger)
            logger.info("ANCRDT Step 3 completed successfully")
            return True
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"ANCRDT Step 3 failed: {str(e)}\nDetails: {error_detail}")
            raise Exception(f"ANCRDT Step 3 failed: {str(e) or 'Unknown error occurred'}")

    @staticmethod
    def run_all_steps():
        """Run all ANCRDT transformation steps in sequence"""
        logger.info("Starting complete ANCRDT transformation process")

        try:
            # Step 0: Fetch ANCRDT CSV data
            RunANCRDTTransformation.run_step_0_fetch_ancrdt_csv()

            # Step 1: Import
            RunANCRDTTransformation.run_step_1_import()

            # Step 2: Create joins metadata
            RunANCRDTTransformation.run_step_2_joins_metadata()

            # Step 3: Create executable joins
            RunANCRDTTransformation.run_step_3_executable_joins()

            logger.info("Complete ANCRDT transformation process finished successfully")
            return True
        except Exception as e:
            logger.error(f"ANCRDT transformation process failed: {str(e)}")
            raise

    def ready(self):
        # This method is still needed for Django's AppConfig
        pass


def main():
    """Main entry point for command line execution"""
    # This allows running the script directly from command line
    if __name__ == "__main__":
        RunANCRDTTransformation.run_all_steps()


if __name__ == "__main__":
    main()
