import csv
import os
from django.apps import apps
from django.utils import timezone
from pybirdai.bird_meta_data_model import CUBE_LINK, CUBE_STRUCTURE_ITEM_LINK

class ExporterJoins:

    @staticmethod
    def handle(output_path:str="resources/joins_export/export_file.csv"):

        # Ensure the directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(output_path.replace(".csv","cube_link.csv"), 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)

            # --- Export CUBE_LINK ---
            cube_link_headers = [
                "MAINTENANCE_AGENCY_ID",
                "CUBE_LINK_ID",
                "CODE",
                "NAME",
                "DESCRIPTION",
                "VALID_FROM",
                "VALID_TO",
                "VERSION",
                "ORDER_RELEVANCE",
                "PRIMARY_CUBE_ID",
                "FOREIGN_CUBE_ID",
                "CUBE_LINK_TYPE",
                "JOIN_IDENTIFIER",
            ]
            writer.writerow(cube_link_headers)

            cube_links = CUBE_LINK.objects.all()
            for link in cube_links:
                row = [
                    link.maintenance_agency_id.pk if link.maintenance_agency_id else None,
                    link.cube_link_id,
                    link.code,
                    link.name,
                    link.description,
                    link.valid_from.isoformat() if link.valid_from else None,
                    link.valid_to.isoformat() if link.valid_to else None,
                    link.version,
                    link.order_relevance,
                    link.primary_cube_id.pk if link.primary_cube_id else None,
                    link.foreign_cube_id.pk if link.foreign_cube_id else None,
                    link.cube_link_type,
                    link.join_identifier,
                ]
                writer.writerow(row)

        with open(output_path.replace(".csv","cube_structure_item_link.csv"), 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)

            # --- Export CUBE_STRUCTURE_ITEM_LINK ---
            # Assuming headers correspond to field names or related names
            cube_structure_item_link_headers = [
                "CUBE_STRUCTURE_ITEM_LINK_ID",
                "CUBE_LINK_ID", # FK to CUBE_LINK
                "FOREIGN_CUBE_VARIABLE_CODE", # FK to CUBE_STRUCTURE_ITEM
                "PRIMARY_CUBE_VARIABLE_CODE", # FK to CUBE_STRUCTURE_ITEM
            ]
            writer.writerow(cube_structure_item_link_headers)

            cube_structure_item_links = CUBE_STRUCTURE_ITEM_LINK.objects.all()
            for item_link in cube_structure_item_links:
                row = [
                    item_link.cube_structure_item_link_id,
                    item_link.cube_link_id.pk if item_link.cube_link_id else None,
                    item_link.foreign_cube_variable_code.cube_variable_code if item_link.foreign_cube_variable_code else None,
                    item_link.primary_cube_variable_code.cube_variable_code if item_link.primary_cube_variable_code else None,
                ]
                writer.writerow(row)
