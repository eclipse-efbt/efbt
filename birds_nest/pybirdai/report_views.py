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
from django.shortcuts import render
from django.conf import settings
import csv
import os

# CSV views
def mappings_csv_view(request, filename):
    base_dir = settings.BASE_DIR
    csv_path = os.path.join(base_dir, 'results', 'generated_mapping_warnings', filename)
    csv_contents = []

    with open(csv_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            csv_contents.append(row)

    return render(request, f'{filename.split(".")[0]}.html', {'csv_contents': csv_contents})

def hierarchy_csv_view(request, filename):
    base_dir = settings.BASE_DIR
    csv_path = os.path.join(base_dir, 'results', 'generated_hierarchy_warnings', filename)
    csv_contents = []

    with open(csv_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            csv_contents.append(row)

    return render(request, f'{filename.split(".")[0]}.html', {'csv_contents': csv_contents})

def missing_children(request):
    return hierarchy_csv_view(request, 'missing_children.csv')

def missing_members(request):
    return hierarchy_csv_view(request, 'missing_members.csv')

def mappings_missing_members(request):
    return mappings_csv_view(request, 'mappings_missing_members.csv')

def mappings_missing_variables(request):
    return mappings_csv_view(request, 'mappings_missing_variables.csv')

def mappings_warnings_summary(request):
    return mappings_csv_view(request, 'mappings_warnings_summary.csv')

# Review views
def review_semantic_integrations(request):
    return render(request, 'pybirdai/review_semantic_integrations.html')

def review_filters(request):
    return render(request, 'pybirdai/review_filters.html')

def review_import_hierarchies(request):
    return render(request, 'pybirdai/review_import_hierarchies.html')

def review_report_templates(request):
    return render(request, 'pybirdai/review_report_templates.html')

def review_join_meta_data(request):
    return render(request, 'pybirdai/review_join_meta_data.html')

def executable_transformations(request):
    return render(request, 'pybirdai/executable_transformations.html')

def input_model(request):
    return render(request, 'pybirdai/input_model.html')


def create_database_manual_steps(request):
    return render(request, 'pybirdai/create_database_manual_steps.html')

def create_bird_database(request):
    return render(request, 'pybirdai/create_bird_database.html')

def populate_bird_metadata_database(request):
    return render(request, 'pybirdai/populate_bird_metadata_database.html')

def import_report_template_instructions(request):
    return render(request, 'pybirdai/import_report_template_instructions.html')

def import_data_model_artefacts(request):
    return render(request, 'pybirdai/import_data_model_artefacts.html')

def import_sqldev_eil_files(request):
    return render(request, 'pybirdai/import_sqldev_eil_files.html')

def import_sqldev_eldm_files(request):
    return render(request, 'pybirdai/import_sqldev_eldm_files.html')

def import_bird_eil_datamodel(request):
    return render(request, 'pybirdai/import_bird_eil_datamodel.html')

def import_bird_eldm_datamodel(request):
    return render(request, 'pybirdai/import_bird_eldm_datamodel.html')


def create_transformation_rules_in_python(request):
    return render(request, 'pybirdai/create_transformation_rules_in_python.html')

def create_transformation_rules_in_smcubes(request):
    return render(request, 'pybirdai/create_transformation_rules_in_smcubes.html')

def report_templates(request):
    return render(request, 'pybirdai/report_templates.html')

def create_transformations_metadata(request):
    return render(request, 'pybirdai/create_transformations_metadata.html')

def create_transformation_rules_configuration(request):
    return render(request, 'pybirdai/create_transformation_rules_configuration.html')

def derivation_transformation_rules(request):
    return render(request, 'pybirdai/derivation_transformation_rules.html')

def manual_edits(request):
    return render(request, 'pybirdai/manual_edits.html')

def insert_data_into_bird_database(request):
    return render(request, 'pybirdai/insert_data_into_bird_database.html')





