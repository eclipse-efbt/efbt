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
import glob

# CSV views
def mappings_csv_view(request, filename):
    base_dir = settings.BASE_DIR
    csv_path = os.path.join(base_dir, 'results', 'generated_mapping_warnings', filename)
    csv_contents = []

    with open(csv_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            csv_contents.append(row)

    return render(request, f'pybirdai/reports/validation/{filename.split(".")[0]}.html', {'csv_contents': csv_contents})

def hierarchy_csv_view(request, filename):
    base_dir = settings.BASE_DIR
    csv_path = os.path.join(base_dir, 'results', 'generated_hierarchy_warnings', filename)
    csv_contents = []

    with open(csv_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            csv_contents.append(row)

    return render(request, f'pybirdai/reports/validation/{filename.split(".")[0]}.html', {'csv_contents': csv_contents})

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
    return render(request, 'pybirdai/miscellaneous/review_semantic_integrations.html')

def review_filters(request):
    return render(request, 'pybirdai/miscellaneous/review_filters.html')

def review_import_hierarchies(request):
    return render(request, 'pybirdai/miscellaneous/review_import_hierarchies.html')

def review_join_meta_data(request):
    return render(request, 'pybirdai/miscellaneous/review_join_meta_data.html')

def create_transformation_rules_in_smcubes(request):
    return render(request, 'pybirdai/miscellaneous/create_transformation_rules_in_smcubes.html')

def report_templates(request):
    """
    Display available FINREP report templates dynamically based on files in templates directory.
    """
    # Get the templates directory path
    templates_dir = os.path.join(settings.BASE_DIR, 'pybirdai', 'templates', 'pybirdai', 'reports', 'populated_templates')

    # Find all HTML files containing "FINREP" in the templates directory
    finrep_templates = []

    if os.path.exists(templates_dir):
        # Look for HTML files containing FINREP
        pattern = os.path.join(templates_dir, '*FINREP*.html')
        template_files = glob.glob(pattern)
        
        for template_path in template_files:
            filename = os.path.basename(template_path)
            # Extract display name (remove .html extension)
            display_name = filename.replace('.html', '')
            finrep_templates.append({
                'filename': filename,
                'display_name': display_name
            })
        
        # Sort templates by display name for consistent ordering
        finrep_templates.sort(key=lambda x: x['display_name'])
    
    context = {
        'templates': finrep_templates
    }
    
    return render(request, 'pybirdai/reports/report_templates.html', context)

def create_transformation_rules_configuration(request):
    return render(request, 'pybirdai/miscellaneous/create_transformation_rules_configuration.html')

def derivation_transformation_rules(request):
    return render(request, 'pybirdai/miscellaneous/derivation_transformation_rules.html')

def manual_edits(request):
    return render(request, 'pybirdai/miscellaneous/manual_edits.html')





