from django.shortcuts import render
from django.conf import settings
import csv
import os

# CSV views
def csv_view(request, filename):
    base_dir = settings.BASE_DIR
    csv_path = os.path.join(base_dir, 'results', filename)
    csv_contents = []

    with open(csv_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            csv_contents.append(row)

    return render(request, f'{filename.split(".")[0]}.html', {'csv_contents': csv_contents})

def missing_children(request):
    return csv_view(request, 'missing_children.csv')

def missing_members(request):
    return csv_view(request, 'missing_members.csv')

def mappings_missing_members(request):
    return csv_view(request, 'mappings_missing_members.csv')

def mappings_missing_variables(request):
    return csv_view(request, 'mappings_missing_variables.csv')

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

def joins(request):
    return render(request, 'pybirdai/joins.html')

def filters(request):
    return render(request, 'pybirdai/filters.html')

def report_templates(request):
    return render(request, 'pybirdai/report_templates.html')
