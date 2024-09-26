from django.http import HttpResponse
from django.shortcuts import render, redirect, get_object_or_404
from birdai.sdd_models import VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM
from django.contrib import messages
from django.forms import modelformset_factory
from django.core.paginator import Paginator
from django.db import transaction
from django.http import HttpResponse
from .entry_points.import_input_model import RunImportInputModelFromSQLDev
from .entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
from django.http import JsonResponse
from .entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
from .entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
import csv
import os
from django.conf import settings
from .entry_points.create_filters import RunCreateFilters
from .entry_points.create_joins_metadata import RunCreateJoinsMetadata
from .entry_points.delete_joins_metadata import RunDeleteJoinsMetadata
from .entry_points.create_executable_joins import RunCreateExecutableJoins



def run_create_joins_meta_data(request):
    app_config = RunCreateJoinsMetadata('birdai', 'birds_nest')
    app_config.run_create_joins_meta_data()
    return HttpResponse("Created Transformation Metadata")

def run_create_python_joins(request):
    app_config = RunCreateExecutableJoins('birdai', 'birds_nest')
    app_config.create_python_joins()
    return HttpResponse("Created Python Transformations ")

def run_delete_joins_meta_data(request):
    app_config = RunDeleteJoinsMetadata('birdai', 'birds_nest')
    app_config.run_delete_joins_meta_data()
    return HttpResponse("Deleted Transformation Metadata")


def run_import_semantic_integrations_from_website(request):
    app_config = RunImportSemanticIntegrationsFromWebsite('birdai', 'birds_nest')
    app_config.import_mappings_from_website()
    return HttpResponse("Import Semantic Integrations completed successfully.")
  


def run_import_input_model_from_sqldev(request):
    app_config = RunImportInputModelFromSQLDev('birdai', 'birds_nest')
    app_config.ready()
    return HttpResponse("Import reference info from SQLDev process completed successfully.")

def run_import_hierarchies(request):
    app_config = RunImportHierarchiesFromWebsite('birdai', 'birds_nest')
    app_config.import_hierarchies()
    return HttpResponse("Import hierarchies successfully.")

def import_report_templates(request):
    app_config = RunImportReportTemplatesFromWebsite('birdai', 'birds_nest')
    app_config.run_import()
    return HttpResponse("Import non-reference info from website completed successfully.")

def index(request):
    return HttpResponse("Hello, world. You're at the birdai index.")

def home_view(request):
    return render(request, 'birdai/home.html')

def edit_variable_mappings(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = VARIABLE_MAPPING.objects.all().order_by('variable_mapping_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    VariableMappingFormSet = modelformset_factory(VARIABLE_MAPPING, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = VariableMappingFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Variable mappings updated successfully.')
            return redirect('birdai:edit_variable_mappings')
        else:
            messages.error(request, 'There was an error updating the variable mappings.')
    else:
        formset = VariableMappingFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_variable_mappings.html', context)

def delete_variable_mapping(request, variable_mapping_id):
    variable_mapping = get_object_or_404(VARIABLE_MAPPING, variable_mapping_id=variable_mapping_id)
    if request.method == 'POST':
        variable_mapping.delete()
        messages.success(request, 'Variable mapping deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_variable_mappings')

def edit_variable_mapping_items(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = VARIABLE_MAPPING_ITEM.objects.all().order_by('id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    VariableMappingItemFormSet = modelformset_factory(VARIABLE_MAPPING_ITEM, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = VariableMappingItemFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Variable mapping items updated successfully.')
            return redirect('birdai:edit_variable_mapping_items')
        else:
            messages.error(request, 'There was an error updating the variable mapping items.')
    else:
        formset = VariableMappingItemFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_variable_mapping_items.html', context)

def delete_variable_mapping_item(request, item_id):
    item = get_object_or_404(VARIABLE_MAPPING_ITEM, id=item_id)
    if request.method == 'POST':
        item.delete()
        messages.success(request, 'Variable mapping item deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_variable_mapping_items')



from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import MEMBER_MAPPING
from django.core.paginator import Paginator
from django.db import transaction

def edit_member_mappings(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = MEMBER_MAPPING.objects.all().order_by('member_mapping_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    MemberMappingFormSet = modelformset_factory(MEMBER_MAPPING, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = MemberMappingFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Member mappings updated successfully.')
            return redirect('birdai:edit_member_mappings')
        else:
            messages.error(request, 'There was an error updating the member mappings.')
    else:
        formset = MemberMappingFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_member_mappings.html', context)

def delete_member_mapping(request, member_mapping_id):
    member_mapping = get_object_or_404(MEMBER_MAPPING, member_mapping_id=member_mapping_id)
    if request.method == 'POST':
        member_mapping.delete()
        messages.success(request, 'Member mapping deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_member_mappings')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import MEMBER_MAPPING_ITEM
from django.core.paginator import Paginator
from django.db import transaction

def edit_member_mapping_items(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = MEMBER_MAPPING_ITEM.objects.all().order_by('id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    MemberMappingItemFormSet = modelformset_factory(MEMBER_MAPPING_ITEM, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = MemberMappingItemFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Member mapping items updated successfully.')
            return redirect('birdai:edit_member_mapping_items')
        else:
            messages.error(request, 'There was an error updating the member mapping items.')
    else:
        formset = MemberMappingItemFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_member_mapping_items.html', context)

def delete_member_mapping_item(request, item_id):
    member_mapping_item = get_object_or_404(MEMBER_MAPPING_ITEM, id=item_id)
    if request.method == 'POST':
        member_mapping_item.delete()
        messages.success(request, 'Member mapping item deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_member_mapping_items')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import CUBE_LINK
from django.core.paginator import Paginator
from django.db import transaction

def edit_cube_links(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = CUBE_LINK.objects.all().order_by('cube_link_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    CubeLinkFormSet = modelformset_factory(CUBE_LINK, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = CubeLinkFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Cube links updated successfully.')
            return redirect('birdai:edit_cube_links')
        else:
            messages.error(request, 'There was an error updating the cube links.')
    else:
        formset = CubeLinkFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_cube_links.html', context)

def delete_cube_link(request, cube_link_id):
    cube_link = get_object_or_404(CUBE_LINK, cube_link_id=cube_link_id)
    if request.method == 'POST':
        cube_link.delete()
        messages.success(request, 'Cube link deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_cube_links')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import CUBE_STRUCTURE_ITEM_LINK
from django.core.paginator import Paginator
from django.db import transaction

def edit_cube_structure_item_links(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = CUBE_STRUCTURE_ITEM_LINK.objects.all().order_by('cube_structure_item_link_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    CubeStructureItemLinkFormSet = modelformset_factory(CUBE_STRUCTURE_ITEM_LINK, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = CubeStructureItemLinkFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Cube structure item links updated successfully.')
            return redirect('birdai:edit_cube_structure_item_links')
        else:
            messages.error(request, 'There was an error updating the cube structure item links.')
    else:
        formset = CubeStructureItemLinkFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_cube_structure_item_links.html', context)

def delete_cube_structure_item_link(request, cube_structure_item_link_id):
    cube_structure_item_link = get_object_or_404(CUBE_STRUCTURE_ITEM_LINK, cube_structure_item_link_id=cube_structure_item_link_id)
    if request.method == 'POST':
        cube_structure_item_link.delete()
        messages.success(request, 'Cube structure item link deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_cube_structure_item_links')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import MAPPING_TO_CUBE
from django.core.paginator import Paginator
from django.db import transaction

def edit_mapping_to_cubes(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
from django.http import HttpResponse
from django.shortcuts import render, redirect, get_object_or_404
from birdai.sdd_models import VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM
from django.contrib import messages
from django.forms import modelformset_factory
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.core.paginator import Paginator
from django.db import transaction
from django.http import HttpResponse
from .entry_points.import_input_model import RunImportInputModelFromSQLDev
from .entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
from django.http import JsonResponse
from .entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
from .entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
import csv
import os
from django.conf import settings
from .entry_points.create_filters import RunCreateFilters
from .entry_points.run_create_executable_filters import RunCreateExecutableFilters


def run_import_input_model_from_sqldev(request):
    app_config = RunImportInputModelFromSQLDev('birdai', 'birds_nest')
    app_config.ready()
    return HttpResponse("Import reference info from SQLDev process completed successfully.")

def run_create_executable_filters(request):
    app_config = RunCreateExecutableFilters('birdai', 'birds_nest')
    app_config.ready()
    return HttpResponse("Create executable filters process completed successfully.")


def index(request):
    return HttpResponse("Hello, world. You're at the birdai index.")

def home_view(request):
    return render(request, 'birdai/home.html')

def edit_variable_mappings(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = VARIABLE_MAPPING.objects.all().order_by('variable_mapping_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    VariableMappingFormSet = modelformset_factory(VARIABLE_MAPPING, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = VariableMappingFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Variable mappings updated successfully.')
            return redirect('birdai:edit_variable_mappings')
        else:
            messages.error(request, 'There was an error updating the variable mappings.')
    else:
        formset = VariableMappingFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_variable_mappings.html', context)

def delete_variable_mapping(request, variable_mapping_id):
    variable_mapping = get_object_or_404(VARIABLE_MAPPING, variable_mapping_id=variable_mapping_id)
    if request.method == 'POST':
        variable_mapping.delete()
        messages.success(request, 'Variable mapping deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_variable_mappings')

def edit_variable_mapping_items(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = VARIABLE_MAPPING_ITEM.objects.all().order_by('id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    VariableMappingItemFormSet = modelformset_factory(VARIABLE_MAPPING_ITEM, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = VariableMappingItemFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Variable mapping items updated successfully.')
            return redirect('birdai:edit_variable_mapping_items')
        else:
            messages.error(request, 'There was an error updating the variable mapping items.')
    else:
        formset = VariableMappingItemFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_variable_mapping_items.html', context)

def delete_variable_mapping_item(request, item_id):
    item = get_object_or_404(VARIABLE_MAPPING_ITEM, id=item_id)
    if request.method == 'POST':
        item.delete()
        messages.success(request, 'Variable mapping item deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_variable_mapping_items')

def review_semantic_integrations(request):
    return render(request, 'birdai/review_semantic_integrations.html')

def review_filters(request):
    return render(request, 'birdai/review_filters.html')

def review_import_hierarchies(request):
    return render(request, 'birdai/review_import_hierarchies.html')

def review_report_templates(request):
    return render(request, 'birdai/review_report_templates.html')


def executable_transformations(request):
    return render(request, 'birdai/executable_transformations.html')


def input_model(request):
    return render(request, 'birdai/input_model.html')

def joins(request):
    return render(request, 'birdai/joins.html')

def filters(request):
    return render(request, 'birdai/filters.html')


def review_join_meta_data(request):
    return render(request, 'birdai/review_join_meta_data.html')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import MEMBER_MAPPING
from django.core.paginator import Paginator
from django.db import transaction

def edit_member_mappings(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = MEMBER_MAPPING.objects.all().order_by('member_mapping_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    MemberMappingFormSet = modelformset_factory(MEMBER_MAPPING, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = MemberMappingFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Member mappings updated successfully.')
            return redirect('birdai:edit_member_mappings')
        else:
            messages.error(request, 'There was an error updating the member mappings.')
    else:
        formset = MemberMappingFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_member_mappings.html', context)

def delete_member_mapping(request, member_mapping_id):
    member_mapping = get_object_or_404(MEMBER_MAPPING, member_mapping_id=member_mapping_id)
    if request.method == 'POST':
        member_mapping.delete()
        messages.success(request, 'Member mapping deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_member_mappings')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import MEMBER_MAPPING_ITEM
from django.core.paginator import Paginator
from django.db import transaction

def edit_member_mapping_items(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = MEMBER_MAPPING_ITEM.objects.all().order_by('id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    MemberMappingItemFormSet = modelformset_factory(MEMBER_MAPPING_ITEM, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = MemberMappingItemFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Member mapping items updated successfully.')
            return redirect('birdai:edit_member_mapping_items')
        else:
            messages.error(request, 'There was an error updating the member mapping items.')
    else:
        formset = MemberMappingItemFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_member_mapping_items.html', context)

def delete_member_mapping_item(request, item_id):
    member_mapping_item = get_object_or_404(MEMBER_MAPPING_ITEM, id=item_id)
    if request.method == 'POST':
        member_mapping_item.delete()
        messages.success(request, 'Member mapping item deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_member_mapping_items')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import CUBE_LINK
from django.core.paginator import Paginator
from django.db import transaction

def edit_cube_links(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = CUBE_LINK.objects.all().order_by('cube_link_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    CubeLinkFormSet = modelformset_factory(CUBE_LINK, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = CubeLinkFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Cube links updated successfully.')
            return redirect('birdai:edit_cube_links')
        else:
            messages.error(request, 'There was an error updating the cube links.')
    else:
        formset = CubeLinkFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_cube_links.html', context)

def delete_cube_link(request, cube_link_id):
    cube_link = get_object_or_404(CUBE_LINK, cube_link_id=cube_link_id)
    if request.method == 'POST':
        cube_link.delete()
        messages.success(request, 'Cube link deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_cube_links')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import CUBE_STRUCTURE_ITEM_LINK
from django.core.paginator import Paginator
from django.db import transaction

def edit_cube_structure_item_links(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = CUBE_STRUCTURE_ITEM_LINK.objects.all().order_by('cube_structure_item_link_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    CubeStructureItemLinkFormSet = modelformset_factory(CUBE_STRUCTURE_ITEM_LINK, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = CubeStructureItemLinkFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Cube structure item links updated successfully.')
            return redirect('birdai:edit_cube_structure_item_links')
        else:
            messages.error(request, 'There was an error updating the cube structure item links.')
    else:
        formset = CubeStructureItemLinkFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_cube_structure_item_links.html', context)

def delete_cube_structure_item_link(request, cube_structure_item_link_id):
    cube_structure_item_link = get_object_or_404(CUBE_STRUCTURE_ITEM_LINK, cube_structure_item_link_id=cube_structure_item_link_id)
    if request.method == 'POST':
        cube_structure_item_link.delete()
        messages.success(request, 'Cube structure item link deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_cube_structure_item_links')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import MAPPING_TO_CUBE
from django.core.paginator import Paginator
from django.db import transaction

def edit_mapping_to_cubes(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
from django.http import HttpResponse
from django.shortcuts import render, redirect, get_object_or_404
from birdai.sdd_models import VARIABLE_MAPPING, VARIABLE_MAPPING_ITEM
from django.contrib import messages
from django.forms import modelformset_factory
from django.core.paginator import Paginator
from django.db import transaction
from django.http import HttpResponse
from .entry_points.import_input_model import RunImportInputModelFromSQLDev
from .entry_points.import_report_templates_from_website import RunImportReportTemplatesFromWebsite
from django.http import JsonResponse
from .entry_points.import_semantic_integrations_from_website import RunImportSemanticIntegrationsFromWebsite
from .entry_points.import_hierarchy_analysis_from_website import RunImportHierarchiesFromWebsite
import csv
import os
from django.conf import settings




def run_create_filters(request):
    app_config = RunCreateFilters('birdai', 'birds_nest')
    app_config.run_create_filters()
    return JsonResponse({"status": "success", "message": "Created output artefacts successfully"})
    


def run_import_input_model_from_sqldev(request):
    app_config = RunImportInputModelFromSQLDev('birdai', 'birds_nest')
    app_config.ready()
    return HttpResponse("Import reference info from SQLDev process completed successfully.")




def index(request):
    return HttpResponse("Hello, world. You're at the birdai index.")

def home_view(request):
    return render(request, 'birdai/home.html')

def edit_variable_mappings(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = VARIABLE_MAPPING.objects.all().order_by('variable_mapping_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    VariableMappingFormSet = modelformset_factory(VARIABLE_MAPPING, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = VariableMappingFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Variable mappings updated successfully.')
            return redirect('birdai:edit_variable_mappings')
        else:
            messages.error(request, 'There was an error updating the variable mappings.')
    else:
        formset = VariableMappingFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_variable_mappings.html', context)

def delete_variable_mapping(request, variable_mapping_id):
    variable_mapping = get_object_or_404(VARIABLE_MAPPING, variable_mapping_id=variable_mapping_id)
    if request.method == 'POST':
        variable_mapping.delete()
        messages.success(request, 'Variable mapping deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_variable_mappings')

def edit_variable_mapping_items(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = VARIABLE_MAPPING_ITEM.objects.all().order_by('id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    VariableMappingItemFormSet = modelformset_factory(VARIABLE_MAPPING_ITEM, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = VariableMappingItemFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Variable mapping items updated successfully.')
            return redirect('birdai:edit_variable_mapping_items')
        else:
            messages.error(request, 'There was an error updating the variable mapping items.')
    else:
        formset = VariableMappingItemFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_variable_mapping_items.html', context)

def delete_variable_mapping_item(request, item_id):
    item = get_object_or_404(VARIABLE_MAPPING_ITEM, id=item_id)
    if request.method == 'POST':
        item.delete()
        messages.success(request, 'Variable mapping item deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_variable_mapping_items')



from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import MEMBER_MAPPING
from django.core.paginator import Paginator
from django.db import transaction

def edit_member_mappings(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = MEMBER_MAPPING.objects.all().order_by('member_mapping_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    MemberMappingFormSet = modelformset_factory(MEMBER_MAPPING, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = MemberMappingFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Member mappings updated successfully.')
            return redirect('birdai:edit_member_mappings')
        else:
            messages.error(request, 'There was an error updating the member mappings.')
    else:
        formset = MemberMappingFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_member_mappings.html', context)

def delete_member_mapping(request, member_mapping_id):
    member_mapping = get_object_or_404(MEMBER_MAPPING, member_mapping_id=member_mapping_id)
    if request.method == 'POST':
        member_mapping.delete()
        messages.success(request, 'Member mapping deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_member_mappings')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import MEMBER_MAPPING_ITEM
from django.core.paginator import Paginator
from django.db import transaction

def edit_member_mapping_items(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = MEMBER_MAPPING_ITEM.objects.all().order_by('id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    MemberMappingItemFormSet = modelformset_factory(MEMBER_MAPPING_ITEM, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = MemberMappingItemFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Member mapping items updated successfully.')
            return redirect('birdai:edit_member_mapping_items')
        else:
            messages.error(request, 'There was an error updating the member mapping items.')
    else:
        formset = MemberMappingItemFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_member_mapping_items.html', context)

def delete_member_mapping_item(request, item_id):
    member_mapping_item = get_object_or_404(MEMBER_MAPPING_ITEM, id=item_id)
    if request.method == 'POST':
        member_mapping_item.delete()
        messages.success(request, 'Member mapping item deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_member_mapping_items')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import CUBE_LINK
from django.core.paginator import Paginator
from django.db import transaction

def edit_cube_links(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = CUBE_LINK.objects.all().order_by('cube_link_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    CubeLinkFormSet = modelformset_factory(CUBE_LINK, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = CubeLinkFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Cube links updated successfully.')
            return redirect('birdai:edit_cube_links')
        else:
            messages.error(request, 'There was an error updating the cube links.')
    else:
        formset = CubeLinkFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_cube_links.html', context)

def delete_cube_link(request, cube_link_id):
    cube_link = get_object_or_404(CUBE_LINK, cube_link_id=cube_link_id)
    if request.method == 'POST':
        cube_link.delete()
        messages.success(request, 'Cube link deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_cube_links')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import CUBE_STRUCTURE_ITEM_LINK
from django.core.paginator import Paginator
from django.db import transaction

def edit_cube_structure_item_links(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = CUBE_STRUCTURE_ITEM_LINK.objects.all().order_by('cube_structure_item_link_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    CubeStructureItemLinkFormSet = modelformset_factory(CUBE_STRUCTURE_ITEM_LINK, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = CubeStructureItemLinkFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Cube structure item links updated successfully.')
            return redirect('birdai:edit_cube_structure_item_links')
        else:
            messages.error(request, 'There was an error updating the cube structure item links.')
    else:
        formset = CubeStructureItemLinkFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_cube_structure_item_links.html', context)

def delete_cube_structure_item_link(request, cube_structure_item_link_id):
    cube_structure_item_link = get_object_or_404(CUBE_STRUCTURE_ITEM_LINK, cube_structure_item_link_id=cube_structure_item_link_id)
    if request.method == 'POST':
        cube_structure_item_link.delete()
        messages.success(request, 'Cube structure item link deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_cube_structure_item_links')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import MAPPING_TO_CUBE
from django.core.paginator import Paginator
from django.db import transaction

def edit_mapping_to_cubes(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = MAPPING_TO_CUBE.objects.all().order_by('id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    MappingToCubeFormSet = modelformset_factory(MAPPING_TO_CUBE, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = MappingToCubeFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Mapping to cube entries updated successfully.')
            return redirect('birdai:edit_mapping_to_cubes')
        else:
            messages.error(request, 'There was an error updating the mapping to cube entries.')
    else:
        formset = MappingToCubeFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_mapping_to_cubes.html', context)

def delete_mapping_to_cube(request, mapping_to_cube_id):
    mapping_to_cube = get_object_or_404(MAPPING_TO_CUBE, id=mapping_to_cube_id)
    if request.method == 'POST':
        mapping_to_cube.delete()
        messages.success(request, 'Mapping to cube entry deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_mapping_to_cubes')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.forms import modelformset_factory
from .sdd_models import MAPPING_DEFINITION
from django.core.paginator import Paginator
from django.db import transaction

def edit_mapping_definitions(request):
    items_per_page = 20  # You can adjust this number as needed
    page_number = request.GET.get('page', 1)
    
    all_items = MAPPING_DEFINITION.objects.all().order_by('mapping_id')
    paginator = Paginator(all_items, items_per_page)
    page_obj = paginator.get_page(page_number)
    
    MappingDefinitionFormSet = modelformset_factory(MAPPING_DEFINITION, fields='__all__', extra=0)
    
    if request.method == 'POST':
        formset = MappingDefinitionFormSet(request.POST, queryset=page_obj.object_list)
        if formset.is_valid():
            with transaction.atomic():
                formset.save()
            messages.success(request, 'Mapping definitions updated successfully.')
            return redirect('birdai:edit_mapping_definitions')
        else:
            messages.error(request, 'There was an error updating the mapping definitions.')
    else:
        formset = MappingDefinitionFormSet(queryset=page_obj.object_list)
    
    context = {
        'formset': formset,
        'page_obj': page_obj,
    }
    return render(request, 'birdai/edit_mapping_definitions.html', context)

def delete_mapping_definition(request, mapping_id):
    mapping_definition = get_object_or_404(MAPPING_DEFINITION, mapping_id=mapping_id)
    if request.method == 'POST':
        mapping_definition.delete()
        messages.success(request, 'Mapping definition deleted successfully.')
    else:
        messages.error(request, 'Invalid request for deletion.')
    return redirect('birdai:edit_mapping_definitions')

def missing_children(request):
    base_dir = settings.BASE_DIR
    csv_path = os.path.join(base_dir, 'results' , 'missing_children.csv')
        
    csv_contents = []

    with open(csv_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            csv_contents.append(row)

    return render(request, 'missing_children.html', {'csv_contents': csv_contents})

def missing_members(request):
    base_dir = settings.BASE_DIR
    csv_path = os.path.join(base_dir, 'results' , 'missing_members.csv')
    csv_contents = []

    with open(csv_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            csv_contents.append(row)

    return render(request, 'missing_members.html', {'csv_contents': csv_contents})

def mappings_missing_members(request):
    base_dir = settings.BASE_DIR
    csv_path = os.path.join(base_dir, 'results' , 'mappings_missing_members.csv')
    
    csv_contents = []

    with open(csv_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            csv_contents.append(row)

    return render(request, 'mappings_missing_members.html', {'csv_contents': csv_contents})

def mappings_missing_variables(request):
    base_dir = settings.BASE_DIR
    csv_path = os.path.join(base_dir, 'results' , 'mappings_missing_variables.csv')
    csv_contents = []

    with open(csv_path, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            csv_contents.append(row)

    return render(request, 'mappings_missing_variables.html', {'csv_contents': csv_contents})
