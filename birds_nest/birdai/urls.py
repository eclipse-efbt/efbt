from django.urls import path

from . import views

app_name = 'birdai'  # Add this line if using namespaces

urlpatterns = [
    path('', views.home_view, name='home'),  # This should handle the root URL of your app
    path('run-import-ref-info-from-sqldev/', views.run_import_input_model_from_sqldev, name='run_import_input_model_from_sqldev'),
    path('edit-variable-mappings/', views.edit_variable_mappings, name='edit_variable_mappings'),
    path('delete-variable-mapping/<str:variable_mapping_id>/', views.delete_variable_mapping, name='delete_variable_mapping'),
    path('edit-variable-mapping-items/', views.edit_variable_mapping_items, name='edit_variable_mapping_items'),
    path('delete-variable-mapping-item/<int:item_id>/', views.delete_variable_mapping_item, name='delete_variable_mapping_item'),
    path('review-semantic-integrations/', views.review_semantic_integrations, name='review_semantic_integrations'),
    path('review-filters/', views.review_filters, name='review_filters'),
    path('review-import-hierarchies/', views.review_import_hierarchies, name='review_import_hierarchies'),
    path('review-report-templates/', views.review_report_templates, name='review_report_templates'),
    path('edit-member-mappings/', views.edit_member_mappings, name='edit_member_mappings'),
    path('delete-member-mapping/<str:member_mapping_id>/', views.delete_member_mapping, name='delete_member_mapping'),
    path('edit-member-mapping-items/', views.edit_member_mapping_items, name='edit_member_mapping_items'),
    path('delete-member-mapping-item/<int:item_id>/', views.delete_member_mapping_item, name='delete_member_mapping_item'),
    path('edit-cube-links/', views.edit_cube_links, name='edit_cube_links'),
    path('delete-cube-link/<str:cube_link_id>/', views.delete_cube_link, name='delete_cube_link'),
    path('edit-cube-structure-item-links/', views.edit_cube_structure_item_links, name='edit_cube_structure_item_links'),
    path('delete-cube-structure-item-link/<str:cube_structure_item_link_id>/', views.delete_cube_structure_item_link, name='delete_cube_structure_item_link'),
    path('edit-mapping-to-cubes/', views.edit_mapping_to_cubes, name='edit_mapping_to_cubes'),
    path('delete-mapping-to-cube/<int:mapping_to_cube_id>/', views.delete_mapping_to_cube, name='delete_mapping_to_cube'),
    path('edit-mapping-definitions/', views.edit_mapping_definitions, name='edit_mapping_definitions'),
    path('delete-mapping-definition/<str:mapping_id>/', views.delete_mapping_definition, name='delete_mapping_definition'),
    path('run-import-non-ref-info/', views.import_report_templates, name='import_report_templates'),
    path('run_import_semantic_integrations_from_website/', views.run_import_semantic_integrations_from_website, name='run_import_semantic_integrations_from_website'),
    path('run_import_hierarchies/', views.run_import_hierarchies, name='run_import_hierarchies'),
    path('missing-children/', views.missing_children, name='missing_children'),
    path('missing-members/', views.missing_members, name='missing_members'),
    path('mappings-missing-members/', views.mappings_missing_members, name='mappings_missing_members'),
    path('mappings-missing-variables/', views.mappings_missing_variables, name='mappings_missing_variables'),
    path('run-create-output-concepts/', views.run_create_filters, name='run_create_filters'),
    path('run-create-transformation-meta-data/', views.run_create_joins_meta_data, name='run_create_joins_meta_data'),
    path('review-transformation-meta-data/', views.review_join_meta_data, name='review_join_meta_data'),
    path('run-delete-transformation-meta-data/', views.run_delete_joins_meta_data, name='run_delete_joins_meta_data'),
    path('run_create_joins_meta_data/', views.run_create_joins_meta_data, name='run_create_joins_meta_data'),
    path('run-create-python-transformations/', views.run_create_python_joins, name='run_create_python_joins'),
    path('executable-transformations/', views.executable_transformations, name='executable_transformations'),
    path('create-input-structures/', views.input_model, name='input_model'),
    path('create-logical-transformations/', views.joins, name='joins'),   
    path('filters/', views.filters, name='filters'),  
    path('run-create-executable-filters/', views.run_create_executable_filters, name='run_create_executable_filters'),
        
]