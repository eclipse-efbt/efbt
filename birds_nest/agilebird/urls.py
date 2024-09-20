from django.urls import path

from . import views

app_name = 'agilebird'  # Add this line if using namespaces

urlpatterns = [
    path('', views.home_view, name='home'),  # This should handle the root URL of your app
    path('run-import/', views.run_import_view, name='run_import'),
    path('run-import-ref-info-from-sqldev/', views.run_import_ref_info_from_sqldev, name='run_import_ref_info_from_sqldev'),
    path('edit-variable-mappings/', views.edit_variable_mappings, name='edit_variable_mappings'),
    path('delete-variable-mapping/<str:variable_mapping_id>/', views.delete_variable_mapping, name='delete_variable_mapping'),
    path('edit-variable-mapping-items/', views.edit_variable_mapping_items, name='edit_variable_mapping_items'),
    path('delete-variable-mapping-item/<int:item_id>/', views.delete_variable_mapping_item, name='delete_variable_mapping_item'),
    path('review-input-output-analysis/', views.review_input_output_analysis, name='review_input_output_analysis'),
    path('review-import-hierarchies/', views.review_import_hierarchies, name='review_import_hierarchies'),
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
    path('import-ref-info-from-website/', views.import_ref_info_from_website, name='import_ref_info_from_website'),
    path('run-import-non-ref-info/', views.run_import_non_ref_info, name='run_import_non_ref_info'),
    path('run_import_mappings_from_website/', views.run_import_mappings_from_website, name='run_import_mappings_from_website'),
    path('run_import_hierarchies/', views.run_import_hierarchies, name='run_import_hierarchies'),
    path('missing-children/', views.missing_children, name='missing_children'),
    path('missing-members/', views.missing_members, name='missing_members'),
    path('mappings-missing-members/', views.mappings_missing_members, name='mappings_missing_members'),
    path('mappings-missing-variables/', views.mappings_missing_variables, name='mappings_missing_variables'),
    path('run-create-output-concepts/', views.run_create_output_concepts, name='run_create_output_concepts'),
    path('run-create-transformation-meta-data/', views.run_create_transformation_meta_data, name='run_create_transformation_meta_data'),
    path('review-transformation-meta-data/', views.review_transformation_meta_data, name='review_transformation_meta_data'),
    path('run-delete-transformation-meta-data/', views.run_delete_transformation_meta_data, name='run_delete_transformation_meta_data'),
    
]