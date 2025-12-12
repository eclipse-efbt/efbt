from django.urls import path
from .views import core_views as views
from .views import report_views
from .views import aorta_views
from .views import workflow_views
from .views import ancrdt_transformation_views
from .views import ancrdt_workflow_views
from .views import ancrdt_sql_fixture_editor_views
from .views import ancrdt_views
from .views import lineage_views
from .views import output_layer_mapping_workflow_views
from .views import execution_code_editor_views
from .views import member_link_views
from .views import joins_metadata_embed_views
from .api import lineage_api
from .api import enhanced_lineage_api
from .views import bpmn_metadata_lineage_views
from .views import joins_configuration_views
from .views import annotated_template_visualizer_views
from .views import ancrdt_tables_graph_views
from .api import ancrdt_tables_graph_api
from .views.core import derivation_configuration_views
from django.views.generic import TemplateView
from .views.core_views import JoinIdentifierListView, DuplicatePrimaryMemberIdListView

app_name = "pybirdai"
urlpatterns = [
    path("", views.home_view, name="home"),
    path("automode/", views.automode_view, name="automode"),
    path("automode/create-database/", views.automode_create_database, name="automode_create_database"),
    path("automode/test-components/", views.test_automode_components, name="test_automode_components"),
    path(
        "run_import_input_model_from_sqldev/",
        views.run_import_input_model_from_sqldev,
        name="run_import_input_model_from_sqldev",
    ),
    path("edit-variable-mappings/", views.edit_variable_mappings, name="edit_variable_mappings"),
    path("create-variable-mapping/", views.create_variable_mapping, name="create_variable_mapping"),
    path(
        "delete-variable-mapping/<str:variable_mapping_id>/",
        views.delete_variable_mapping,
        name="delete_variable_mapping",
    ),
    path("edit-variable-mapping-items/", views.edit_variable_mapping_items, name="edit_variable_mapping_items"),
    path("create-variable-mapping-item/", views.create_variable_mapping_item, name="create_variable_mapping_item"),
    path("delete-variable-mapping-item/", views.delete_variable_mapping_item, name="delete_variable_mapping_item"),
    path(
        "review-semantic-integrations/", report_views.review_semantic_integrations, name="review_semantic_integrations"
    ),
    path("review-filters/", report_views.review_filters, name="review_filters"),
    path("review-import-hierarchies/", report_views.review_import_hierarchies, name="review_import_hierarchies"),
    path("edit-member-mappings/", views.edit_member_mappings, name="edit_member_mappings"),
    path("delete-member-mapping/<str:member_mapping_id>/", views.delete_member_mapping, name="delete_member_mapping"),
    path("edit-member-mapping-items/", views.edit_member_mapping_items, name="edit_member_mapping_items"),
    path(
        "delete-member-mapping-item/<int:item_id>/", views.delete_member_mapping_item, name="delete_member_mapping_item"
    ),
    path("edit-cube-links/", views.edit_cube_links, name="edit_cube_links"),
    path("delete-cube-link/<str:cube_link_id>/", views.delete_cube_link, name="delete_cube_link"),
    path("add-cube-link/", views.add_cube_link, name="add_cube_link"),
    path(
        "edit-cube-structure-item-links/", views.edit_cube_structure_item_links, name="edit_cube_structure_item_links"
    ),
    path(
        "delete-cube-structure-item-link/<str:cube_structure_item_link_id>/",
        views.delete_cube_structure_item_link,
        name="delete_cube_structure_item_link",
    ),
    path(
        "add-cube-structure-item-link/",
        views.add_cube_structure_item_link,
        name="add_cube_structure_item_link",
    ),
    # Embed versions for ANCRDT workflow dashboard
    path("edit-cube-links/embed/", joins_metadata_embed_views.edit_cube_links_embed, name="edit_cube_links_embed"),
    path("api/cube-links/list/", joins_metadata_embed_views.api_cube_links_list, name="api_cube_links_list"),
    path("api/cube-links/filter-options/", joins_metadata_embed_views.api_cube_links_filter_options, name="api_cube_links_filter_options"),
    path(
        "edit-cube-structure-item-links/embed/",
        joins_metadata_embed_views.edit_cube_structure_item_links_embed,
        name="edit_cube_structure_item_links_embed",
    ),
    path(
        "api/cube-structure-item-links/list/",
        joins_metadata_embed_views.api_cube_structure_item_links_list,
        name="api_cube_structure_item_links_list",
    ),
    path(
        "api/cube-structure-item-links/filter-options/",
        joins_metadata_embed_views.api_cube_structure_item_links_filter_options,
        name="api_cube_structure_item_links_filter_options",
    ),
    # Additional Cube Links APIs for add functionality and cascading filters
    path("api/cube-links/cubes/", joins_metadata_embed_views.get_cubes_json, name="api_get_cubes"),
    path(
        "api/cube-links/join-identifiers/<str:foreign_cube_id>/",
        joins_metadata_embed_views.get_join_identifiers_for_cube,
        name="api_get_join_identifiers_for_cube",
    ),
    path("api/cube-links/add/", joins_metadata_embed_views.add_cube_link_ajax, name="api_add_cube_link"),
    # Additional Cube Structure Item Links APIs for add functionality
    path(
        "api/cube-structure-item-links/cube-links/",
        joins_metadata_embed_views.get_cube_links_json,
        name="api_get_cube_links",
    ),
    path(
        "api/cube-structure-item-links/variables/<str:cube_link_id>/",
        joins_metadata_embed_views.get_cube_structure_items_for_link,
        name="api_get_cube_structure_items_for_link",
    ),
    path(
        "api/cube-structure-item-links/add/",
        joins_metadata_embed_views.add_cube_structure_item_link_ajax,
        name="api_add_cube_structure_item_link",
    ),
    # Member Link URLs
    path("edit-member-links/", member_link_views.edit_member_links, name="edit_member_links"),
    path("edit-member-links/embed/", member_link_views.edit_member_links_embed, name="edit_member_links_embed"),
    path(
        "delete-member-link/<str:cube_structure_item_link_id>/<str:primary_member_id>/<str:foreign_member_id>/",
        member_link_views.delete_member_link,
        name="delete_member_link",
    ),
    # Member Link API endpoints
    path("api/member-links/list/", member_link_views.get_member_links_json, name="api_member_links_list"),
    path("api/member-links/filter-options/", member_link_views.get_member_links_filter_options, name="api_member_links_filter_options"),
    path(
        "api/member-links/related-members/<str:cube_link_id>/",
        member_link_views.get_related_members_json,
        name="api_related_members",
    ),
    path("api/member-links/add/", member_link_views.add_member_link_ajax, name="api_add_member_link"),
    path("edit-mapping-to-cubes/", views.edit_mapping_to_cubes, name="edit_mapping_to_cubes"),
    path("create-mapping-to-cube/", views.create_mapping_to_cube, name="create_mapping_to_cube"),
    path(
        "delete-mapping-to-cube/<int:mapping_to_cube_id>/", views.delete_mapping_to_cube, name="delete_mapping_to_cube"
    ),
    path("edit-mapping-definitions/", views.edit_mapping_definitions, name="edit_mapping_definitions"),
    path("create-mapping-definition/", views.create_mapping_definition, name="create_mapping_definition"),
    path(
        "delete-mapping-definition/<str:mapping_id>/", views.delete_mapping_definition, name="delete_mapping_definition"
    ),
    path("export-mapping-template/", views.export_mapping_template, name="export_mapping_template"),
    path("export-mapping-data/<str:mapping_id>/", views.export_mapping_data, name="export_mapping_data"),
    path("import-mapping-from-csv/", views.import_mapping_from_csv, name="import_mapping_from_csv"),
    path("delete-cube/<str:cube_id>/", views.delete_cube, name="delete_cube"),
    path("import_report_templates/", views.import_report_templates, name="import_report_templates"),

    # ANCRDT Transformation URLs (old step-by-step views - deprecated)
    path("ancrdt/fetch-csv/", ancrdt_transformation_views.ancrdt_fetch_csv, name="ancrdt_fetch_csv"),
    path("ancrdt/import/", ancrdt_transformation_views.ancrdt_import, name="ancrdt_import"),
    path("ancrdt/create-joins-metadata/", ancrdt_transformation_views.ancrdt_create_joins_metadata, name="ancrdt_create_joins_metadata"),
    path("ancrdt/create-executable-joins/", ancrdt_transformation_views.ancrdt_create_executable_joins, name="ancrdt_create_executable_joins"),

    # ANCRDT Workflow - Separate step execution views
    path("ancrdt-workflow/step-0/", ancrdt_workflow_views.ancrdt_step_0_view, name="ancrdt_step_0"),

    # ANCRDT Workflow - Separate review views (no review for step 0)
    path("ancrdt-workflow/step-1/review/", ancrdt_workflow_views.ancrdt_step_1_review_view, name="ancrdt_step_1_review"),
    path("ancrdt-workflow/step-2/review/", ancrdt_workflow_views.ancrdt_step_2_review_view, name="ancrdt_step_2_review"),
    path("ancrdt-workflow/step-3/review/", ancrdt_workflow_views.ancrdt_step_3_review_view, name="ancrdt_step_3_review"),

    # ANCRDT Workflow - Step 4: Execute Tables
    path("ancrdt-workflow/step-4/", ancrdt_workflow_views.ancrdt_step_4_execute_view, name="ancrdt_step_4"),
    path("ancrdt-workflow/execute-table/<str:table_name>/", ancrdt_workflow_views.execute_ancrdt_table_with_fixture, name="ancrdt_execute_table_with_fixture"),
    path("download-ancrdt-csv/<str:table_name>/", ancrdt_workflow_views.download_ancrdt_csv, name="download_ancrdt_csv"),

    # ANCRDT Workflow - Step 5: Full Execution with Test Suite
    path("ancrdt-workflow/step-5/", ancrdt_workflow_views.ancrdt_step_5_test_suite_view, name="ancrdt_step_5"),
    path("ancrdt-workflow/step-5/review/", ancrdt_workflow_views.ancrdt_step_5_review_view, name="ancrdt_step_5_review"),

    # ANCRDT Workflow - SQL Fixtures Editor
    path("ancrdt-workflow/sql-fixtures-editor/", ancrdt_sql_fixture_editor_views.sql_fixtures_editor, name="ancrdt_sql_fixtures_editor"),
    path("ancrdt-workflow/sql-fixtures-editor/<str:table_name>/", ancrdt_sql_fixture_editor_views.sql_fixtures_editor, name="ancrdt_sql_fixtures_editor_table"),
    path("api/ancrdt/sql-fixtures/load/", ancrdt_sql_fixture_editor_views.load_sql_fixture, name="ancrdt_load_sql_fixture"),
    path("api/ancrdt/sql-fixtures/save/", ancrdt_sql_fixture_editor_views.save_sql_fixture, name="ancrdt_save_sql_fixture"),
    path("api/ancrdt/sql-fixtures/create/", ancrdt_sql_fixture_editor_views.create_sql_fixture, name="ancrdt_create_sql_fixture"),
    path("api/ancrdt/sql-fixtures/delete/", ancrdt_sql_fixture_editor_views.delete_sql_fixture, name="ancrdt_delete_sql_fixture"),
    path("api/ancrdt/sql-fixtures/list/<str:table_name>/", ancrdt_sql_fixture_editor_views.list_sql_fixtures, name="ancrdt_list_sql_fixtures"),

    # ANCRDT Workflow - API endpoints for cube structure visualization
    path("api/ancrdt/cubes/", ancrdt_workflow_views.api_ancrdt_cubes, name="api_ancrdt_cubes"),
    path("api/ancrdt/cube-structure/<str:cube_id>/", ancrdt_workflow_views.api_ancrdt_cube_structure, name="api_ancrdt_cube_structure"),

    # ANCRDT Tables Graph - Interactive visualization of table relationships
    path("ancrdt/tables/graph/", ancrdt_tables_graph_views.ancrdt_tables_graph_viewer, name="ancrdt_tables_graph"),
    path("api/ancrdt/tables/graph/", ancrdt_tables_graph_api.get_ancrdt_tables_graph, name="api_ancrdt_tables_graph"),

    # Execution Code Editing Workflow URLs
    path("execution-code-editing/review-joins/<int:step>/", execution_code_editor_views.review_joins_metadata, name="review_joins_metadata"),
    path("execution-code-editing/regenerate-code/<int:step>/", execution_code_editor_views.regenerate_execution_code, name="regenerate_execution_code"),
    path("execution-code-editing/review-code/", execution_code_editor_views.review_execution_code, name="review_execution_code"),
    path("execution-code-editing/review-code/<str:source>/", execution_code_editor_views.review_execution_code, name="review_execution_code"),
    path("execution-code-editing/edit/<str:source>/<str:file_name>/", execution_code_editor_views.edit_execution_code, name="edit_execution_code"),
    path("execution-code-editing/view/<str:source>/<str:file_name>/", execution_code_editor_views.edit_execution_code, name="view_execution_code"),  # For viewing
    path("execution-code-editing/save/", execution_code_editor_views.save_code_modifications, name="save_code_modifications"),
    path("execution-code-editing/api/structure/<str:source>/<str:file_name>/", execution_code_editor_views.get_code_structure, name="get_code_structure"),
    path("execution-code-editing/api/validate/", execution_code_editor_views.validate_python_code, name="validate_python_code"),
    path("execution-code-editing/duplicate-class/", execution_code_editor_views.duplicate_class_node, name="duplicate_class_node"),
    path("execution-code-editing/approve/<int:step>/", execution_code_editor_views.approve_execution_code, name="approve_execution_code"),

    # Unified Filter Code Editor URLs
    path("filter-code-editor/", execution_code_editor_views.unified_filter_code_editor, name="unified_filter_code_editor"),
    path("filter-code-editor/api/load/", execution_code_editor_views.load_filter_code_file, name="load_filter_code_file"),
    path("filter-code-editor/api/save/", execution_code_editor_views.save_filter_code_file, name="save_filter_code_file"),

    path("edit-ancrdt-output-tables/", execution_code_editor_views.edit_ancrdt_output_tables, name="edit_ancrdt_output_tables"),
    path("save-ancrdt-output-tables/", execution_code_editor_views.save_ancrdt_output_tables, name="save_ancrdt_output_tables"),

    # Code Sync URLs - ANCRDT Lifecycle Management
    path("code-sync/deploy/", execution_code_editor_views.sync_file_to_production, name="sync_file_to_production"),
    path("code-sync/deploy-all/", execution_code_editor_views.sync_all_ancrdt_files, name="sync_all_ancrdt_files"),
    path("code-sync/status/", execution_code_editor_views.get_sync_status, name="get_sync_status"),
    path("code-sync/status/<str:file_name>/", execution_code_editor_views.get_sync_status, name="get_sync_status_file"),
    path("code-sync/diff/<str:file_name>/", execution_code_editor_views.get_file_diff, name="get_file_diff"),
    path("code-sync/check-edits/<str:file_name>/", execution_code_editor_views.check_manual_edits, name="check_manual_edits"),
    path("code-sync/save-and-deploy/", execution_code_editor_views.save_and_deploy, name="save_and_deploy"),
    path("code-sync/file-info/<str:source>/<str:file_name>/", execution_code_editor_views.get_file_info, name="get_file_info"),

    path(
        "run_import_semantic_integrations_from_website/",
        views.run_import_semantic_integrations_from_website,
        name="run_import_semantic_integrations_from_website",
    ),
    path("run_import_hierarchies/", views.run_import_hierarchies, name="run_import_hierarchies"),
    path("missing-children/", report_views.missing_children, name="missing_children"),
    path("missing-members/", report_views.missing_members, name="missing_members"),
    path("mappings-missing-members/", report_views.mappings_missing_members, name="mappings_missing_members"),
    path("mappings-missing-variables/", report_views.mappings_missing_variables, name="mappings_missing_variables"),
    path("mappings-warnings-summary/", report_views.mappings_warnings_summary, name="mappings_warnings_summary"),
    path("run-create-output-concepts/", views.run_create_filters, name="run_create_filters"),
    path("run-create-transformation-meta-data/", views.run_create_joins_meta_data, name="run_create_joins_meta_data"),
    path("review-transformation-meta-data/", report_views.review_join_meta_data, name="review_join_meta_data"),
    path("run-delete-transformation-meta-data/", views.run_delete_joins_meta_data, name="run_delete_joins_meta_data"),
    path("run-delete-mappings/", views.run_delete_mappings, name="run_delete_mappings"),
    path("run-delete-output-concepts/", views.run_delete_output_concepts, name="run_delete_output_concepts"),
    path("run_create_joins_meta_data/", views.run_create_joins_meta_data, name="run_create_joins_meta_data"),
    path("run-create-python-transformations/", views.run_create_python_joins, name="run_create_python_joins"),
    path(
        "create-transformation-rules-in-smcubes/",
        report_views.create_transformation_rules_in_smcubes,
        name="create_transformation_rules_in_smcubes",
    ),
    path("run-create-executable-filters/", views.run_create_executable_filters, name="run_create_executable_filters"),
    path(
        "run-create-executable-filters-from-db/",
        views.run_create_executable_filters_from_db,
        name="run_create_executable_filters_from_db",
    ),
    path(
        "run-create-python-joins-from-db/",
        views.run_create_python_joins_from_db,
        name="run_create_python_joins_from_db",
    ),
    path(
        "run-create-python-transformations-from-db/",
        views.run_create_python_transformations_from_db,
        name="run_create_python_transformations_from_db",
    ),
    path("execute-data-point/<str:data_point_id>/", views.execute_data_point, name="execute_data_point"),
    path("execute-ancrdt-table/<str:table_name>/", ancrdt_views.execute_ancrdt_table, name="execute_ancrdt_table"),
    path("show-report/<str:report_id>/", views.show_report, name="show_report"),
    path("report-templates/", report_views.report_templates, name="report_templates"),
    path("lineage/", views.list_lineage_files, name="list_lineage_files"),
    path("lineage/<str:filename>/", views.view_csv_file, name="view_csv"),
    path("upload-sqldev-eil-files/", views.upload_sqldev_eil_files, name="upload_sqldev_eil_files"),
    path("upload-technical-export-files/", views.upload_technical_export_files, name="upload_technical_export_files"),
    path("create-django-models/", views.create_django_models, name="create_django_models"),
    path(
        "delete-existing-contents-of-bird-metadata-database/",
        views.delete_existing_contents_of_bird_metadata_database,
        name="delete_existing_contents_of_bird_metadata_database",
    ),
    path(
        "create-transformation-rules-configuration/",
        report_views.create_transformation_rules_configuration,
        name="create_transformation_rules_configuration",
    ),
    path(
        "derivation-transformation-rules/",
        report_views.derivation_transformation_rules,
        name="derivation_transformation_rules",
    ),
    path("manual-edits/", report_views.manual_edits, name="manual_edits"),
    path("upload-joins-configuration/", views.upload_joins_configuration, name="upload_joins_configuration"),
    # Join Configuration Management - AJAX API
    path("joins-config/list-frameworks/", joins_configuration_views.list_frameworks, name="joins_config_list_frameworks"),
    path("joins-config/load/", joins_configuration_views.load_csv, name="joins_config_load"),
    path("joins-config/save/", joins_configuration_views.save_csv, name="joins_config_save"),
    path("joins-config/create-framework/", joins_configuration_views.create_framework, name="joins_config_create_framework"),
    path("joins-config/file-info/", joins_configuration_views.get_file_info, name="joins_config_file_info"),
    path("joins-config/il-tables/", joins_configuration_views.get_il_tables, name="joins_config_il_tables"),
    path("joins-config/filters/", joins_configuration_views.get_filters_list, name="joins_config_filters"),
    path("combinations/", views.combinations, name="combinations"),
    path("combination-items/", views.combination_items, name="combination_items"),
    path("output-layers/", views.output_layers, name="output_layers"),
    path("delete-combination/<str:combination_id>/", views.delete_combination, name="delete_combination"),
    path("delete-combination-item/<int:item_id>/", views.delete_combination_item, name="delete_combination_item"),
    path("join-identifiers/", JoinIdentifierListView.as_view(), name="join_identifier_list"),
    path(
        "duplicate-primary-member-ids/", views.duplicate_primary_member_id_list, name="duplicate_primary_member_id_list"
    ),
    path("add-cube-structure-item-link/", views.add_cube_structure_item_link, name="add_cube_structure_item_link"),
    path("upload_sqldev_eldm_files/", views.upload_sqldev_eldm_files, name="upload_sqldev_eldm_files"),
    path("show-gaps/", views.show_gaps, name="show_gaps"),
    path("create_member_mapping/", views.create_member_mapping, name="create_member_mapping"),
    path("edit_member_mapping_items/", views.edit_member_mapping_items, name="edit_member_mapping_items"),
    path("add_member_mapping_item/", views.add_member_mapping_item, name="add_member_mapping_item"),
    path(
        "view_member_mapping_items_by_row/",
        views.view_member_mapping_items_by_row,
        name="view_member_mapping_items_by_row",
    ),
    path("export-database-to-csv/", views.export_database_to_csv, name="export_database_to_csv"),
    path("export-database-to-github/", workflow_views.export_database_to_github, name="export_database_to_github"),
    path("bird_diffs_and_corrections/", views.bird_diffs_and_corrections, name="bird_diffs_and_corrections"),
    path(
        "convert_ldm_to_sdd_hierarchies/", views.convert_ldm_to_sdd_hierarchies, name="convert_ldm_to_sdd_hierarchies"
    ),
    path("view_ldm_to_sdd_results/", views.view_ldm_to_sdd_results, name="view_ldm_to_sdd_results"),
    path("import_members_from_csv/", views.import_members_from_csv, name="import_members_from_csv"),
    path("import_variables_from_csv/", views.import_variables_from_csv, name="import_variables_from_csv"),
    path(
        "semantic_integration_editor/",
        views.semantic_integration_editor,
        name="semantic_integration_editor",
    ),
    path(
        "semantic_integration_editor/<str:mapping_id>/",
        views.semantic_integration_editor,
        name="semantic_integration_editor_with_id",
    ),
    path("edit_mapping_endpoint/", views.edit_mapping_endpoint, name="edit_mapping_endpoint"),
    path("add_variable_endpoint/", views.add_variable_endpoint, name="add_variable_endpoint"),
    path("get_domain_members/<str:variable_id>/", views.get_domain_members, name="get_domain_members"),
    path("get_mapping_details/<str:mapping_id>/", views.get_mapping_details, name="get_mapping_details"),
    path("delete_mapping_row/", views.delete_mapping_row, name="delete_mapping_row"),
    path("duplicate_mapping/", views.duplicate_mapping, name="duplicate_mapping"),
    path("update_mapping_row/", views.update_mapping_row, name="update_mapping_row"),
    path("return_cubelink_visualisation/", views.return_cubelink_visualisation, name="return_cubelink_visualisation"),
    path(
        "return_cubelink_visualisation/?cube_id=<int:cube_id>&join_identifier=<str:join_identifier>&in_md=<str:in_md>",
        views.return_cubelink_visualisation,
        name="return_cubelink_visualisation",
    ),
    path("test_report_view/", views.test_report_view, name="test_report_view"),
    path(
        "bulk-delete-cube-structure-item-links/",
        views.bulk_delete_cube_structure_item_links,
        name="bulk_delete_cube_structure_item_links",
    ),
    path(
        "delete-cube-structure-item-link-dupl/<str:cube_structure_item_link_id>/",
        views.delete_cube_structure_item_link_dupl,
        name="delete_cube_structure_item_link_dupl",
    ),
    path("member_hierarchy_editor/", views.member_hierarchy_editor, name="member_hierarchy_editor"),
    path("member_hierarchy_editor/<str:hierarchy_id>/", views.member_hierarchy_editor, name="member_hierarchy_editor"),
    path("add_member_to_hierarchy/", views.add_member_to_hierarchy, name="add_member_to_hierarchy"),
    path("delete_member_from_hierarchy/", views.delete_member_from_hierarchy, name="delete_member_from_hierarchy"),
    path("edit_hierarchy_node/", views.edit_hierarchy_node, name="edit_hierarchy_node"),
    path("get_members_by_domain/<str:domain_id>/", views.get_members_by_domain, name="get_members_by_domain"),
    path("get_subdomain_enumerations/", views.get_subdomain_enumerations, name="get_subdomain_enumerations"),
    path("run-full-setup/", views.run_full_setup, name="run_full_setup"),
    path(
        "import_bird_data_from_csv_export/",
        views.import_bird_data_from_csv_export,
        name="import_bird_data_from_csv_export",
    ),
    path(
        "automode-import-bird-metamodel-from-website/",
        views.automode_import_bird_metamodel_from_website,
        name="automode_import_bird_metamodel_from_website",
    ),
    path("run_fetch_curated_resources/", views.run_fetch_curated_resources, name="run_fetch_curated_resources"),
    path(
        "workflow/task/<int:task_number>/substep/<str:substep_name>/",
        workflow_views.workflow_task_substep,
        name="workflow_task_substep",
    ),
    path("workflow/session-check/", workflow_views.workflow_session_check, name="workflow_session_check"),
    path(
        "workflow/task/<int:task_number>/substep-loading/<str:substep_name>/",
        workflow_views.workflow_task_substep_with_loading,
        name="workflow_task_substep_with_loading",
    ),
    path(
        "workflow/reset-session-full/", workflow_views.workflow_reset_session_full, name="workflow_reset_session_full"
    ),
    path(
        "workflow/reset-session-partial/",
        workflow_views.workflow_reset_session_partial,
        name="workflow_reset_session_partial",
    ),
    path("api/hierarchy/<str:hierarchy_id>/json/", views.get_hierarchy_json, name="get_hierarchy_json"),
    path("api/hierarchy/save/", views.save_hierarchy_json, name="save_hierarchy_json"),
    path("api/domain/<str:domain_id>/members/", views.get_domain_members_json, name="get_domain_members_json"),
    path("api/hierarchies/", views.get_available_hierarchies_json, name="get_available_hierarchies_json"),
    path(
        "api/hierarchy/create/", views.create_hierarchy_from_visualization, name="create_hierarchy_from_visualization"
    ),
    path("api/hierarchy/create-simple/", views.create_hierarchy_simple, name="create_hierarchy_simple"),
    path("api/member/create/", views.create_member_json, name="create_member_json"),
    path("automode/configure/", views.automode_configure, name="automode_configure"),
    path("automode/execute/", views.automode_execute, name="automode_execute"),
    path(
        "automode/continue-post-restart/", views.automode_continue_post_restart, name="automode_continue_post_restart"
    ),
    path("automode/debug-config/", views.automode_debug_config, name="automode_debug_config"),
    path("automode/status/", views.automode_status, name="automode_status"),
    path("workflow/", workflow_views.workflow_dashboard, name="workflow_dashboard"),
    path("workflow/task/<int:task_number>/<str:operation>/", workflow_views.workflow_task_router, name="workflow_task"),
    path("workflow/automode/", workflow_views.workflow_automode, name="workflow_automode"),
    path("workflow/database-setup/", workflow_views.workflow_database_setup, name="workflow_database_setup"),
    path("workflow/run-migrations/", workflow_views.workflow_run_migrations, name="workflow_run_migrations"),
    path("workflow/migration-status/", workflow_views.workflow_migration_status, name="workflow_migration_status"),
    path("workflow/setup-database-models/", workflow_views.workflow_setup_database_models, name="workflow_setup_database_models"),
    path("workflow/setup-database-models-status/", workflow_views.workflow_setup_database_models_status, name="workflow_setup_database_models_status"),
    path(
        "workflow/database-setup-status/",
        workflow_views.workflow_database_setup_status,
        name="workflow_database_setup_status",
    ),
    path("workflow/automode-status/", workflow_views.workflow_automode_status, name="workflow_automode_status"),
    path("workflow/save-config/", workflow_views.workflow_save_config, name="workflow_save_config"),
    path("workflow/task/<int:task_number>/status/", workflow_views.workflow_task_status, name="workflow_task_status"),
    path("workflow/clone-import/", workflow_views.workflow_clone_import, name="workflow_clone_import"),
    # DPM execution endpoints
    path("workflow/dpm/execute/<int:step_number>/", workflow_views.execute_dpm_step, name="workflow_execute_dpm_step"),
    path("workflow/dpm/status/", workflow_views.get_dpm_status, name="workflow_dpm_status"),
    path("workflow/dpm/review/<int:step_number>/", workflow_views.workflow_dpm_review, name="workflow_dpm_review"),
    # DPM API endpoints for cube structure visualization
    path("api/dpm/cubes/", workflow_views.api_dpm_cubes, name="api_dpm_cubes"),
    # DPM table selection endpoints
    path("workflow/dpm/get-available-tables/", workflow_views.get_available_tables_for_selection, name="workflow_dpm_get_available_tables"),
    path("workflow/dpm/save-table-selection/", workflow_views.save_table_selection, name="workflow_dpm_save_table_selection"),
    path("workflow/dpm/presets/", workflow_views.manage_table_presets, name="workflow_dpm_manage_presets"),
    # AnaCredit execution endpoints
    path("workflow/ancrdt/execute/<int:step_number>/", workflow_views.execute_ancrdt_step, name="workflow_execute_ancrdt_step"),
    path("workflow/ancrdt/status/", workflow_views.get_ancrdt_status, name="workflow_ancrdt_status"),
    path("api/aorta/trails/", aorta_views.AortaTrailListView.as_view(), name="aorta-trail-list"),
    path("api/aorta/trails/<int:trail_id>/", aorta_views.AortaTrailDetailView.as_view(), name="aorta-trail-detail"),
    path(
        "api/aorta/values/<int:value_id>/lineage/",
        aorta_views.AortaValueLineageView.as_view(),
        name="aorta-value-lineage",
    ),
    path(
        "api/aorta/tables/<int:table_id>/dependencies/",
        aorta_views.AortaTableDependenciesView.as_view(),
        name="aorta-table-dependencies",
    ),
    path(
        "api/aorta/trails/<int:trail_id>/graph/",
        aorta_views.AortaLineageGraphView.as_view(),
        name="aorta-lineage-graph",
    ),
    path("trails/", lineage_views.trail_list, name="trail_list"),
    path("trails/<int:trail_id>/lineage/", lineage_views.trail_lineage_viewer, name="trail_lineage_viewer"),
    path(
        "trails/<int:trail_id>/filtered-lineage/",
        lineage_views.trail_filtered_lineage_viewer,
        name="trail_filtered_lineage_viewer",
    ),
    path("api/trail/<int:trail_id>/lineage/", lineage_views.get_trail_lineage_data, name="get_trail_lineage_data"),
    path(
        "api/trail/<int:trail_id>/node/<str:node_type>/<int:node_id>/",
        lineage_views.get_node_details,
        name="get_node_details",
    ),
    path(
        "api/trail/<int:trail_id>/complete-lineage/",
        lineage_api.get_trail_complete_lineage,
        name="get_trail_complete_lineage",
    ),
    path("api/trail/<int:trail_id>/summary/", lineage_api.get_trail_lineage_summary, name="get_trail_lineage_summary"),
    path(
        "api/trail/<int:trail_id>/filtered-lineage/",
        enhanced_lineage_api.get_trail_filtered_lineage,
        name="get_trail_filtered_lineage",
    ),
    path(
        "api/trail/<int:trail_id>/calculation-summary/",
        enhanced_lineage_api.get_calculation_summary,
        name="get_calculation_summary",
    ),
    path(
        "api/trail/<int:trail_id>/debug/",
        lambda request, trail_id: __import__(
            "pybirdai.api.debug_tracking", fromlist=["create_debug_api_endpoint"]
        ).create_debug_api_endpoint()(request, trail_id),
        name="debug_trail_data",
    ),
    path(
        "api/execute-datapoint-with-lineage/<str:data_point_id>/",
        views.execute_datapoint_with_lineage,
        name="execute_datapoint_with_lineage",
    ),
    path(
        "datapoint/<str:datapoint_id>/bpmn-metadata-lineage/",
        bpmn_metadata_lineage_views.datapoint_bpmn_metadata_lineage_viewer,
        name="datapoint_bpmn_metadata_lineage_viewer",
    ),
    path(
        "datapoint/<str:datapoint_id>/bpmn_metadata_lineage/process/",
        bpmn_metadata_lineage_views.process_datapoint_bpmn_metadata_lineage,
        name="process_datapoint_bpmn_metadata_lineage",
    ),
    path(
        "api/datapoint/<str:datapoint_id>/bpmn-metadata-lineage/graph/",
        bpmn_metadata_lineage_views.get_datapoint_bpmn_metadata_lineage_graph,
        name="get_datapoint_bpmn_metadata_lineage_graph",
    ),

    # Output Layer Mapping Workflow URLs
    path("output-layer-mapping/", output_layer_mapping_workflow_views.select_table_for_mapping, name="output_layer_mapping"),
    path("output-layer-mapping/step1/", output_layer_mapping_workflow_views.select_table_for_mapping, name="output_layer_mapping_step1"),
    path("output-layer-mapping/step2/", output_layer_mapping_workflow_views.check_existing_mappings, name="output_layer_mapping_step2"),

    # Step 2 bulk operations
    path("output-layer-mapping/step2/go-back/", output_layer_mapping_workflow_views.step2_go_back, name="output_layer_mapping_step2_go_back"),
    path("output-layer-mapping/step2/apply-bulk/", output_layer_mapping_workflow_views.step2_apply_bulk, name="output_layer_mapping_step2_apply_bulk"),
    path("output-layer-mapping/step2/edit-bulk/", output_layer_mapping_workflow_views.step2_edit_bulk, name="output_layer_mapping_step2_edit_bulk"),
    path("output-layer-mapping/step2/reapply-all/", output_layer_mapping_workflow_views.step2_reapply_all, name="output_layer_mapping_step2_reapply_all"),
    path("output-layer-mapping/step2/delete-bulk/", output_layer_mapping_workflow_views.step2_delete_bulk, name="output_layer_mapping_step2_delete_bulk"),

    path("output-layer-mapping/step3/", output_layer_mapping_workflow_views.select_axis_ordinates, name="output_layer_mapping_step3"),
    path("output-layer-mapping/step3/quick-start/", output_layer_mapping_workflow_views.quick_start_variable_groups, name="output_layer_mapping_quick_start"),
    path("output-layer-mapping/step4/", output_layer_mapping_workflow_views.define_variable_breakdown, name="output_layer_mapping_step4"),
    path("output-layer-mapping/step5/", output_layer_mapping_workflow_views.edit_mappings_tabbed, name="output_layer_mapping_step5"),
    path("output-layer-mapping/step6/", output_layer_mapping_workflow_views.review_and_name_mapping, name="output_layer_mapping_step6"),
    path("output-layer-mapping/step7/", output_layer_mapping_workflow_views.generate_structures, name="output_layer_mapping_step7"),

    # Output Layer Mapping API endpoints
    path("api/output-layer-mapping/table-cells/", output_layer_mapping_workflow_views.get_table_cells_api, name="olm_get_table_cells_api"),
    path("api/output-layer-mapping/variable-domain/", output_layer_mapping_workflow_views.get_variable_domain_api, name="olm_get_variable_domain_api"),
    path("api/output-layer-mapping/filter-options/", output_layer_mapping_workflow_views.get_filter_options_api, name="olm_filter_options_api"),
    path("api/output-layer-mapping/delete-conflicts/", output_layer_mapping_workflow_views.delete_mapping_conflicts, name="olm_delete_conflicts_api"),

    # Z-axis variant management APIs
    path("api/output-layer-mapping/z-axis-siblings/", output_layer_mapping_workflow_views.get_z_axis_siblings_api, name="olm_get_z_axis_siblings_api"),
    path("api/output-layer-mapping/save-selected-z-tables/", output_layer_mapping_workflow_views.save_selected_z_tables_api, name="olm_save_selected_z_tables_api"),
    path("api/output-layer-mapping/regenerate-combinations/", output_layer_mapping_workflow_views.regenerate_combinations_api, name="olm_regenerate_combinations_api"),

    # Cube structure viewer endpoints (reusable service)
    path("api/cube-structure/<str:cube_id>/", output_layer_mapping_workflow_views.api_cube_structure, name="api_cube_structure"),
    path("cube-viewer/<str:cube_id>/", output_layer_mapping_workflow_views.cube_structure_viewer, name="cube_structure_viewer"),

    # Output Layer Viewer endpoints (Task 1 Review)
    path("api/output-layer/frameworks/", output_layer_mapping_workflow_views.api_output_layer_frameworks, name="api_output_layer_frameworks"),
    path("api/output-layer/tables/<str:framework_id>/", output_layer_mapping_workflow_views.api_output_layer_tables, name="api_output_layer_tables"),
    path("api/output-layer/detail/<str:table_id>/", output_layer_mapping_workflow_views.api_output_layer_detail, name="api_output_layer_detail"),

    path("api/get_domains/", output_layer_mapping_workflow_views.get_domains, name="api_get_domains"),
    path("create_member/", output_layer_mapping_workflow_views.create_member, name="create_member"),
    path("api/create_variable/", output_layer_mapping_workflow_views.create_variable, name="api_create_variable"),
    path("api/update_variable_domain/", output_layer_mapping_workflow_views.update_variable_domain, name="api_update_variable_domain"),
    path("api/get_variable_info/", output_layer_mapping_workflow_views.get_variable_info, name="api_get_variable_info"),
    path("api/create_domain/", output_layer_mapping_workflow_views.create_domain, name="api_create_domain"),

    # Annotated Template Visualizer
    path("annotated-template-visualizer/", annotated_template_visualizer_views.annotated_template_view, name="annotated_template_visualizer"),
    path("annotated-template/<str:table_id>/embed/", annotated_template_visualizer_views.annotated_template_embed_view, name="annotated_template_embed"),
    path("api/annotated-template/<str:table_id>/", annotated_template_visualizer_views.get_annotated_template_api, name="annotated_template_api"),
    path("export/annotated-template/<str:table_id>/excel/", annotated_template_visualizer_views.export_annotated_template_excel, name="annotated_template_export_excel"),

    # Derivation Configuration API
    path("api/derivations/available/", derivation_configuration_views.get_available_derivations, name="api_get_available_derivations"),
    path("api/derivations/config/", derivation_configuration_views.get_current_derivation_config, name="api_get_derivation_config"),
    path("api/derivations/save/", derivation_configuration_views.save_derivation_config, name="api_save_derivation_config"),
    path("api/derivations/merge/", derivation_configuration_views.merge_derived_fields, name="api_merge_derived_fields"),
    path("api/derivations/regenerate/", derivation_configuration_views.regenerate_derivation_config, name="api_regenerate_derivation_config"),
    path("api/derivations/enable-all/", derivation_configuration_views.enable_all_derivations, name="api_enable_all_derivations"),
    path("api/derivations/disable-all/", derivation_configuration_views.disable_all_derivations, name="api_disable_all_derivations"),
]
