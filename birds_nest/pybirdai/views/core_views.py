# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
"""
Core views orchestrator module.

This module serves as a thin orchestrator that re-exports all view functions
from the pybirdai.views.core package for backward compatibility.

All actual view implementations are in the pybirdai.views.core submodules:
- navigation_views: Basic page rendering
- process_execution_views: Data processing tasks
- variable_mapping_views: Variable mapping CRUD
- member_mapping_views: Member mapping CRUD
- cube_link_views: Cube link CRUD
- mapping_definition_views: Mapping definition CRUD
- combination_views: Combination and output layer CRUD
- csv_views: CSV import/export and mapping operations
- analysis_views: Duplicate detection, gap analysis
- setup_views: Full setup and test reports
- loading_helpers: Loading page patterns
- view_helpers: Common view patterns
- cache_manager: SDDContext cache operations
- semantic_integration_views: Semantic integration editor
- member_hierarchy_views: Member hierarchy editor
- automode_views: Automode configuration and execution
"""

# Re-export all functions from the core package for backward compatibility
from pybirdai.views.core import (
    # Navigation
    home_view,
    automode_view,
    show_report,
    bird_diffs_and_corrections,
    # Process execution
    run_create_joins_meta_data,
    create_django_models,
    run_create_python_joins,
    run_delete_joins_meta_data,
    run_delete_mappings,
    run_delete_output_concepts,
    delete_existing_contents_of_bird_metadata_database,
    run_import_semantic_integrations_from_website,
    run_import_input_model_from_sqldev,
    run_import_hierarchies,
    import_report_templates,
    run_create_filters,
    run_create_executable_filters,
    run_create_executable_filters_from_db,
    run_create_python_joins_from_db,
    run_create_python_transformations_from_db,
    convert_ldm_to_sdd_hierarchies,
    upload_sqldev_eil_files,
    upload_sqldev_eldm_files,
    upload_technical_export_files,
    upload_joins_configuration,
    execute_data_point,
    execute_datapoint_with_lineage,
    # Variable mappings
    edit_variable_mappings,
    edit_variable_mapping_items,
    create_variable_mapping_item,
    create_variable_mapping,
    delete_variable_mapping,
    delete_variable_mapping_item,
    # Member mappings
    edit_member_mappings,
    edit_member_mapping_items,
    create_member_mapping,
    add_member_mapping_item,
    delete_member_mapping,
    delete_member_mapping_item,
    view_member_mapping_items_by_row,
    # Cube links
    edit_cube_links,
    edit_cube_structure_item_links,
    delete_cube_link,
    delete_cube_structure_item_link,
    delete_cube_structure_item_link_dupl,
    bulk_delete_cube_structure_item_links,
    add_cube_structure_item_link,
    add_cube_link,
    # Mapping definitions
    edit_mapping_definitions,
    edit_mapping_to_cubes,
    create_mapping_definition,
    create_mapping_to_cube,
    delete_mapping_definition,
    delete_mapping_to_cube,
    # Combinations
    combinations,
    combination_items,
    output_layers,
    delete_combination,
    delete_combination_item,
    delete_cube,
    # CSV and mapping import/export
    list_lineage_files,
    view_csv_file,
    view_ldm_to_sdd_results,
    import_members_from_csv,
    import_variables_from_csv,
    export_database_to_csv,
    import_bird_data_from_csv_export,
    load_variables_from_csv_file,
    export_mapping_template,
    export_mapping_data,
    import_mapping_from_csv,
    delete_mapping_row,
    duplicate_mapping,
    update_mapping_row,
    # Analysis
    DuplicatePrimaryMemberIdListView,
    JoinIdentifierListView,
    duplicate_primary_member_id_list,
    show_gaps,
    return_cubelink_visualisation,
    # Setup
    execute_full_setup_core,
    run_full_setup,
    test_report_view,
    # Helpers
    create_response_with_loading,
    create_response_with_loading_extended,
    serialize_datetime,
    paginated_modelformset_view,
    delete_item,
    # Semantic integration
    semantic_integration_editor,
    add_variable_endpoint,
    edit_mapping_endpoint,
    get_domain_members,
    get_mapping_details,
    # Member hierarchy
    member_hierarchy_editor,
    add_member_to_hierarchy,
    delete_member_from_hierarchy,
    edit_hierarchy_node,
    get_members_by_domain,
    get_subdomain_enumerations,
    get_hierarchy_json,
    save_hierarchy_json,
    get_domain_members_json,
    get_available_hierarchies_json,
    create_hierarchy_from_visualization,
    create_hierarchy_simple,
    create_member_json,
    # Automode
    automode_create_database,
    automode_import_bird_metamodel_from_website,
    test_automode_components,
    run_fetch_curated_resources,
    automode_configure,
    automode_execute,
    automode_continue_post_restart,
    automode_debug_config,
    automode_status,
)

# Define __all__ for explicit public API
__all__ = [
    # Navigation
    'home_view',
    'automode_view',
    'show_report',
    'bird_diffs_and_corrections',
    # Process execution
    'run_create_joins_meta_data',
    'create_django_models',
    'run_create_python_joins',
    'run_delete_joins_meta_data',
    'run_delete_mappings',
    'run_delete_output_concepts',
    'delete_existing_contents_of_bird_metadata_database',
    'run_import_semantic_integrations_from_website',
    'run_import_input_model_from_sqldev',
    'run_import_hierarchies',
    'import_report_templates',
    'run_create_filters',
    'run_create_executable_filters',
    'run_create_executable_filters_from_db',
    'run_create_python_joins_from_db',
    'run_create_python_transformations_from_db',
    'convert_ldm_to_sdd_hierarchies',
    'upload_sqldev_eil_files',
    'upload_sqldev_eldm_files',
    'upload_technical_export_files',
    'upload_joins_configuration',
    'execute_data_point',
    'execute_datapoint_with_lineage',
    # Variable mappings
    'edit_variable_mappings',
    'edit_variable_mapping_items',
    'create_variable_mapping_item',
    'create_variable_mapping',
    'delete_variable_mapping',
    'delete_variable_mapping_item',
    # Member mappings
    'edit_member_mappings',
    'edit_member_mapping_items',
    'create_member_mapping',
    'add_member_mapping_item',
    'delete_member_mapping',
    'delete_member_mapping_item',
    'view_member_mapping_items_by_row',
    # Cube links
    'edit_cube_links',
    'edit_cube_structure_item_links',
    'delete_cube_link',
    'delete_cube_structure_item_link',
    'delete_cube_structure_item_link_dupl',
    'bulk_delete_cube_structure_item_links',
    'add_cube_structure_item_link',
    'add_cube_link',
    # Mapping definitions
    'edit_mapping_definitions',
    'edit_mapping_to_cubes',
    'create_mapping_definition',
    'create_mapping_to_cube',
    'delete_mapping_definition',
    'delete_mapping_to_cube',
    # Combinations
    'combinations',
    'combination_items',
    'output_layers',
    'delete_combination',
    'delete_combination_item',
    'delete_cube',
    # CSV and mapping import/export
    'list_lineage_files',
    'view_csv_file',
    'view_ldm_to_sdd_results',
    'import_members_from_csv',
    'import_variables_from_csv',
    'export_database_to_csv',
    'import_bird_data_from_csv_export',
    'load_variables_from_csv_file',
    'export_mapping_template',
    'export_mapping_data',
    'import_mapping_from_csv',
    'delete_mapping_row',
    'duplicate_mapping',
    'update_mapping_row',
    # Analysis
    'DuplicatePrimaryMemberIdListView',
    'JoinIdentifierListView',
    'duplicate_primary_member_id_list',
    'show_gaps',
    'return_cubelink_visualisation',
    # Setup
    'execute_full_setup_core',
    'run_full_setup',
    'test_report_view',
    # Helpers
    'create_response_with_loading',
    'create_response_with_loading_extended',
    'serialize_datetime',
    'paginated_modelformset_view',
    'delete_item',
    # Semantic integration
    'semantic_integration_editor',
    'add_variable_endpoint',
    'edit_mapping_endpoint',
    'get_domain_members',
    'get_mapping_details',
    # Member hierarchy
    'member_hierarchy_editor',
    'add_member_to_hierarchy',
    'delete_member_from_hierarchy',
    'edit_hierarchy_node',
    'get_members_by_domain',
    'get_subdomain_enumerations',
    'get_hierarchy_json',
    'save_hierarchy_json',
    'get_domain_members_json',
    'get_available_hierarchies_json',
    'create_hierarchy_from_visualization',
    'create_hierarchy_simple',
    'create_member_json',
    # Automode
    'automode_create_database',
    'automode_import_bird_metamodel_from_website',
    'test_automode_components',
    'run_fetch_curated_resources',
    'automode_configure',
    'automode_execute',
    'automode_continue_post_restart',
    'automode_debug_config',
    'automode_status',
]
