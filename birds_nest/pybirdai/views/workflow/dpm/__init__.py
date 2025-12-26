# coding=UTF-8
# DPM workflow views

from .execution import execute_dpm_step, get_dpm_status
from .cubes import get_cubes_for_dpm_step3, api_dpm_cubes
from .tables import get_available_tables_for_selection, save_table_selection, manage_table_presets
from .review import workflow_dpm_review

# GitHub-based DPM workflow views (4-step flow)
from .github_execution import (
    execute_github_dpm_step,
    get_github_dpm_status,
    workflow_github_dpm_review,
    configure_github_dpm_source,
    validate_github_dpm_package,
    get_github_dpm_task_grid,
)

# Output Layer Mapping views
from .output_layer_mapping_views import (
    select_table_for_mapping,
    check_existing_mappings,
    step2_go_back,
    step2_apply_bulk,
    step2_edit_bulk,
    step2_reapply_all,
    step2_delete_bulk,
    select_axis_ordinates,
    quick_start_variable_groups,
    define_variable_breakdown,
    edit_mappings_tabbed,
    review_and_name_mapping,
    generate_structures,
    get_table_cells_api,
    get_variable_domain_api,
    get_filter_options_api,
    delete_mapping_conflicts,
    get_z_axis_siblings_api,
    save_selected_z_tables_api,
    regenerate_combinations_api,
    api_cube_structure,
    cube_structure_viewer,
    api_output_layer_frameworks,
    api_output_layer_tables,
    api_output_layer_detail,
    get_domains,
    create_member,
    create_variable,
    update_variable_domain,
    get_variable_info,
    create_domain,
)

__all__ = [
    'execute_dpm_step', 'get_dpm_status',
    'get_cubes_for_dpm_step3', 'api_dpm_cubes',
    'get_available_tables_for_selection', 'save_table_selection', 'manage_table_presets',
    'workflow_dpm_review',
    # GitHub-based DPM workflow
    'execute_github_dpm_step', 'get_github_dpm_status', 'workflow_github_dpm_review',
    'configure_github_dpm_source', 'validate_github_dpm_package', 'get_github_dpm_task_grid',
    # Output Layer Mapping
    'select_table_for_mapping', 'check_existing_mappings',
    'step2_go_back', 'step2_apply_bulk', 'step2_edit_bulk', 'step2_reapply_all', 'step2_delete_bulk',
    'select_axis_ordinates', 'quick_start_variable_groups', 'define_variable_breakdown',
    'edit_mappings_tabbed', 'review_and_name_mapping', 'generate_structures',
    'get_table_cells_api', 'get_variable_domain_api', 'get_filter_options_api', 'delete_mapping_conflicts',
    'get_z_axis_siblings_api', 'save_selected_z_tables_api', 'regenerate_combinations_api',
    'api_cube_structure', 'cube_structure_viewer',
    'api_output_layer_frameworks', 'api_output_layer_tables', 'api_output_layer_detail',
    'get_domains', 'create_member', 'create_variable', 'update_variable_domain', 'get_variable_info', 'create_domain',
]
