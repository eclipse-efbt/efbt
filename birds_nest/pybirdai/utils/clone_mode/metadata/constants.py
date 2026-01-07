# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
"""
Constants for process metadata management.

Schema versioning and workflow configuration constants.
"""

# Schema version for forward compatibility
# 1.0 - Initial version
# 1.1 - Added primary_framework, framework info per workflow, overall status
# 1.2 - Added export completeness tracking (is_complete, total_steps, steps_status)
SCHEMA_VERSION = "1.2"

# Step counts for each workflow type
WORKFLOW_STEP_COUNTS = {
    'main': 4,
    'dpm_eba': 6,
    'dpm_github': 4,
    'anacredit': 4,
}

# Final step (tests) that marks workflow as complete
COMPLETION_STEPS = {
    'main': 4,           # Task 4 (tests)
    'dpm_eba': 6,        # Step 6 (Execute DPM Tests)
    'dpm_github': 4,     # Step 4 (Execute Tests)
    'anacredit': 4,      # Step 4 (Tests)
}

# Valid restart points for clone mode (metadata parsing steps only)
VALID_RESTART_POINTS = {
    'main': ['task1_database_setup', 'task2_data_import'],
    'dpm': ['step1_extract_metadata', 'step2_process_tables'],
    'anacredit': ['step1', 'step2', 'step3'],
}

# Steps that involve code generation (not valid for clone)
CODE_GENERATION_STEPS = {
    'main': ['task3_hierarchy_conversion', 'task4_code_generation'],
    'dpm': ['step3_output_layers', 'step4_transformation_rules', 'step5_generate_code', 'step6_execute_tests'],
    'anacredit': ['step4', 'step5'],
}
