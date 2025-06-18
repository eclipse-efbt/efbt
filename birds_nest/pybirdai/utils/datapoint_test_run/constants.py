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
#

import os
# Define constants
# Base directories
TESTS_DIR = "tests"
PYBIRDAI_DIR = "pybirdai"
UTILS_DIR = f"utils{os.sep}datapoint_test_run"
TEST_RESULTS_DIR = os.path.join(TESTS_DIR, "test_results")

# Test results folders
TEST_RESULTS_TXT_FOLDER = os.path.join(TEST_RESULTS_DIR, "txt")
TEST_RESULTS_JSON_FOLDER = os.path.join(TEST_RESULTS_DIR, "json")

# Utility file paths
GENERATOR_FILE_PATH = os.path.join(PYBIRDAI_DIR, UTILS_DIR, "generator_for_tests.py")
PARSER_FILE_PATH = os.path.join(PYBIRDAI_DIR, UTILS_DIR, "parser_for_tests.py")

# SQL file names
SQL_INSERT_FILE_NAME = "sql_inserts.sql"
SQL_DELETE_FILE_NAME = "sql_deletes.sql"

# Define argument defaults as constants
DEFAULT_UV = "True"
DEFAULT_DP_VALUE = 83491250
DEFAULT_REG_TID = "F_05_01_REF_FINREP_3_0"
DEFAULT_DP_SUFFIX = "152589_REF"
