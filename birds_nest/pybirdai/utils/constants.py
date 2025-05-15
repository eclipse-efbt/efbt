"""
Copyright 2025 Arfa Digital Consulting

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import os
# Define constants
# Base directories
TESTS_DIR = "tests"
PYBIRDAI_DIR = "pybirdai"
UTILS_DIR = "utils"
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
