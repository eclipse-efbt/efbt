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
import re
import json
import logging
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class TemplateInfo:
    """Information about a regulatory template"""
    id: str
    name: str
    description: str
    cell_count: int
    categories: List[str]
    format_version: str = "3.0"

@dataclass
class CellInfo:
    """Information about a specific cell within a template"""
    id: str
    suffix: str
    name: str
    template_id: str
    description: Optional[str] = None

@dataclass
class CellFilter:
    """Represents a filter condition from a cell's logic"""
    field_name: str
    values: List[str]
    operator: str = "in"
    description: Optional[str] = None

@dataclass
class CellAnalysis:
    """Analysis result for a specific cell"""
    cell_name: str
    template_id: str
    cell_suffix: str
    filters: List[CellFilter]
    referenced_table: str
    metric_field: str
    filter_count: int

@dataclass
class EntityData:
    """Data for creating test entities"""
    entity_type: str
    entity_id: str
    attributes: Dict[str, Any]
    relationships: Dict[str, Any]

@dataclass
class WebFixtureConfig:
    """Configuration for web-based fixture generation"""
    template_id: str
    cell_suffix: str
    scenario_name: str
    expected_value: int
    custom_entities: List[EntityData]
    database_prefill: bool = False
    custom_sql: List[str] = None

    def __post_init__(self):
        if self.custom_sql is None:
            self.custom_sql = []

@dataclass
class FixtureResult:
    """Result of fixture generation"""
    success: bool
    template_id: str
    cell_suffix: str
    scenario: str
    fixture_path: str
    sql_inserts: str
    sql_deletes: str
    test_config_entry: Dict[str, Any]
    errors: List[str]
    generated_files: List[str]
    test_file_path: str = ""
    test_file_content: str = ""

@dataclass
class SqlFixtureResult:
    """Result of SQL fixture generation"""
    success: bool
    sql_inserts: str
    sql_deletes: str
    statements_count: int
    errors: List[str]

@dataclass
class ConfigUpdateResult:
    """Result of test configuration update"""
    success: bool
    updated_entries: int
    config_file_path: str
    errors: List[str]

class WebFixtureService:
    """
    Web-optimized service for generating test fixtures without CLI dependencies
    """

    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.path.join(os.getcwd(), 'db.sqlite3')
        self.logger = logger
        self.cells_file = Path(__file__).parent.parent / "filter_code/report_cells.py"

    def discover_available_templates(self) -> Dict[str, TemplateInfo]:
        """Discover available regulatory templates from cell classes"""
        try:
            if not self.cells_file.exists():
                self.logger.error(f"Cell file not found: {self.cells_file}")
                return {}

            with open(self.cells_file, 'r') as f:
                content = f.read()

            # Extract template IDs from cell class names with improved pattern
            pattern = r'class Cell_([A-Z0-9_]+_REF_[A-Z_0-9]+)_(\d+_REF):'
            matches = re.findall(pattern, content)

            templates = {}
            for template_id, suffix in matches:
                if template_id not in templates:
                    templates[template_id] = []
                templates[template_id].append(suffix)

            # Convert to TemplateInfo objects
            template_info = {}
            for template_id, cells in templates.items():
                # Determine template category and description
                category = self._determine_template_category(template_id)
                description = self._generate_template_description(template_id, len(cells))

                template_info[template_id] = TemplateInfo(
                    id=template_id,
                    name=template_id.replace('_', ' '),
                    description=description,
                    cell_count=len(cells),
                    categories=[category],
                    format_version="3.0"
                )

            self.logger.info(f"Discovered {len(template_info)} templates with {sum(t.cell_count for t in template_info.values())} total cells")
            return template_info

        except Exception as e:
            self.logger.error(f"Error discovering templates: {e}")
            return {}

    def get_template_cells(self, template_id: str) -> List[CellInfo]:
        """Get all cells for a specific template"""
        try:
            templates = self.discover_available_templates()
            template_info = templates.get(template_id)

            if not template_info:
                self.logger.warning(f"Template {template_id} not found")
                return []

            # Re-discover to get cell suffixes
            if not self.cells_file.exists():
                return []

            with open(self.cells_file, 'r') as f:
                content = f.read()

            pattern = rf'class Cell_{re.escape(template_id)}_(\d+_REF):'
            matches = re.findall(pattern, content)

            cells = []
            for suffix in matches:
                cell_info = CellInfo(
                    id=suffix,
                    suffix=suffix,
                    name=f"{template_id}_{suffix}",
                    template_id=template_id,
                    description=f"Cell {suffix} for template {template_id}"
                )
                cells.append(cell_info)

            self.logger.info(f"Found {len(cells)} cells for template {template_id}")
            return cells

        except Exception as e:
            self.logger.error(f"Error getting cells for template {template_id}: {e}")
            return []

    def analyze_cell_requirements(self, template_id: str, cell_suffix: str) -> Optional[CellAnalysis]:
        """Analyze a specific cell to extract its filter conditions and requirements"""
        try:
            cell_name = f"Cell_{template_id}_{cell_suffix}"

            if not self.cells_file.exists():
                self.logger.error(f"Cell file not found: {self.cells_file}")
                return None

            with open(self.cells_file, 'r') as f:
                content = f.read()

            # Find the cell class definition
            class_pattern = rf'class {re.escape(cell_name)}:(.*?)(?=class|\Z)'
            class_match = re.search(class_pattern, content, re.DOTALL)

            if not class_match:
                self.logger.error(f"Cell class {cell_name} not found")
                return None

            class_content = class_match.group(1)

            # Extract table reference
            table_pattern = r'(\w+)_Table = None'
            table_match = re.search(table_pattern, class_content)
            referenced_table = table_match.group(1) if table_match else f"{template_id}_Table"

            # Extract metric field from metric_value method
            metric_pattern = r'total \+= item\.(\w+)\(\)'
            metric_match = re.search(metric_pattern, class_content)
            metric_field = metric_match.group(1) if metric_match else "CRRYNG_AMNT"

            # Extract filter conditions
            filters = self._extract_filters_from_cell_content(class_content)

            analysis = CellAnalysis(
                cell_name=cell_name,
                template_id=template_id,
                cell_suffix=cell_suffix,
                filters=filters,
                referenced_table=referenced_table,
                metric_field=metric_field,
                filter_count=len(filters)
            )

            self.logger.info(f"Analyzed cell {cell_name}: {len(filters)} filters found")
            return analysis

        except Exception as e:
            self.logger.error(f"Error analyzing cell {template_id}_{cell_suffix}: {e}")
            return None

    def create_fixture_set(self, template_id: str, cell_suffix: str, scenario: str, expected_value: int,
                          custom_entities: List[EntityData] = None, database_prefill: bool = False) -> FixtureResult:
        """Create a complete fixture set for a specific cell"""
        try:
            # Create configuration
            config = WebFixtureConfig(
                template_id=template_id,
                cell_suffix=cell_suffix,
                scenario_name=scenario,
                expected_value=expected_value,
                custom_entities=custom_entities or [],
                database_prefill=database_prefill
            )

            # Generate fixtures
            return self._generate_complete_fixture_set(config)

        except Exception as e:
            self.logger.error(f"Error creating fixture set: {e}")
            return FixtureResult(
                success=False,
                template_id=template_id,
                cell_suffix=cell_suffix,
                scenario=scenario,
                fixture_path="",
                sql_inserts="",
                sql_deletes="",
                test_config_entry={},
                errors=[str(e)],
                generated_files=[],
                test_file_path="",
                test_file_content=""
            )

    def generate_sql_fixtures(self, fixture_config: WebFixtureConfig) -> SqlFixtureResult:
        """Generate SQL fixtures based on configuration"""
        try:
            # Import SQL fixture builder
            from ...utils.datapoint_test_run.sql_fixture_builder import SQLFixtureBuilder, FixtureTemplate, EntityData as SQLEntityData

            builder = SQLFixtureBuilder(self.db_path)

            # Convert entities to SQL builder format
            sql_entities = []
            for entity in fixture_config.custom_entities:
                sql_entity = SQLEntityData(
                    entity_type=entity.entity_type,
                    entity_id=entity.entity_id,
                    attributes=entity.attributes,
                    relationships=entity.relationships
                )
                sql_entities.append(sql_entity)

            # Create fixture template
            if sql_entities:
                # Use custom entities
                template = FixtureTemplate(
                    template_id=fixture_config.template_id,
                    cell_suffix=fixture_config.cell_suffix,
                    scenario_name=fixture_config.scenario_name,
                    expected_value=fixture_config.expected_value,
                    entities=sql_entities,
                    custom_sql=fixture_config.custom_sql
                )
            else:
                # Generate default template
                template = builder.generate_fixture_template_for_cell(
                    fixture_config.template_id,
                    fixture_config.cell_suffix,
                    fixture_config.expected_value
                )
                template.scenario_name = fixture_config.scenario_name

            # Build SQL statements
            sql_statements = builder.build_sql_from_template(template)
            sql_inserts = '\n'.join(sql_statements)

            # Generate delete statements
            sql_deletes = self._generate_delete_statements(sql_statements)

            return SqlFixtureResult(
                success=True,
                sql_inserts=sql_inserts,
                sql_deletes=sql_deletes,
                statements_count=len(sql_statements),
                errors=[]
            )

        except Exception as e:
            self.logger.error(f"Error generating SQL fixtures: {e}")
            return SqlFixtureResult(
                success=False,
                sql_inserts="",
                sql_deletes="",
                statements_count=0,
                errors=[str(e)]
            )

    def update_test_configuration(self, fixture_results: List[FixtureResult]) -> ConfigUpdateResult:
        """Update test configuration file with new fixture entries"""
        try:
            config_file_path = Path("tests/configuration_file_tests.json")

            # Load existing configuration
            if config_file_path.exists():
                with open(config_file_path, 'r') as f:
                    config_data = json.load(f)
            else:
                config_data = {"tests": []}

            updated_count = 0
            errors = []

            for result in fixture_results:
                if not result.success:
                    continue

                try:
                    # Create new test entry
                    new_test = {
                        "reg_tid": result.template_id,
                        "dp_suffix": result.cell_suffix,
                        "dp_value": result.test_config_entry.get("expected_value", 83491250),
                        "scenario": result.scenario
                    }

                    # Check if test already exists
                    existing_index = None
                    for i, test in enumerate(config_data["tests"]):
                        if (test["reg_tid"] == result.template_id and
                            test["dp_suffix"] == result.cell_suffix and
                            test["scenario"] == result.scenario):
                            existing_index = i
                            break

                    if existing_index is not None:
                        config_data["tests"][existing_index] = new_test
                    else:
                        config_data["tests"].append(new_test)

                    updated_count += 1

                except Exception as e:
                    errors.append(f"Failed to update config for {result.template_id}_{result.cell_suffix}: {str(e)}")

            # Save updated configuration
            with open(config_file_path, 'w') as f:
                json.dump(config_data, f, indent=2)

            return ConfigUpdateResult(
                success=len(errors) == 0,
                updated_entries=updated_count,
                config_file_path=str(config_file_path),
                errors=errors
            )

        except Exception as e:
            self.logger.error(f"Error updating test configuration: {e}")
            return ConfigUpdateResult(
                success=False,
                updated_entries=0,
                config_file_path="",
                errors=[str(e)]
            )

    def generate_test_file(self, fixture_config: WebFixtureConfig) -> Tuple[str, str]:
        """Generate test file content and path using TestCodeGenerator"""
        try:
            from ...utils.datapoint_test_run.generator_for_tests import TestCodeGenerator

            datapoint_id = f"{fixture_config.template_id}_{fixture_config.cell_suffix}"
            cell_class = f"Cell_{fixture_config.template_id}_{fixture_config.cell_suffix}"

            # Generate test code using existing generator
            import_code = TestCodeGenerator.create_import_statements(cell_class)
            test_code = TestCodeGenerator.create_test_functions(fixture_config.expected_value, datapoint_id)

            # Combine import and test code
            test_file_content = f"{import_code}\n\n{test_code}"

            # Generate test file path following the established convention
            test_file_path = f"tests/test_cell_{fixture_config.template_id.lower()}_{fixture_config.cell_suffix.lower()}__{fixture_config.scenario_name}.py"

            return test_file_path, test_file_content

        except Exception as e:
            self.logger.error(f"Error generating test file: {e}")
            return "", ""

    def get_test_status_for_template(self, template_id: str) -> Dict[str, List[str]]:
        """Get test status for all cells in a template"""
        try:
            config_file_path = Path("tests/configuration_file_tests.json")

            if not config_file_path.exists():
                return {}

            with open(config_file_path, 'r') as f:
                config_data = json.load(f)

            tests = config_data.get('tests', [])
            cell_tests = {}

            for test in tests:
                if test.get('reg_tid') == template_id:
                    cell_suffix = test.get('dp_suffix', '')
                    scenario = test.get('scenario', 'base')

                    if cell_suffix not in cell_tests:
                        cell_tests[cell_suffix] = []
                    cell_tests[cell_suffix].append(scenario)

            return cell_tests

        except Exception as e:
            self.logger.error(f"Error getting test status for template {template_id}: {e}")
            return {}

    def has_test(self, template_id: str, cell_suffix: str, scenario: str = None) -> bool:
        """Check if a specific cell has tests"""
        try:
            cell_tests = self.get_test_status_for_template(template_id)

            if cell_suffix not in cell_tests:
                return False

            if scenario is None:
                return len(cell_tests[cell_suffix]) > 0
            else:
                return scenario in cell_tests[cell_suffix]

        except Exception as e:
            self.logger.error(f"Error checking test status for {template_id}_{cell_suffix}: {e}")
            return False

    def prefill_from_database(self, template_id: str, cell_suffix: str) -> Optional[Dict[str, Any]]:
        """Analyze database to prefill configuration with realistic data"""
        try:
            if not os.path.exists(self.db_path):
                self.logger.warning(f"Database not found: {self.db_path}")
                return None

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            sample_data = {}

            # Check for existing instrument data
            cursor.execute("""
                SELECT CRRYNG_AMNT, ACCNTNG_CLSSFCTN, IMPRMNT_STTS
                FROM pybirdai_blnc_sht_rcgnsd_fnncl_asst_instrmnt
                WHERE CRRYNG_AMNT IS NOT NULL
                LIMIT 5
            """)

            instruments = cursor.fetchall()
            if instruments:
                sample_data['instruments'] = [
                    {
                        'CRRYNG_AMNT': row[0],
                        'ACCNTNG_CLSSFCTN': row[1],
                        'IMPRMNT_STTS': row[2]
                    } for row in instruments
                ]

            # Check for party data
            cursor.execute("""
                SELECT INSTTTNL_SCTR, ECNMC_ACTVTY, PRTY_TYP
                FROM pybirdai_prty
                WHERE INSTTTNL_SCTR IS NOT NULL
                LIMIT 5
            """)

            parties = cursor.fetchall()
            if parties:
                sample_data['parties'] = [
                    {
                        'INSTTTNL_SCTR': row[0],
                        'ECNMC_ACTVTY': row[1],
                        'PRTY_TYP': row[2]
                    } for row in parties
                ]

            conn.close()
            return sample_data

        except Exception as e:
            self.logger.error(f"Failed to prefill from database: {e}")
            return None

    # Private helper methods
    def _determine_template_category(self, template_id: str) -> str:
        """Determine the category of a template based on its ID"""
        if "FINREP" in template_id:
            return "FINREP"
        elif "COREP" in template_id:
            return "COREP"
        elif "AE" in template_id:
            return "Asset Encumbrance"
        else:
            return "Other"

    def _generate_template_description(self, template_id: str, cell_count: int) -> str:
        """Generate a human-readable description for a template"""
        category = self._determine_template_category(template_id)
        return f"{category} regulatory template with {cell_count} data cells"

    def _extract_filters_from_cell_content(self, class_content: str) -> List[CellFilter]:
        """Extract filter conditions from the calc_referenced_items method"""
        filters = []

        try:
            # Find all filter blocks with improved pattern matching
            or_block_pattern = r'if\s+(.*?)\s*:\s*pass\s*else:\s*filter_passed = False'
            or_blocks = re.findall(or_block_pattern, class_content, re.DOTALL)

            for block in or_blocks:
                # Extract field name and values from OR conditions
                condition_pattern = r'\(item\.(\w+)\(\)\s*==\s*[\'"]([^\'"]+)[\'"]\)'
                conditions = re.findall(condition_pattern, block)

                if conditions:
                    # Group by field name
                    field_groups = {}
                    for field_name, value in conditions:
                        if field_name not in field_groups:
                            field_groups[field_name] = []
                        field_groups[field_name].append(value)

                    # Create filter objects
                    for field_name, values in field_groups.items():
                        filter_obj = CellFilter(
                            field_name=field_name,
                            values=values,
                            operator="in",
                            description=f"Filter on {field_name} field"
                        )
                        filters.append(filter_obj)

        except Exception as e:
            self.logger.warning(f"Error extracting filters: {e}")

        return filters

    def _generate_complete_fixture_set(self, config: WebFixtureConfig) -> FixtureResult:
        """Generate a complete fixture set including files and configuration"""
        try:
            # Create fixtures directory
            fixtures_dir = Path(f"tests/fixtures/templates/{config.template_id}/{config.cell_suffix}/{config.scenario_name}")
            fixtures_dir.mkdir(parents=True, exist_ok=True)

            generated_files = []

            # Create __init__.py files for package discovery
            self._create_init_files(fixtures_dir)
            generated_files.extend(self._get_init_file_paths(fixtures_dir))

            # Generate SQL fixtures
            sql_result = self.generate_sql_fixtures(config)

            if not sql_result.success:
                return FixtureResult(
                    success=False,
                    template_id=config.template_id,
                    cell_suffix=config.cell_suffix,
                    scenario=config.scenario_name,
                    fixture_path=str(fixtures_dir),
                    sql_inserts="",
                    sql_deletes="",
                    test_config_entry={},
                    errors=sql_result.errors,
                    generated_files=generated_files,
                    test_file_path="",
                    test_file_content=""
                )

            # Write SQL files
            inserts_file = fixtures_dir / "sql_inserts.sql"
            deletes_file = fixtures_dir / "sql_deletes.sql"

            with open(inserts_file, 'w') as f:
                f.write(sql_result.sql_inserts)
            generated_files.append(str(inserts_file))

            with open(deletes_file, 'w') as f:
                f.write(sql_result.sql_deletes)
            generated_files.append(str(deletes_file))

            # Generate test file
            test_file_path, test_file_content = self.generate_test_file(config)

            if test_file_path and test_file_content:
                # Write test file to disk
                test_file_full_path = Path(test_file_path)
                test_file_full_path.parent.mkdir(parents=True, exist_ok=True)

                with open(test_file_full_path, 'w') as f:
                    f.write(test_file_content)
                generated_files.append(str(test_file_full_path))

                self.logger.info(f"Generated test file: {test_file_path}")
            else:
                self.logger.warning("Failed to generate test file")

            # Create test configuration entry
            test_config_entry = {
                "reg_tid": config.template_id,
                "dp_suffix": config.cell_suffix,
                "dp_value": config.expected_value,
                "scenario": config.scenario_name
            }

            return FixtureResult(
                success=True,
                template_id=config.template_id,
                cell_suffix=config.cell_suffix,
                scenario=config.scenario_name,
                fixture_path=str(fixtures_dir),
                sql_inserts=sql_result.sql_inserts,
                sql_deletes=sql_result.sql_deletes,
                test_config_entry=test_config_entry,
                errors=[],
                generated_files=generated_files,
                test_file_path=test_file_path,
                test_file_content=test_file_content
            )

        except Exception as e:
            self.logger.error(f"Error generating complete fixture set: {e}")
            return FixtureResult(
                success=False,
                template_id=config.template_id,
                cell_suffix=config.cell_suffix,
                scenario=config.scenario_name,
                fixture_path="",
                sql_inserts="",
                sql_deletes="",
                test_config_entry={},
                errors=[str(e)],
                generated_files=[],
                test_file_path="",
                test_file_content=""
            )

    def _create_init_files(self, fixtures_dir: Path):
        """Create __init__.py files for package discovery"""
        # Create __init__.py in the scenario directory
        init_file = fixtures_dir / "__init__.py"
        if not init_file.exists():
            init_file.write_text("# Auto-generated by fixture generator\\n")

        # Create __init__.py files in parent directories
        parent_dirs = [
            fixtures_dir.parent,  # cell_suffix dir
            fixtures_dir.parent.parent,  # template_id dir
            fixtures_dir.parent.parent.parent,  # templates dir
            fixtures_dir.parent.parent.parent.parent  # fixtures dir
        ]

        for parent_dir in parent_dirs:
            if parent_dir.name in ['fixtures', 'templates'] or parent_dir.name.endswith('_REF') or 'REF_FINREP' in parent_dir.name:
                init_file = parent_dir / "__init__.py"
                if not init_file.exists():
                    init_file.write_text("# Auto-generated by fixture generator\\n")

    def _get_init_file_paths(self, fixtures_dir: Path) -> List[str]:
        """Get paths of all __init__.py files that would be created"""
        paths = [str(fixtures_dir / "__init__.py")]

        parent_dirs = [
            fixtures_dir.parent,
            fixtures_dir.parent.parent,
            fixtures_dir.parent.parent.parent,
            fixtures_dir.parent.parent.parent.parent
        ]

        for parent_dir in parent_dirs:
            if parent_dir.name in ['fixtures', 'templates'] or parent_dir.name.endswith('_REF') or 'REF_FINREP' in parent_dir.name:
                paths.append(str(parent_dir / "__init__.py"))

        return paths

    def _generate_delete_statements(self, insert_statements: List[str]) -> str:
        """Generate DELETE statements from INSERT statements"""
        try:
            # Use existing delete generator
            from ...utils.datapoint_test_run.generator_delete_fixtures import generate_delete_statements_from_inserts

            delete_statements = generate_delete_statements_from_inserts(insert_statements)
            return '\n'.join(delete_statements)

        except Exception as e:
            self.logger.warning(f"Could not generate delete statements: {e}")
            # Fallback: basic DELETE generation
            delete_statements = []
            for stmt in insert_statements:
                if stmt.strip().upper().startswith('INSERT INTO'):
                    # Extract table name
                    parts = stmt.split()
                    if len(parts) > 2:
                        table_name = parts[2].split('(')[0]
                        delete_statements.append(f"DELETE FROM {table_name} WHERE rowid = 1;")

            return '\n'.join(delete_statements)