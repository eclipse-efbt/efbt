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
import sys
import re
import json
import sqlite3
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from .generator_delete_fixtures import process_sql_file
from .sql_fixture_builder import SQLFixtureBuilder, FixtureTemplate, EntityData

def return_logger(__file_name__: str):
    return logging.getLogger(__file_name__)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

@dataclass
class CellFilter:
    """Represents a filter condition from a cell's calc_referenced_items method"""
    field_name: str
    values: List[str]
    operator: str = "in"  # "in", "equals", "not_in", etc.

@dataclass
class CellAnalysis:
    """Analysis result for a specific cell class"""
    cell_name: str
    template_id: str
    cell_suffix: str
    filters: List[CellFilter]
    referenced_table: str
    metric_field: str

@dataclass
class FixtureConfig:
    """Configuration for generating fixtures"""
    template_id: str
    cell_suffix: str
    scenario_name: str
    expected_value: int
    custom_data: Dict[str, Any]

class FixtureGenerator:
    """
    Main class for generating test fixtures based on cell analysis and user configuration
    """

    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.path.join(os.getcwd(), 'db.sqlite3')
        self.logger = return_logger(str(Path(__file__).resolve()).rsplit("/", 1)[-1])
        self.cells_file = Path(__file__).parent.parent.parent / "process_steps/filter_code/report_cells.py"
        self.sql_builder = SQLFixtureBuilder(db_path)

    def setup_environment(self):
        """Setup Django environment"""
        os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'

        try:
            from django.conf import settings
            from django.core.wsgi import get_wsgi_application
            application = get_wsgi_application()
            self.logger.info("Django environment setup successful")
        except ImportError as e:
            self.logger.error(f"Failed to setup Django environment: {e}")
            raise RuntimeError("Django not available in current environment") from e

    def discover_templates(self) -> Dict[str, List[str]]:
        """Discover available regulatory templates from cell classes"""
        if not self.cells_file.exists():
            self.logger.error(f"Cell file not found: {self.cells_file}")
            return {}

        with open(self.cells_file, 'r') as f:
            content = f.read()

        # Extract template IDs from cell class names
        pattern = r'class Cell_([A-Z0-9_]+)_(\d+_REF):'
        matches = re.findall(pattern, content)

        templates = {}
        for template_id, suffix in matches:
            if template_id not in templates:
                templates[template_id] = []
            templates[template_id].append(suffix)

        return templates

    def discover_cells_for_template(self, template_id: str) -> List[str]:
        """Get all cell suffixes for a specific template"""
        templates = self.discover_templates()
        return templates.get(template_id, [])

    def analyze_cell(self, template_id: str, cell_suffix: str) -> Optional[CellAnalysis]:
        """Analyze a specific cell to extract its filter conditions and requirements"""
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

        return CellAnalysis(
            cell_name=cell_name,
            template_id=template_id,
            cell_suffix=cell_suffix,
            filters=filters,
            referenced_table=referenced_table,
            metric_field=metric_field
        )

    def _extract_filters_from_cell_content(self, class_content: str) -> List[CellFilter]:
        """Extract filter conditions from the calc_referenced_items method"""
        filters = []

        # Find all filter blocks
        filter_pattern = r'if\s+.*?\(item\.(\w+)\(\)\s*==\s*[\'"]([^\'"]+)[\'"].*?(?=if|else|$)'

        # More comprehensive pattern to capture multiple OR conditions
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
                    filters.append(CellFilter(
                        field_name=field_name,
                        values=values,
                        operator="in"
                    ))

        return filters

    def get_database_schema(self) -> Dict[str, List[str]]:
        """Get database schema from SQLite database"""
        if not os.path.exists(self.db_path):
            self.logger.warning(f"Database file not found: {self.db_path}")
            return {}

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Get all table names
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            schema = {}
            for (table_name,) in tables:
                if table_name.startswith('pybirdai_'):
                    # Get column information
                    cursor.execute(f"PRAGMA table_info({table_name});")
                    columns = cursor.fetchall()
                    schema[table_name] = [col[1] for col in columns]  # col[1] is column name

            conn.close()
            return schema

        except Exception as e:
            self.logger.error(f"Error reading database schema: {e}")
            return {}

    def generate_json_schema(self, cell_analysis: CellAnalysis) -> Dict[str, Any]:
        """Generate a comprehensive JSON schema template for user configuration"""
        schema = {
            "template_id": cell_analysis.template_id,
            "cell_suffix": cell_analysis.cell_suffix,
            "scenario_name": "loan_and_guarantee_scenario_1",
            "expected_value": 83491250,
            "description": f"Configuration for {cell_analysis.cell_name}",
            "cell_analysis": {
                "referenced_table": cell_analysis.referenced_table,
                "metric_field": cell_analysis.metric_field,
                "filter_count": len(cell_analysis.filters)
            },
            "filters": {},
            "entities": {
                "description": "Define the entities that should be created to satisfy the cell filters",
                "data": [
                    {
                        "entity_type": "financial_asset_instrument",
                        "entity_id": "123321_2018-09-30_BLZ10",
                        "attributes": {
                            "CRRYNG_AMNT": 83491250,
                            "ACCNTNG_CLSSFCTN": "6",
                            "ACCRD_INTRST": 191200,
                            "GRSS_CRRYNG_AMNT_E_INTRST": 83300000,
                            "IMPRMNT_STTS": "23"
                        },
                        "relationships": {}
                    },
                    {
                        "entity_type": "party",
                        "entity_id": "78451209_2018-09-30_BLZ10",
                        "attributes": {
                            "INSTTTNL_SCTR": "S11",
                            "ECNMC_ACTVTY": "23_32",
                            "PRTY_RL_TYP": "28",
                            "LGL_PRSN_TYP": "13"
                        },
                        "relationships": {}
                    }
                ]
            },
            "custom_sql": [],
            "generation_notes": [
                "Modify 'entities.data' to customize the generated fixtures",
                "Each entity_type corresponds to database table groups",
                "Attributes should match database column names",
                "Use 'custom_sql' for additional SQL statements",
                f"Cell expects metric_field '{cell_analysis.metric_field}' to sum to expected_value"
            ]
        }

        # Add detailed filter analysis
        for filter_obj in cell_analysis.filters:
            schema["filters"][filter_obj.field_name] = {
                "description": f"Cell filter: {filter_obj.field_name} must be one of the allowed values",
                "operator": filter_obj.operator,
                "allowed_values": filter_obj.values,
                "selected_values": filter_obj.values[:1] if filter_obj.values else [],
                "note": "Entities must have this field set to one of the selected_values"
            }

        return schema

    def interactive_configuration(self, template_id: str, cell_suffix: str) -> Optional[FixtureConfig]:
        """Interactive CLI for user configuration"""
        print(f"\n🔧 Configuring fixture for {template_id}_{cell_suffix}")
        print("=" * 60)

        # Analyze the cell
        cell_analysis = self.analyze_cell(template_id, cell_suffix)
        if not cell_analysis:
            print("❌ Failed to analyze cell")
            return None

        print(f"📊 Cell Analysis:")
        print(f"   Referenced Table: {cell_analysis.referenced_table}")
        print(f"   Metric Field: {cell_analysis.metric_field}")
        print(f"   Filters Found: {len(cell_analysis.filters)}")

        for i, filter_obj in enumerate(cell_analysis.filters, 1):
            print(f"      {i}. {filter_obj.field_name}: {filter_obj.values}")

        # Get scenario name
        scenario_name = input(f"\n📝 Scenario name [loan_and_guarantee_scenario_1]: ").strip()
        if not scenario_name:
            scenario_name = "loan_and_guarantee_scenario_1"

        # Get expected value
        expected_value_input = input(f"💰 Expected value [83491250]: ").strip()
        try:
            expected_value = int(expected_value_input) if expected_value_input else 83491250
        except ValueError:
            expected_value = 83491250

        # Generate and save JSON schema
        schema = self.generate_json_schema(cell_analysis)
        schema["scenario_name"] = scenario_name
        schema["expected_value"] = expected_value

        # Optionally prefill from database
        prefill = input(f"\n🗄️  Prefill configuration from database? [y/N]: ").strip().lower()
        if prefill in ['y', 'yes']:
            db_data = self.prefill_from_database(template_id, cell_suffix)
            if db_data:
                print(f"📊 Found sample data from database:")
                if 'instruments' in db_data:
                    print(f"   Instruments: {len(db_data['instruments'])} samples")
                if 'parties' in db_data:
                    print(f"   Parties: {len(db_data['parties'])} samples")
                schema['database_samples'] = db_data

        # Ask if user wants to customize
        customize = input(f"\n⚙️  Generate JSON configuration file for customization? [y/N]: ").strip().lower()

        custom_data = {}
        if customize in ['y', 'yes']:
            config_file = f"fixture_config_{template_id}_{cell_suffix}_{scenario_name}.json"
            with open(config_file, 'w') as f:
                json.dump(schema, f, indent=2)
            print(f"📄 Configuration saved to: {config_file}")
            print(f"✏️  Edit the file and run with --config-file {config_file}")
            return None

        return FixtureConfig(
            template_id=template_id,
            cell_suffix=cell_suffix,
            scenario_name=scenario_name,
            expected_value=expected_value,
            custom_data=custom_data
        )

    def generate_fixtures(self, config: FixtureConfig) -> bool:
        """Generate SQL fixtures based on configuration"""
        try:
            # Create fixtures directory
            fixtures_dir = Path(f"tests/fixtures/templates/{config.template_id}/{config.cell_suffix}/{config.scenario_name}")
            fixtures_dir.mkdir(parents=True, exist_ok=True)

            # Create __init__.py files for package discovery
            self._create_init_files(fixtures_dir)

            # Generate SQL inserts file
            inserts_file = fixtures_dir / "sql_inserts.sql"
            self._generate_sql_inserts(config, inserts_file)

            # Generate delete file using existing generator
            deletes_file = fixtures_dir / "sql_deletes.sql"
            self._generate_sql_deletes(inserts_file, deletes_file)

            # Update test configuration file
            self._update_test_configuration(config)

            # Generate corresponding test file
            self._generate_test_file(config)

            self.logger.info(f"Fixtures generated in: {fixtures_dir}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to generate fixtures: {e}")
            return False

    def _generate_sql_inserts(self, config: FixtureConfig, output_file: Path):
        """Generate SQL INSERT statements using advanced builder"""
        try:
            # Check if there's custom data with full template
            if config.custom_data and 'entities' in config.custom_data:
                # Use custom configuration
                template = FixtureTemplate(
                    template_id=config.template_id,
                    cell_suffix=config.cell_suffix,
                    scenario_name=config.scenario_name,
                    expected_value=config.expected_value,
                    entities=[EntityData(**entity) for entity in config.custom_data['entities']],
                    custom_sql=config.custom_data.get('custom_sql', [])
                )
            else:
                # Generate default template
                template = self.sql_builder.generate_fixture_template_for_cell(
                    config.template_id,
                    config.cell_suffix,
                    config.expected_value
                )
                template.scenario_name = config.scenario_name

            # Build SQL statements
            sql_statements = self.sql_builder.build_sql_from_template(template)

            # Write to file
            with open(output_file, 'w') as f:
                f.write('\n'.join(sql_statements))

            self.logger.info(f"Generated {len(sql_statements)} SQL statements")

        except Exception as e:
            self.logger.error(f"Failed to generate SQL inserts: {e}")
            # Fallback to basic template
            self._generate_basic_sql_inserts(config, output_file)

    def _generate_basic_sql_inserts(self, config: FixtureConfig, output_file: Path):
        """Fallback method for basic SQL generation"""
        sql_statements = [
            "-- Generated fixture data (basic template)",
            f"-- Template: {config.template_id}",
            f"-- Cell: {config.cell_suffix}",
            f"-- Scenario: {config.scenario_name}",
            f"-- Expected Value: {config.expected_value}",
            "",
            # Basic instrument entry
            f"INSERT INTO pybirdai_balance_sheet_recognised_financial_asset_instrument_type(rowid,test_id,Balance_sheet_recognised_financial_asset_instrument_type_uniqueID) VALUES(1,'1','123321_2018-09-30_BLZ10');",
            f"INSERT INTO pybirdai_blnc_sht_rcgnsd_fnncl_asst_instrmnt(rowid,financial_asset_instrument_type_ptr_id,CRRYNG_AMNT) VALUES(1,'123321_2018-09-30_BLZ10',{config.expected_value});",
        ]

        with open(output_file, 'w') as f:
            f.write('\n'.join(sql_statements))

    def _generate_sql_deletes(self, inserts_file: Path, deletes_file: Path):
        """Generate delete statements using existing generator"""
        try:
            process_sql_file(str(inserts_file))
            self.logger.info(f"Delete fixtures generated: {deletes_file}")
        except Exception as e:
            self.logger.error(f"Failed to generate delete fixtures: {e}")

    def _create_init_files(self, fixtures_dir: Path):
        """Create __init__.py files for package discovery"""
        # Create __init__.py in the scenario directory
        init_file = fixtures_dir / "__init__.py"
        if not init_file.exists():
            init_file.write_text("# Auto-generated by fixture generator\n")

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
                    init_file.write_text("# Auto-generated by fixture generator\n")

    def _update_test_configuration(self, config: FixtureConfig):
        """Update tests/configuration_file_tests.json with new test configuration using precise editing"""
        config_file_path = Path("tests/configuration_file_tests.json")

        try:
            # Read existing configuration file
            if not config_file_path.exists():
                # Create new file if it doesn't exist
                initial_content = '{\n  "tests": []\n}'
                with open(config_file_path, 'w') as f:
                    f.write(initial_content)

            with open(config_file_path, 'r') as f:
                content = f.read()
                config_data = json.loads(content)

            # Create new test entry
            new_test = {
                "reg_tid": config.template_id,
                "dp_suffix": config.cell_suffix,
                "dp_value": config.expected_value,
                "scenario": config.scenario_name
            }

            # Check if test already exists
            existing_test_index = None
            for i, test in enumerate(config_data["tests"]):
                if (test["reg_tid"] == config.template_id and
                    test["dp_suffix"] == config.cell_suffix and
                    test["scenario"] == config.scenario_name):
                    existing_test_index = i
                    break

            if existing_test_index is not None:
                # Update existing test using precise string replacement
                old_test = config_data["tests"][existing_test_index]
                old_test_str = self._format_test_entry(old_test)
                new_test_str = self._format_test_entry(new_test)

                # Replace the specific test entry in the file content
                new_content = content.replace(old_test_str, new_test_str)
                with open(config_file_path, 'w') as f:
                    f.write(new_content)

                self.logger.info(f"Updated existing test configuration for {config.template_id}_{config.cell_suffix}_{config.scenario_name}")
            else:
                # Add new test entry
                if config_data["tests"]:
                    # Insert before the closing bracket of the tests array
                    new_test_str = "," + self._format_test_entry(new_test, is_last=False)
                    insertion_point = content.rfind("    }\n  ]")
                    if insertion_point != -1:
                        new_content = content[:insertion_point + 5] + new_test_str + content[insertion_point + 5:]
                        with open(config_file_path, 'w') as f:
                            f.write(new_content)
                    else:
                        # Fallback if pattern not found
                        self._update_test_configuration_fallback(config)
                        return
                else:
                    # First test entry - replace empty array
                    new_test_str = self._format_test_entry(new_test, is_first=True)
                    new_content = content.replace('  "tests": []', f'  "tests": [\n{new_test_str}\n  ]')
                    with open(config_file_path, 'w') as f:
                        f.write(new_content)

                self.logger.info(f"Added new test configuration for {config.template_id}_{config.cell_suffix}_{config.scenario_name}")

        except Exception as e:
            self.logger.error(f"Failed to update test configuration: {e}")
            # Fallback to original method if editing fails
            self._update_test_configuration_fallback(config)

    def _format_test_entry(self, test_data: dict, is_first: bool = False, is_last: bool = True) -> str:
        """Format a test entry for consistent JSON formatting"""
        if is_first:
            prefix = "    "
        else:
            prefix = "\n    "

        formatted = f"""{prefix}{{
      "reg_tid": "{test_data['reg_tid']}",
      "dp_suffix": "{test_data['dp_suffix']}",
      "dp_value": {test_data['dp_value']},
      "scenario": "{test_data['scenario']}"
    }}"""

        return formatted

    def _update_test_configuration_fallback(self, config: FixtureConfig):
        """Fallback method that writes entire file (original implementation)"""
        config_file_path = Path("tests/configuration_file_tests.json")

        try:
            # Load existing configuration
            if config_file_path.exists():
                with open(config_file_path, 'r') as f:
                    config_data = json.load(f)
            else:
                config_data = {"tests": []}

            # Create new test entry
            new_test = {
                "reg_tid": config.template_id,
                "dp_suffix": config.cell_suffix,
                "dp_value": config.expected_value,
                "scenario": config.scenario_name
            }

            # Check if test already exists
            existing_test = None
            for i, test in enumerate(config_data["tests"]):
                if (test["reg_tid"] == config.template_id and
                    test["dp_suffix"] == config.cell_suffix and
                    test["scenario"] == config.scenario_name):
                    existing_test = i
                    break

            if existing_test is not None:
                config_data["tests"][existing_test] = new_test
            else:
                config_data["tests"].append(new_test)

            # Save updated configuration
            with open(config_file_path, 'w') as f:
                json.dump(config_data, f, indent=2)

        except Exception as e:
            self.logger.error(f"Fallback update also failed: {e}")

    def _generate_test_file(self, config: FixtureConfig):
        """Generate corresponding test file using existing generator"""
        try:
            from .generator_for_tests import TestCodeGenerator

            # Use existing test generator
            generator = TestCodeGenerator()

            # Generate the test file
            datapoint_id = f"{config.template_id}_{config.cell_suffix}"
            cell_class = f"Cell_{config.template_id}_{config.cell_suffix}"

            import_code = generator.create_import_statements(cell_class)
            test_code = generator.create_test_functions(config.expected_value, datapoint_id)

            # Save test file
            output_file = f'tests/test_{cell_class.lower()}__{config.scenario_name}.py'
            with open(output_file, 'w') as f:
                f.write(import_code)
                f.write('\n\n')
                f.write(test_code)

            self.logger.info(f"Test file generated: {output_file}")

        except Exception as e:
            self.logger.error(f"Failed to generate test file: {e}")

    def prefill_from_database(self, template_id: str, cell_suffix: str) -> Optional[Dict[str, Any]]:
        """Analyze database to prefill configuration with realistic data"""
        try:
            if not os.path.exists(self.db_path):
                self.logger.warning(f"Database not found: {self.db_path}")
                return None

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Get sample data from key tables
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
            self.logger.error(f"Failed to analyze database: {e}")
            return None

    def run_cli(self):
        """Main CLI interface"""
        parser = argparse.ArgumentParser(description='Generate test fixtures for regulatory templates')
        parser.add_argument('--list-templates', action='store_true', help='List available templates')
        parser.add_argument('--template', type=str, help='Template ID to work with')
        parser.add_argument('--list-cells', action='store_true', help='List cells for specified template')
        parser.add_argument('--cell', type=str, help='Cell suffix to analyze/generate')
        parser.add_argument('--analyze', action='store_true', help='Analyze specified cell')
        parser.add_argument('--generate', action='store_true', help='Generate fixtures')
        parser.add_argument('--config-file', type=str, help='Use JSON configuration file')
        parser.add_argument('--prefill', action='store_true', help='Prefill configuration from database')
        parser.add_argument('--db-path', type=str, help='Path to SQLite database')

        args = parser.parse_args()

        if args.db_path:
            self.db_path = args.db_path

        # Setup Django environment
        self.setup_environment()

        # List templates
        if args.list_templates:
            templates = self.discover_templates()
            print("\n📋 Available Templates:")
            print("=" * 40)
            for template_id, cells in templates.items():
                print(f"🏷️  {template_id} ({len(cells)} cells)")
            print()
            return

        # Require template for other operations
        if not args.template:
            print("❌ Please specify --template or use --list-templates")
            return

        # List cells for template
        if args.list_cells:
            cells = self.discover_cells_for_template(args.template)
            print(f"\n📋 Cells for {args.template}:")
            print("=" * 40)
            for cell in cells:
                print(f"🔬 {cell}")
            print()
            return

        # Require cell for other operations
        if not args.cell:
            print("❌ Please specify --cell or use --list-cells")
            return

        # Analyze cell
        if args.analyze:
            analysis = self.analyze_cell(args.template, args.cell)
            if analysis:
                print(f"\n🔍 Analysis for {analysis.cell_name}:")
                print("=" * 50)
                print(f"Referenced Table: {analysis.referenced_table}")
                print(f"Metric Field: {analysis.metric_field}")
                print(f"Filters ({len(analysis.filters)}):")
                for i, filter_obj in enumerate(analysis.filters, 1):
                    print(f"  {i}. {filter_obj.field_name}: {filter_obj.values}")
                print()
            return

        # Generate fixtures
        if args.generate:
            if args.config_file:
                # Load from JSON config
                try:
                    with open(args.config_file, 'r') as f:
                        config_data = json.load(f)

                    # Extract required fields for FixtureConfig
                    required_fields = {
                        'template_id': config_data.get('template_id', args.template),
                        'cell_suffix': config_data.get('cell_suffix', args.cell),
                        'scenario_name': config_data.get('scenario_name', 'loan_and_guarantee_scenario_1'),
                        'expected_value': config_data.get('expected_value', 83491250),
                        'custom_data': config_data  # Store entire JSON for custom processing
                    }

                    config = FixtureConfig(**required_fields)
                except Exception as e:
                    print(f"❌ Failed to load config file: {e}")
                    return
            else:
                # Interactive configuration
                config = self.interactive_configuration(args.template, args.cell)
                if not config:
                    return

            # Generate fixtures
            success = self.generate_fixtures(config)
            if success:
                print("✅ Fixtures generated successfully!")
            else:
                print("❌ Failed to generate fixtures")
            return

        # Show help if no action specified
        parser.print_help()

def main():
    """Main entry point"""
    generator = FixtureGenerator()
    generator.run_cli()

if __name__ == "__main__":
    main()