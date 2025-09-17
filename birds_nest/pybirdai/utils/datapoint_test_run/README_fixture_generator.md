# Fixture Generator for FINREP Regulatory Templates

This tool automatically generates SQL fixtures and test files for regulatory reporting templates based on cell analysis and user configuration.

## Overview

The fixture generator analyzes FINREP cell classes to understand their filter requirements and generates realistic test data that satisfies those constraints. It creates:

- SQL INSERT statements for test data
- SQL DELETE statements for cleanup
- Corresponding test files using the existing test framework
- JSON configuration files for customization

## Usage

### Basic Commands

```bash
# List all available templates
python -m pybirdai.utils.datapoint_test_run.fixture_generator --list-templates

# List cells for a specific template
python -m pybirdai.utils.datapoint_test_run.fixture_generator --template F_05_01_REF_FINREP_3_0 --list-cells

# Analyze a specific cell
python -m pybirdai.utils.datapoint_test_run.fixture_generator --template F_05_01_REF_FINREP_3_0 --cell 152457_REF --analyze

# Generate fixtures with interactive configuration
python -m pybirdai.utils.datapoint_test_run.fixture_generator --template F_05_01_REF_FINREP_3_0 --cell 152457_REF --generate

# Generate fixtures from JSON configuration
python -m pybirdai.utils.datapoint_test_run.fixture_generator --template F_05_01_REF_FINREP_3_0 --cell 152457_REF --generate --config-file my_config.json
```

### Advanced Options

```bash
# Use custom database path
python -m pybirdai.utils.datapoint_test_run.fixture_generator --db-path /path/to/db.sqlite3 --generate --template F_05_01_REF_FINREP_3_0 --cell 152457_REF

# Prefill configuration from existing database data
python -m pybirdai.utils.datapoint_test_run.fixture_generator --template F_05_01_REF_FINREP_3_0 --cell 152457_REF --prefill --generate
```

## Interactive Workflow

1. **Template Selection**: The tool discovers all available regulatory templates from the codebase
2. **Cell Analysis**: It analyzes the selected cell's filter requirements and metric calculations
3. **Configuration**: User configures scenario name, expected value, and data preferences
4. **Generation**: Creates SQL fixtures, cleanup scripts, and test files

### Example Interactive Session

```
🔧 Configuring fixture for F_05_01_REF_FINREP_3_0_152457_REF
============================================================

📊 Cell Analysis:
   Referenced Table: F_05_01_REF_FINREP_3_0_Table
   Metric Field: CRRYNG_AMNT
   Filters Found: 5
      1. PRPS: ['12', '7', '9', '6', '8', '4', '13', '1', '19']
      2. ACCNTNG_CLSSFCTN: ['6', '14', '45', '9', '7', '8']
      3. HLD_SL_INDCTR: ['2']
      4. INSTTTNL_SCTR: ['S121', 'S11', 'S123', ...]
      5. PRTY_RL_TYP: ['28']

📝 Scenario name [loan_and_guarantee_scenario_1]: my_custom_scenario

💰 Expected value [83491250]: 100000000

🗄️  Prefill configuration from database? [y/N]: y
📊 Found sample data from database:
   Instruments: 3 samples
   Parties: 5 samples

⚙️  Generate JSON configuration file for customization? [y/N]: y
📄 Configuration saved to: fixture_config_F_05_01_REF_FINREP_3_0_152457_REF_my_custom_scenario.json
✏️  Edit the file and run with --config-file fixture_config_F_05_01_REF_FINREP_3_0_152457_REF_my_custom_scenario.json
```

## JSON Configuration Format

The tool generates comprehensive JSON configuration files:

```json
{
  "template_id": "F_05_01_REF_FINREP_3_0",
  "cell_suffix": "152457_REF",
  "scenario_name": "loan_and_guarantee_scenario_1",
  "expected_value": 83491250,
  "description": "Configuration for Cell_F_05_01_REF_FINREP_3_0_152457_REF",
  "cell_analysis": {
    "referenced_table": "F_05_01_REF_FINREP_3_0_Table",
    "metric_field": "CRRYNG_AMNT",
    "filter_count": 5
  },
  "filters": {
    "PRPS": {
      "description": "Cell filter: PRPS must be one of the allowed values",
      "operator": "in",
      "allowed_values": ["12", "7", "9", "6", "8", "4", "13", "1", "19"],
      "selected_values": ["12"],
      "note": "Entities must have this field set to one of the selected_values"
    }
  },
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
      }
    ]
  },
  "custom_sql": [],
  "generation_notes": [
    "Modify 'entities.data' to customize the generated fixtures",
    "Each entity_type corresponds to database table groups",
    "Attributes should match database column names",
    "Use 'custom_sql' for additional SQL statements",
    "Cell expects metric_field 'CRRYNG_AMNT' to sum to expected_value"
  ]
}
```

## Generated Files

For each fixture generation, the tool creates:

### 1. SQL Fixtures Directory
```
tests/fixtures/templates/F_05_01_REF_FINREP_3_0/152457_REF/loan_and_guarantee_scenario_1/
├── sql_inserts.sql    # INSERT statements for test data
└── sql_deletes.sql    # DELETE statements for cleanup
```

### 2. Test File
```
tests/test_cell_f_05_01_ref_finrep_3_0_152457_ref__loan_and_guarantee_scenario_1.py
```

## Entity Types

The SQL builder supports these entity types:

- **financial_asset_instrument**: Main financial instruments with carrying amounts
- **loan_excluding_repurchase_agreement**: Loan-specific attributes
- **financial_asset_instrument_data**: Derived data and lineage information
- **party**: Legal entities (debtors, creditors, protection providers)
- **instrument_entity_role_assignment**: Relationships between instruments and parties

## Database Integration

The tool can:

- Analyze existing database schema from SQLite files
- Extract sample data for realistic fixture generation
- Generate fixtures that respect foreign key relationships
- Create cleanup scripts compatible with existing database structure

## Customization

### Entity Attributes

Customize entity attributes to match your test requirements:

```json
{
  "entity_type": "financial_asset_instrument",
  "entity_id": "MY_CUSTOM_ID",
  "attributes": {
    "CRRYNG_AMNT": 50000000,
    "ACCNTNG_CLSSFCTN": "6",
    "IMPRMNT_STTS": "23"
  }
}
```

### Custom SQL

Add custom SQL statements for complex scenarios:

```json
{
  "custom_sql": [
    "UPDATE pybirdai_prty SET ECNMC_ACTVTY='64_1' WHERE rowid=1;",
    "INSERT INTO pybirdai_custom_table(id, value) VALUES(1, 'test');"
  ]
}
```

## Integration with Existing Tools

The fixture generator integrates with:

- **`generator_for_tests.py`**: Generates corresponding test files
- **`generator_delete_fixtures.py`**: Creates cleanup SQL from insert statements
- **Django Models**: Respects database schema and relationships
- **Cell Analysis**: Understands filter requirements from report_cells.py

## Troubleshooting

### Common Issues

1. **Django not available**: Ensure you're running in the correct virtual environment with Django installed
2. **Cell not found**: Check that the cell class exists in `report_cells.py`
3. **Database connection**: Verify the SQLite database path is correct
4. **Permission errors**: Ensure write permissions for the tests/ directory

### Debug Mode

Enable detailed logging:

```bash
export PYTHONPATH=/path/to/birds_nest
python -c "import logging; logging.basicConfig(level=logging.DEBUG)" -m pybirdai.utils.datapoint_test_run.fixture_generator --template F_05_01_REF_FINREP_3_0 --cell 152457_REF --generate
```

## Examples

### Generate Fixtures for Loan Scenario

```bash
# Interactive generation
python -m pybirdai.utils.datapoint_test_run.fixture_generator \
    --template F_05_01_REF_FINREP_3_0 \
    --cell 152457_REF \
    --generate

# With custom database and scenario
python -m pybirdai.utils.datapoint_test_run.fixture_generator \
    --template F_05_01_REF_FINREP_3_0 \
    --cell 152589_REF \
    --generate \
    --db-path /path/to/my_test_db.sqlite3
```

### Analyze Multiple Cells

```bash
for cell in 152457_REF 152589_REF; do
    python -m pybirdai.utils.datapoint_test_run.fixture_generator \
        --template F_05_01_REF_FINREP_3_0 \
        --cell $cell \
        --analyze
done
```

This comprehensive fixture generator streamlines the creation of test data for complex regulatory reporting scenarios while maintaining consistency with existing database structures and test frameworks.