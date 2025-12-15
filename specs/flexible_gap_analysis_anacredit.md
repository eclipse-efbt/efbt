# ANACREDIT Joins Meta Data Harmonization Specification

## Overview

This specification describes changes to harmonize the ANACREDIT joins metadata creation process with the FINREP approach. The goal is to use consistent configuration file formats across both frameworks while supporting per-output-table configuration for ANACREDIT.

## Current State

### Classes
- **FINREP**: `JoinsMetaDataCreator` in `create_joins_meta_data.py`
- **ANACREDIT**: `JoinsMetaDataCreatorANCRDT` in `create_joins_meta_data_ancrdt.py`

### Current ANACREDIT Configuration Files

**File 1**: `join_for_product_to_reference_category_ANCRDT_REF.csv`
```csv
rolc,join_identifier
ANCRDT_INSTRMNT_C_1,Loans and advances
ANCRDT_INSTRMNT_C_1,Non Negotiable bonds
```
- Maps output layer cubes (rolc) to join identifiers (product names)
- Single file for all 10 ANACREDIT output tables

**File 2**: `join_for_product_il_definitions_ANCRDT_REF.csv`
```csv
Name,Main Table,Filter,Related Tables,Comments
Non Negotiable bonds,LNG_SCRTY_PSTN_PRDNTL_PRTFL_ASSGNMNT_ACCNTNG_CLSSFCTN_FNNCL_ASSTS_ASSGNMNT,LNG_NN_NGTBL_SCRTY_PSTN,PRTY:,
Loans and advances,INSTRMNT,OTHR_LN,INSTRMNT_RL:PRTY:CLLTRL,
```
- Defines input layer tables and relationships for each product
- Single file for all ANACREDIT processing

### Current FINREP Configuration Files

**File 1**: `join_for_product_to_reference_category_FINREP_REF.csv`
```csv
Main Category,Name,slice_name
INSTRMNT_TYP_PRDCT=TYP_INSTRMNT_1003,Reverse repurchase agreements,Reverse repurchase agreements
TYP_ACCNTNG_ITM=TYP_ACCNTNG_ITM_10,Cash on hands,Cash on hand
```

**File 2**: `join_for_product_il_definitions_FINREP_REF.csv`
```csv
Name,Main Table,Filter,Related Tables,Comments
Reverse repurchase agreements,INSTRMNT,RVRS_RPCHS,INSTRMNT_RL:PRTY:CLLTRL,
```

## Proposed Changes

### Goal
1. Use FINREP-style headers for ANACREDIT configuration files
2. Support per-output-table configuration (10 separate file pairs for ANACREDIT)
3. Enable future use of breakdown conditions (e.g., `TYP_INSTRMNT=TYP_INSTRMNT_1003`)

### ANACREDIT Output Tables (10 total)
| Output Table Code | Description |
|-------------------|-------------|
| ANCRDT_INSTRMNT_C_1 | Instrument |
| ANCRDT_FNNCL_C_1 | Financial |
| ANCRDT_ACCNTNG_C_1 | Accounting |
| ANCRDT_CNTRPRTY_RFRNC_C_1 | Counterparty Reference |
| ANCRDT_CNTRPRTY_DFLT_C_1 | Counterparty Default |
| ANCRDT_CNTRPRTY_RSK_C_1 | Counterparty Risk |
| ANCRDT_PRTCTN_RCVD_C_1 | Protection Received |
| ANCRDT_INSTRMNT_PRTCTN_RCVD_C_1 | Instrument-Protection Received |
| ANCRDT_JNT_LBLTS_C_1 | Joint Liabilities |
| ANCRDT_CNTRPRTY_INSTRMNT_C_1 | Counterparty-Instrument |

### New File Structure

#### Per-Output-Table Configuration

For each output table, create two files:

**File 1**: `join_for_product_to_reference_category_ANCRDT_REF_{OUTPUT_TABLE}.csv`

New format (harmonized with FINREP):
```csv
Main Category,Name,slice_name
,Loans and advances,Loans and advances
,Non Negotiable bonds,Non Negotiable bonds
,Credit card debt,Credit card debt
```

Notes:
- Empty `Main Category` = no breakdown (process all data together)
- In future, can add conditions like `TYP_INSTRMNT=TYP_INSTRMNT_114` for specific filtering
- `Name` = join identifier / product name
- `slice_name` = display name for the slice

**File 2**: `join_for_product_il_definitions_ANCRDT_REF_{OUTPUT_TABLE}.csv`

Unchanged format (already compatible):
```csv
Name,Main Table,Filter,Related Tables,Comments
Loans and advances,INSTRMNT,OTHR_LN,INSTRMNT_RL:PRTY:INSTRMNT_ENTTY_RL_ASSGNMNT:CLLTRL,
Credit card debt,INSTRMNT,CRDT_CRD_DBT,INSTRMNT_RL:PRTY:INSTRMNT_ENTTY_RL_ASSGNMNT:CLLTRL,
```

### Example: ANCRDT_INSTRMNT_C_1

**`join_for_product_to_reference_category_ANCRDT_REF_ANCRDT_INSTRMNT_C_1.csv`**:
```csv
Main Category,Name,slice_name
,Advances that are not loans,Advances that are not loans
,Other loans,Other loans
,Credit card debt,Credit card debt
,Trade receivables,Trade receivables
,Finance leases,Finance leases
,Deposits,Deposits
,On demand and short notice,On demand and short notice
,Reverse repurchase agreements,Reverse repurchase agreements
```

**`join_for_product_il_definitions_ANCRDT_REF_ANCRDT_INSTRMNT_C_1.csv`**:
```csv
Name,Main Table,Filter,Related Tables,Comments
Advances that are not loans,INSTRMNT,ADVNC,INSTRMNT_RL:PRTY:INSTRMNT_ENTTY_RL_ASSGNMNT:CLLTRL,
Other loans,INSTRMNT,OTHR_LN,INSTRMNT_RL:PRTY:INSTRMNT_ENTTY_RL_ASSGNMNT:CLLTRL,
Credit card debt,INSTRMNT,CRDT_CRD_DBT,INSTRMNT_RL:PRTY:INSTRMNT_ENTTY_RL_ASSGNMNT:CLLTRL,
Trade receivables,INSTRMNT,TRD_RCVBL,INSTRMNT_RL:PRTY:INSTRMNT_ENTTY_RL_ASSGNMNT:CLLTRL,
Finance leases,INSTRMNT,FNNCL_LS,INSTRMNT_RL:PRTY:INSTRMNT_ENTTY_RL_ASSGNMNT:CLLTRL,
Deposits,INSTRMNT,DPST,INSTRMNT_RL:PRTY:INSTRMNT_ENTTY_RL_ASSGNMNT,
On demand and short notice,INSTRMNT,ON_DMND,INSTRMNT_RL:PRTY:INSTRMNT_ENTTY_RL_ASSGNMNT,
Reverse repurchase agreements,INSTRMNT,RVRS_RPCHS_LNS,INSTRMNT_RL:PRTY:INSTRMNT_ENTTY_RL_ASSGNMNT,
```

### Example: ANCRDT_CNTRPRTY_RFRNC_C_1 (No Breakdown)

Some ANACREDIT output tables like Counterparty Reference may not need any product breakdown:

**`join_for_product_to_reference_category_ANCRDT_REF_ANCRDT_CNTRPRTY_RFRNC_C_1.csv`**:
```csv
Main Category,Name,slice_name
,All Counterparties,All Counterparties
```

**`join_for_product_il_definitions_ANCRDT_REF_ANCRDT_CNTRPRTY_RFRNC_C_1.csv`**:
```csv
Name,Main Table,Filter,Related Tables,Comments
All Counterparties,PRTY,,PRTY_RL,No filter - all counterparties
```

## Technical Implementation

### 1. Update JoinsMetaDataCreatorANCRDT

Modify `__init__` to use `JoinsConfigurationResolver`:

```python
from pybirdai.process_steps.joins_meta_data.config_resolver import JoinsConfigurationResolver

class JoinsMetaDataCreatorANCRDT:
    def __init__(self, output_table: str = None):
        DjangoSetup.configure_django()
        self.join_map = {}

        config_dir = os.path.join(os.getcwd(), "resources/joins_configuration")
        resolver = JoinsConfigurationResolver(config_dir)

        # If output_table specified, use per-table config; otherwise use framework-wide
        if output_table:
            self._load_per_table_config(resolver, output_table)
        else:
            self._load_all_tables_config(resolver)
```

### 2. Add Per-Table Config Loading

```python
def _load_per_table_config(self, resolver, output_table: str):
    """Load configuration for a specific output table."""
    # File 1: Product to reference category mapping
    file1 = f"join_for_product_to_reference_category_ANCRDT_REF_{output_table}.csv"
    # File 2: IL definitions
    file2 = f"join_for_product_il_definitions_ANCRDT_REF_{output_table}.csv"

    self._parse_config_files(
        os.path.join(resolver.config_directory, file1),
        os.path.join(resolver.config_directory, file2),
        output_table
    )

def _load_all_tables_config(self, resolver):
    """Load configuration for all output tables."""
    output_tables = [
        'ANCRDT_INSTRMNT_C_1', 'ANCRDT_FNNCL_C_1', 'ANCRDT_ACCNTNG_C_1',
        'ANCRDT_CNTRPRTY_RFRNC_C_1', 'ANCRDT_CNTRPRTY_DFLT_C_1',
        'ANCRDT_CNTRPRTY_RSK_C_1', 'ANCRDT_PRTCTN_RCVD_C_1',
        'ANCRDT_INSTRMNT_PRTCTN_RCVD_C_1', 'ANCRDT_JNT_LBLTS_C_1',
        'ANCRDT_CNTRPRTY_INSTRMNT_C_1'
    ]

    for output_table in output_tables:
        if resolver.has_per_cube_config('ANCRDT_REF', output_table):
            self._load_per_table_config(resolver, output_table)
        else:
            logger.warning(f"No per-table config for {output_table}, skipping")
```

### 3. Update Config Parsing

```python
def _parse_config_files(self, file1_path: str, file2_path: str, output_table: str):
    """Parse harmonized config files (FINREP-style headers)."""
    # Read IL definitions (file2)
    il_definitions = {}
    with open(file2_path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            il_definitions[row['Name']] = row

    # Read product to reference category mapping (file1)
    with open(file1_path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            join_identifier = row['Name']  # Was 'join_identifier'
            main_category = row.get('Main Category', '').strip()

            if join_identifier in il_definitions:
                il_def = il_definitions[join_identifier]
                key = (output_table, join_identifier)
                self.join_map[key] = {
                    "rolc": output_table,
                    "join_identifier": join_identifier,
                    "main_category": main_category,  # For future use
                    "ilc": [t for t in [il_def["Main Table"]] + il_def["Related Tables"].split(":") if t]
                }
```

## Migration Strategy

### Phase 1: Create Per-Table Config Files
1. Create 10 pairs of config files (one pair per output table)
2. Convert from old format to new FINREP-style headers
3. Leave `Main Category` empty initially (no breakdown)

### Phase 2: Update JoinsMetaDataCreatorANCRDT
1. Add support for per-table config file discovery
2. Update CSV parsing for new header format
3. Store `main_category` for future filter generation

### Phase 3: Deprecate Old Files
1. Remove old `join_for_product_to_reference_category_ANCRDT_REF.csv`
2. Remove old `join_for_product_il_definitions_ANCRDT_REF.csv`

### Phase 4: Future Enhancement (Optional)
1. Add breakdown conditions to `Main Category` column
2. Generate filters using `BreakdownCondition` class
3. Harmonize filter generation between FINREP and ANACREDIT

## Key Differences from FINREP

| Aspect | FINREP | ANACREDIT |
|--------|--------|-----------|
| Config files | 1 pair for all reports | 10 pairs (1 per output table) |
| Main Category | Required (breakdown condition) | Optional (empty = no breakdown) |
| Combinations | Used for cell-level filtering | Not used |
| Output tables | ~140 reports | 10 tables |

## Benefits

1. **Consistency**: Same file format across frameworks
2. **Flexibility**: Per-table configuration allows different products per output table
3. **Future-proof**: Can add breakdown conditions later without format changes
4. **Maintainability**: Easier to understand with consistent patterns
5. **Extensibility**: New output tables just need new config file pairs
