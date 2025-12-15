# Flexible Gap Analysis Specification

## Overview

This specification describes enhancements to the PyBIRD AI gap analysis and joins metadata creation to support flexible product/slice breakdowns across different regulatory frameworks. The goal is to move beyond the FINREP-centric approach based on `TYP_INSTRMNT` or `TYP_ACCNTNG_ITM` to support framework-specific breakdown strategies.

## Current State Analysis

### Current Functionality

The gap analysis process, which starts in `create_joins_meta_data.py`, creates:
- `CUBE_LINK` entries connecting input layer cubes to output layer cubes
- `CUBE_STRUCTURE_ITEM_LINK` entries mapping variables between cubes
- Python stub code for technical joins, organized by product/slice per report

### Current Limitation: FINREP-Centric Design

The current implementation has historically been focused on FINREP and always creates products/slices based upon:
- `TYP_INSTRMNT` (Instrument Type), OR
- `TYP_ACCNTNG_ITM` (Accounting Item Type)

This approach does not suit other reporting frameworks:

| Framework | Current Approach | Limitation |
|-----------|-----------------|------------|
| FINREP | TYP_INSTRMNT or TYP_ACCNTNG_ITM | Works well |
| ANACREDIT | TYP_INSTRMNT or TYP_ACCNTNG_ITM | May need different breakdown per output table |
| ANACREDIT COUNTERPARTY | TYP_INSTRMNT or TYP_ACCNTNG_ITM | May not need any breakdown |
| Other Frameworks | TYP_INSTRMNT or TYP_ACCNTNG_ITM | May need multi-variable conditions |

## Proposed Enhancement

### Goal

Create a flexible product/slice breakdown system that:
1. Allows different breakdown strategies per framework
2. Supports single-variable, multi-variable, or no-breakdown configurations
3. Enables per-output-layer-cube configuration (especially for ANACREDIT)
4. Maintains backward compatibility with existing FINREP configuration

## Functional Requirements

### 1. Enhanced Joins Configuration Format

#### 1.1 Current FINREP Format (Backward Compatible)
```csv
Main Category,Name,slice_name
TYP_INSTRMNT_970,Other commitments received,Other commitments received
TYP_ACCNTNG_ITM_10,Cash on hands,Cash on hand
```

#### 1.2 Current ANACREDIT Format
```csv
rolc,join_identifier
ANCRDT_INSTRMNT_C_1,Loans and advances
ANCRDT_INSTRMNT_C_1,Non Negotiable bonds
```

#### 1.3 New Format: Single Variable with Explicit Variable Name
```csv
Main Category,Name,slice_name
TYP_INSTRMNT=TYP_INSTRMNT_970,Other commitments received,Other commitments received
```

#### 1.4 New Format: Multi-Variable Condition
```csv
Main Category,Name,slice_name
TYP_INSTRMNT=TYP_INSTRMNT_970:TYP_CLLRL=TYP_CLLRL_1,Collateralized commitments,Collateralized commitments
```

#### 1.5 New Format: No Breakdown (Empty Main Category)
```csv
Main Category,Name,slice_name
,,All Counterparties
```

### 2. Framework-Specific Configuration

#### 2.1 Single Configuration File per Framework
**Use Case**: FINREP (current approach)
- One file: `join_for_product_to_reference_category_FINREP_REF.csv`
- Applied to all FINREP reports
- Contains all product breakdowns for the framework

#### 2.2 Per-Output-Layer-Cube Configuration
**Use Case**: ANACREDIT
- Multiple files, one per output layer cube:
  - `join_for_product_to_reference_category_ANACREDIT_CNTRPRTY.csv`
  - `join_for_product_to_reference_category_ANACREDIT_INSTRMNT.csv`
  - `join_for_product_to_reference_category_ANACREDIT_FNNCL.csv`
  - ... (10 total for ANACREDIT)

#### 2.3 Configuration File Discovery
**Requirement**: The system should automatically discover and apply the appropriate configuration.

**Implementation**:
```
resources/joins_configuration/
├── join_for_product_to_reference_category_FINREP_REF.csv      # All FINREP
├── join_for_product_to_reference_category_ANACREDIT_CNTRPRTY.csv  # ANACREDIT per-cube
├── join_for_product_to_reference_category_ANACREDIT_INSTRMNT.csv
└── ...
```

**File Naming Convention**:
- Framework-wide: `join_for_product_to_reference_category_{FRAMEWORK}_REF.csv`
- Per-cube: `join_for_product_to_reference_category_{FRAMEWORK}_{CUBE_CODE}.csv`

### 3. Breakdown Strategy Types

#### 3.1 Single Variable Breakdown
```
TYP_INSTRMNT=TYP_INSTRMNT_970
```
- Filter data where TYP_INSTRMNT equals TYP_INSTRMNT_970

#### 3.2 Multi-Variable Breakdown (AND Condition)
```
TYP_INSTRMNT=TYP_INSTRMNT_970:TYP_CLLRL=TYP_CLLRL_1
```
- Filter data where TYP_INSTRMNT equals TYP_INSTRMNT_970 AND TYP_CLLRL equals TYP_CLLRL_1

#### 3.3 No Breakdown
```
(empty or special marker like "NONE")
```
- No product filtering, process all data together

### 4. LDM and IL Support

**Requirement**: The flexible approach must work with both:
- **IL (Input Layer)**: Primary focus for initial implementation
- **LDM (Logical Data Model)**: Secondary support, same configuration format

## Technical Implementation

### 1. Configuration Parser Enhancement

#### 1.1 Main Category Parser
```python
# pybirdai/process_steps/joins_metadata/condition_parser.py

class BreakdownCondition:
    """Represents a product breakdown condition."""

    def __init__(self, condition_string: str):
        self.conditions = self._parse(condition_string)

    def _parse(self, condition_string: str) -> list[dict]:
        """
        Parse condition string into structured format.

        Examples:
        - "TYP_INSTRMNT_970" -> [{"variable": "TYP_INSTRMNT", "member": "TYP_INSTRMNT_970"}]
        - "TYP_INSTRMNT=TYP_INSTRMNT_970" -> [{"variable": "TYP_INSTRMNT", "member": "TYP_INSTRMNT_970"}]
        - "TYP_INSTRMNT=TYP_INSTRMNT_970:TYP_CLLRL=TYP_CLLRL_1" ->
            [{"variable": "TYP_INSTRMNT", "member": "TYP_INSTRMNT_970"},
             {"variable": "TYP_CLLRL", "member": "TYP_CLLRL_1"}]
        """
        if not condition_string or condition_string.strip() == "":
            return []  # No breakdown

        conditions = []
        parts = condition_string.split(":")

        for part in parts:
            if "=" in part:
                variable, member = part.split("=", 1)
                conditions.append({"variable": variable.strip(), "member": member.strip()})
            else:
                # Legacy format: infer variable from member prefix
                conditions.append(self._infer_variable(part.strip()))

        return conditions

    def _infer_variable(self, member: str) -> dict:
        """Infer variable name from member ID (legacy support)."""
        # e.g., TYP_INSTRMNT_970 -> TYP_INSTRMNT
        if member.startswith("TYP_INSTRMNT_"):
            return {"variable": "TYP_INSTRMNT", "member": member}
        elif member.startswith("TYP_ACCNTNG_ITM_"):
            return {"variable": "TYP_ACCNTNG_ITM", "member": member}
        # Add more inferences as needed
        raise ValueError(f"Cannot infer variable from member: {member}")

    def is_empty(self) -> bool:
        """Check if this represents a no-breakdown condition."""
        return len(self.conditions) == 0

    def get_filter_expression(self) -> str:
        """Generate Python filter expression."""
        if self.is_empty():
            return "True"  # No filtering

        expressions = [
            f"row['{c['variable']}'] == '{c['member']}'"
            for c in self.conditions
        ]
        return " and ".join(expressions)
```

#### 1.2 Configuration File Resolver
```python
# pybirdai/process_steps/joins_metadata/config_resolver.py

class JoinsConfigurationResolver:
    """Resolves which configuration file(s) to use for a given context."""

    def __init__(self, config_directory: str):
        self.config_directory = config_directory

    def get_configuration_files(self, framework: str, cube_code: str = None) -> list[str]:
        """
        Get configuration file(s) for the given framework and optional cube.

        Priority:
        1. Per-cube file: join_for_product_to_reference_category_{FRAMEWORK}_{CUBE_CODE}.csv
        2. Framework-wide file: join_for_product_to_reference_category_{FRAMEWORK}_REF.csv
        3. Default file (if exists)
        """
        files = []

        # Check for per-cube configuration
        if cube_code:
            per_cube_file = f"join_for_product_to_reference_category_{framework}_{cube_code}.csv"
            per_cube_path = os.path.join(self.config_directory, per_cube_file)
            if os.path.exists(per_cube_path):
                files.append(per_cube_path)
                return files  # Per-cube takes precedence

        # Check for framework-wide configuration
        framework_file = f"join_for_product_to_reference_category_{framework}_REF.csv"
        framework_path = os.path.join(self.config_directory, framework_file)
        if os.path.exists(framework_path):
            files.append(framework_path)

        return files
```

### 2. Enhanced Joins Metadata Creation

#### 2.1 Updated create_joins_meta_data.py
```python
# Key changes to pybirdai/entry_points/create_joins_meta_data.py

def create_cube_links_for_framework(framework: str, context: SDDContext):
    """Create cube links using flexible configuration."""
    resolver = JoinsConfigurationResolver(context.joins_config_directory)

    output_cubes = get_output_cubes_for_framework(framework)

    for output_cube in output_cubes:
        # Get appropriate configuration
        config_files = resolver.get_configuration_files(
            framework=framework,
            cube_code=output_cube.cube_id
        )

        for config_file in config_files:
            products = parse_products_from_config(config_file)

            for product in products:
                condition = BreakdownCondition(product.main_category)

                # Create cube_link for this product/output_cube combination
                create_cube_link(
                    output_cube=output_cube,
                    condition=condition,
                    context=context
                )
```

### 3. Python Stub Generation Updates

#### 3.1 Multi-Variable Filter Generation
```python
# Update to generate filter code that handles multi-variable conditions

def generate_filter_stub(condition: BreakdownCondition, output_cube: str) -> str:
    """Generate Python filter stub for the given condition."""

    if condition.is_empty():
        return f"""
def filter_{output_cube}(df):
    '''No product breakdown - process all data.'''
    return df
"""

    filter_expr = condition.get_filter_expression()
    condition_desc = " AND ".join(
        f"{c['variable']}={c['member']}" for c in condition.conditions
    )

    return f"""
def filter_{output_cube}_{condition.get_id()}(df):
    '''
    Filter for: {condition_desc}
    TODO: Implement technical join logic
    '''
    return df[{filter_expr}]
"""
```
### 4. JoinsConfigurationManager Updates
Also Make sure JoinsConfigurationManager as required

## Configuration Examples

### Example 1: FINREP (Current Style)
**File**: `join_for_product_to_reference_category_FINREP_REF.csv`
```csv
Main Category,Name,slice_name
TYP_INSTRMNT_970,Other commitments received,Other commitments received
TYP_INSTRMNT_241,Equity instruments,Equity instruments security
TYP_ACCNTNG_ITM_10,Cash on hands,Cash on hand
```

### Example 2: FINREP (Enhanced with Explicit Variable)
**File**: `join_for_product_to_reference_category_FINREP_REF.csv`
```csv
Main Category,Name,slice_name
TYP_INSTRMNT=TYP_INSTRMNT_970,Other commitments received,Other commitments received
TYP_ACCNTNG_ITM=TYP_ACCNTNG_ITM_10,Cash on hands,Cash on hand
```

### Example 3: ANACREDIT Counterparty (No Breakdown)
**File**: `join_for_product_to_reference_category_ANACREDIT_CNTRPRTY.csv`
```csv
Main Category,Name,slice_name
,,All Counterparties
```

### Example 4: ANACREDIT Instrument (Multi-Variable)
**File**: `join_for_product_to_reference_category_ANACREDIT_INSTRMNT.csv`
```csv
Main Category,Name,slice_name
TYP_INSTRMNT=TYP_INSTRMNT_100:TYP_CLLRL=TYP_CLLRL_1,Collateralized Loans Type 1,Collateralized Loans Type 1
TYP_INSTRMNT=TYP_INSTRMNT_100:TYP_CLLRL=TYP_CLLRL_2,Collateralized Loans Type 2,Collateralized Loans Type 2
TYP_INSTRMNT=TYP_INSTRMNT_200,Other Instruments,Other Instruments
```

### Example 5: ANACREDIT (Current Format - Different Structure)
**File**: `join_for_product_to_reference_category_ANCRDT_REF.csv`
```csv
rolc,join_identifier
ANCRDT_INSTRMNT_C_1,Loans and advances
ANCRDT_INSTRMNT_C_1,Non Negotiable bonds
```
Note: The ANACREDIT format uses different column names (`rolc`, `join_identifier`) and will need to be harmonized with the FINREP format or handled separately.

## Migration Strategy

### Phase 1: Parser Enhancement
1. Implement `BreakdownCondition` class with legacy format support
2. Add unit tests for all parsing scenarios
3. Ensure backward compatibility with existing FINREP configuration

### Phase 2: Configuration Resolver
1. Implement `JoinsConfigurationResolver` class
2. Add file discovery logic for per-cube configurations
3. Test with mock ANACREDIT configurations

### Phase 3: Joins Metadata Integration
1. Update `create_joins_meta_data.py` to use new parser and resolver
2. Update cube_link creation to handle multi-variable conditions
3. Update cube_structure_item_link creation accordingly

### Phase 4: Python Stub Generation
1. Update filter stub generation for multi-variable conditions
2. Handle no-breakdown scenario (passthrough filter)
3. Update join stub generation

### Phase 5: Testing with ANACREDIT
1. Create ANACREDIT configuration files (10 per-cube files)
2. Run full pipeline with ANACREDIT data
3. Validate generated cube_links and Python stubs

## Success Criteria

1. **Backward Compatibility**: Existing FINREP configuration continues to work unchanged
2. **New Format Support**: Parser correctly handles `VAR=MEMBER` and `VAR1=MEM1:VAR2=MEM2` formats
3. **Per-Cube Configuration**: System correctly discovers and applies per-cube configuration files
4. **No-Breakdown Support**: Empty Main Category results in no product filtering
5. **Multi-Variable Filters**: Generated Python code correctly filters on multiple variables
6. **ANACREDIT Ready**: All 10 ANACREDIT output tables can be configured independently

## Dependencies

- Existing `create_joins_meta_data.py` entry point
- Existing `CUBE_LINK` and `CUBE_STRUCTURE_ITEM_LINK` models
- Existing Python stub generation code
- CSV configuration files in `resources/joins_configuration/`

## Risks & Mitigation

1. **Backward Compatibility Break**: Existing configurations may fail
   - Mitigation: Legacy format detection and automatic conversion

2. **Configuration Complexity**: Users may create invalid configurations
   - Mitigation: Validation on load, clear error messages

3. **Performance**: Multi-variable conditions may slow processing
   - Mitigation: Index optimization, caching parsed conditions

## Future Considerations

1. **UI for Configuration**: Web interface for creating/editing join configurations
2. **Validation Rules**: Schema validation for configuration files
3. **OR Conditions**: Support for `VAR=MEM1|MEM2` syntax
4. **Dynamic Discovery**: Auto-detect required breakdowns from data analysis
