# Feature Specification: Enhanced Template Editor & Interactive Report Viewer

**Document Version:** 1.0
**Date:** 2026-01-26
**Author:** PyBIRD AI Development Team
**Status:** Draft

---

## Executive Summary

This specification defines two interconnected features for the PyBIRD AI application:

1. **Enhanced Template Editor** - A redesigned interface for creating and editing regulatory templates (TABLEs in the BIRD data model) with a rich hierarchical visualization of axes and ordinates.

2. **Interactive Report Viewer** - An advanced visualization of regulatory templates that allows users to see report structures with clickable cells, enabling on-demand datapoint execution directly from the visual representation.

These features target regulatory reporting workflows, particularly FINREP templates like F05_01 and F04_01 that have executable datapoint cells.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Feature 1: Enhanced Template Editor](#2-feature-1-enhanced-template-editor)
3. [Feature 2: Interactive Report Viewer](#3-feature-2-interactive-report-viewer)
4. [Data Model Considerations](#4-data-model-considerations)
5. [User Interface Design](#5-user-interface-design)
6. [API Specifications](#6-api-specifications)
7. [Technical Implementation](#7-technical-implementation)
8. [Security Considerations](#8-security-considerations)
9. [Testing Strategy](#9-testing-strategy)
10. [Future Enhancements](#10-future-enhancements)
11. [Appendix](#11-appendix)

---

## 1. Problem Statement

### 1.1 Current Limitations

**Template Editing:**
- The existing table amendment editor provides basic axis/ordinate management
- Hierarchical ordinate relationships are not visually intuitive
- No clear preview of how the final template structure will render
- Limited feedback on ordinate nesting depth and relationships

**Datapoint Execution:**
- Users must manually identify datapoint IDs from lists
- No visual context for where a datapoint fits within a regulatory template
- Disconnect between the report structure users understand and the technical execution interface
- Users cannot easily see which cells are executable vs informational

### 1.2 User Needs

**Regulatory Analysts need to:**
- Understand template structure at a glance
- Create custom templates with proper hierarchical structure
- Execute datapoints in context of the visual report they know
- Click on a cell in a FINREP template and see the calculated value

**Technical Users need to:**
- Debug datapoint calculations with visual context
- Verify cell-to-datapoint mappings
- Understand which cells have executable logic
- Track lineage from visual cell to underlying data

---

## 2. Feature 1: Enhanced Template Editor

### 2.1 Overview

A redesigned template editor that provides:
- Clear hierarchical visualization of axes and ordinates
- Intuitive tree-based ordinate management
- Real-time preview of template structure
- Drag-and-drop ordinate reordering
- Visual distinction between header levels

### 2.2 Functional Requirements

#### FR-1.1: Hierarchical Ordinate Tree View

**Description:** Display ordinates in a collapsible tree structure that clearly shows parent-child relationships.

**Acceptance Criteria:**
- Ordinates display as a tree with indentation reflecting `level` field
- Parent ordinates are expandable/collapsible
- Abstract headers (`is_abstract_header=True`) display with distinct styling (e.g., bold, gray background)
- Leaf ordinates (those with `ORDINATE_ITEM` associations) show associated variable/member
- Tree supports unlimited nesting depth

**Visual Representation:**
```
Y-Axis (Rows)
├── [Abstract] Total Assets
│   ├── [Abstract] Financial Assets
│   │   ├── Debt securities (VAR: ACCNTNG_CLSSFCTN, MBR: 2)
│   │   ├── Equity instruments (VAR: ACCNTNG_CLSSFCTN, MBR: 3)
│   │   └── Loans and advances (VAR: ACCNTNG_CLSSFCTN, MBR: 4)
│   └── [Abstract] Non-financial Assets
│       └── Property, plant and equipment (VAR: ...)
└── [Abstract] Total Liabilities
    └── ...

X-Axis (Columns)
├── Carrying amount (VAR: MTRC, MBR: CRRYNG_AMNT)
├── Accumulated impairment (VAR: MTRC, MBR: ACCMLTD_IMPRMNT)
└── ...
```

#### FR-1.2: Ordinate Detail Panel

**Description:** A side panel showing full details of the selected ordinate.

**Acceptance Criteria:**
- Displays ordinate metadata: name, code, level, path
- Shows all `ORDINATE_ITEM` associations with:
  - Variable name and ID
  - Member name and ID (with domain context)
  - Hierarchy information if applicable
- Editable fields for ordinate properties
- Quick actions: Add child, Delete, Move up/down

#### FR-1.3: Multi-Axis Support

**Description:** Clear separation and management of X (column) and Y (row) axes.

**Acceptance Criteria:**
- Dedicated tree panel for each axis orientation
- Visual indicator of axis orientation (e.g., icons, labels)
- Support for multiple axes per orientation
- Clear ordering within each orientation

#### FR-1.4: Real-time Table Preview

**Description:** Live preview of the resulting table structure as ordinates are added/modified.

**Acceptance Criteria:**
- Preview updates automatically on any change
- Shows hierarchical headers with appropriate colspan/rowspan
- Indicates cells that would be created at row-column intersections
- Highlights currently selected ordinate's row/column in preview
- Abstract headers render as spanning headers, not data cells

**Preview Rendering Logic:**
```
If Y-axis has hierarchy:
  - Abstract headers span all child columns horizontally
  - Leaf ordinates create individual rows

If X-axis has hierarchy:
  - Abstract headers span all child rows vertically
  - Leaf ordinates create individual columns

Cells created at intersection of:
  - Leaf Y-ordinate × Leaf X-ordinate
```

#### FR-1.5: Drag-and-Drop Reordering

**Description:** Reorder ordinates within an axis using drag-and-drop.

**Acceptance Criteria:**
- Drag ordinates to change order within same level
- Drag ordinates to change parent (re-parenting)
- Visual feedback during drag operation
- Automatic `order` and `level` recalculation on drop
- Cascade `path` updates for all descendants

#### FR-1.6: Variable/Member Selection

**Description:** Improved interface for associating ordinates with BIRD variables and members.

**Acceptance Criteria:**
- Searchable dropdown for variable selection
- Filtered member list based on selected variable's domain
- Support for member hierarchy selection
- Clear display of current associations
- Bulk association capability for common patterns

### 2.3 Non-Functional Requirements

- **Performance:** Tree operations complete in < 200ms for templates with up to 500 ordinates
- **Usability:** Keyboard navigation support (arrow keys, Enter, Delete)
- **Accessibility:** ARIA labels for tree nodes, screen reader support
- **Persistence:** Auto-save draft changes every 30 seconds

---

## 3. Feature 2: Interactive Report Viewer

### 3.1 Overview

A visual representation of regulatory templates (like FINREP F05_01, F04_01) that:
- Renders the template as users see it in regulatory documentation
- Displays hierarchical headers with proper spans
- Makes executable cells clickable for on-demand datapoint execution
- Shows calculation results inline

### 3.2 Functional Requirements

#### FR-2.1: Template Rendering

**Description:** Render a TABLE as an HTML table with proper structure matching regulatory layout.

**Acceptance Criteria:**
- Render matches official EBA template layout
- Hierarchical column headers with correct colspan
- Hierarchical row headers with correct rowspan
- Cell shading (`is_shaded=True`) shown as gray/disabled
- Table scrollable for large templates

**Example: F05_01 Layout**
```
┌─────────────────────────────────────────────────────────────────┐
│                          F 05.01                                │
├──────────────────────┬──────────────────────────────────────────┤
│                      │        Breakdown by Counterparty         │
│                      ├──────────┬──────────┬──────────┬─────────┤
│                      │ Central  │ Credit   │ Other    │  Non-   │
│                      │ banks    │ instit.  │ financial│financial│
├──────────────────────┼──────────┼──────────┼──────────┼─────────┤
│ ASSETS               │          │          │          │         │
├──────────────────────┼──────────┼──────────┼──────────┼─────────┤
│ Debt securities      │  [EXEC]  │  [EXEC]  │  [EXEC]  │ [EXEC]  │
│ Equity instruments   │  [EXEC]  │  [EXEC]  │  [EXEC]  │ [EXEC]  │
│ Loans and advances   │  [EXEC]  │  [EXEC]  │  [EXEC]  │ [EXEC]  │
├──────────────────────┼──────────┼──────────┼──────────┼─────────┤
│ LIABILITIES          │ [SHADE]  │ [SHADE]  │ [SHADE]  │ [SHADE] │
└──────────────────────┴──────────┴──────────┴──────────┴─────────┘

Legend:
[EXEC]  = Executable cell (clickable)
[SHADE] = Shaded/disabled cell
```

#### FR-2.2: Cell Type Indication

**Description:** Visual distinction between cell types.

**Cell Types:**
| Type | Visual | Behavior |
|------|--------|----------|
| Executable | Blue border, pointer cursor, hover effect | Clickable, triggers datapoint execution |
| Shaded | Gray background, disabled appearance | Non-interactive, shows "N/A" or empty |
| Header | Bold text, background color | Non-interactive, displays ordinate name |
| Calculated | Green background after execution | Shows result value |
| Error | Red border | Shows error message on hover |

**Acceptance Criteria:**
- Executable cells identified by presence of `table_cell_combination_id`
- Clear visual hover state for clickable cells
- Tooltip showing cell coordinates and datapoint ID on hover

#### FR-2.3: Cell Click Execution

**Description:** Clicking an executable cell triggers datapoint execution and displays the result.

**Acceptance Criteria:**
- Single click initiates execution
- Loading indicator while executing (spinner in cell)
- Result displayed in cell after completion
- Result persists until page refresh or explicit clear
- Error handling with user-friendly messages

**Interaction Flow:**
```
1. User clicks cell → Cell shows loading spinner
2. AJAX call to /api/report/cell/execute/ with cell_id
3. Backend:
   a. Look up TABLE_CELL.table_cell_combination_id
   b. Execute Cell_<combination_id> datapoint
   c. Return metric_value result
4. Frontend updates cell with result value
5. Cell style changes to "calculated" state
```

#### FR-2.4: Execution Results Panel

**Description:** A collapsible side panel showing detailed execution results.

**Acceptance Criteria:**
- Shows when any cell is executed
- Displays:
  - Datapoint ID
  - Calculated value
  - Execution timestamp
  - Execution duration
  - Optional: Input data summary
- History of executed cells in current session
- Clear all results action

#### FR-2.5: Batch Execution

**Description:** Execute all executable cells in the template at once.

**Acceptance Criteria:**
- "Execute All" button in toolbar
- Progress indicator showing completion status
- Ability to cancel batch execution
- Results populate cells as they complete
- Summary report after completion

#### FR-2.6: Lineage Integration

**Description:** View data lineage for an executed cell.

**Acceptance Criteria:**
- "Show Lineage" option for executed cells
- Opens lineage viewer showing:
  - Source tables used
  - Rows consumed
  - Filter conditions applied
  - Calculation path
- Link to full lineage visualization (AORTA)

#### FR-2.7: Template Selection

**Description:** Allow users to select which template to view.

**Acceptance Criteria:**
- Dropdown/search for template selection
- Filter by framework (FINREP, COREP, etc.)
- Filter by version
- Recent templates list
- Quick access to F05_01 and F04_01 (common templates)

### 3.3 Non-Functional Requirements

- **Performance:**
  - Template rendering < 1 second for templates up to 100x100 cells
  - Individual cell execution feedback within 100ms (loading state)
  - Cell result display within execution time + 100ms
- **Scalability:** Handle batch execution of 500+ cells
- **Responsiveness:** Horizontal/vertical scroll for large templates
- **Caching:** Cache cell results for the session duration

---

## 4. Data Model Considerations

### 4.1 Existing Model Usage

The features leverage existing models without modification:

```
TABLE
├── table_id (PK)
├── code (e.g., "F_05_01_REF_FINREP_3_0")
├── name
└── [related: AXIS]

AXIS
├── axis_id (PK)
├── orientation ("X" or "Y")
├── order
├── table_id (FK → TABLE)
└── [related: AXIS_ORDINATE]

AXIS_ORDINATE
├── axis_ordinate_id (PK)
├── name (display text)
├── code
├── order (display sequence)
├── level (hierarchy depth: 0, 1, 2, ...)
├── path (hierarchical path)
├── is_abstract_header (True for grouping headers)
├── parent_axis_ordinate_id (FK → self)
├── axis_id (FK → AXIS)
└── [related: ORDINATE_ITEM]

ORDINATE_ITEM
├── axis_ordinate_id (FK → AXIS_ORDINATE)
├── variable_id (FK → VARIABLE)
├── member_id (FK → MEMBER)
└── member_hierarchy_id (FK → MEMBER_HIERARCHY)

TABLE_CELL
├── cell_id (PK)
├── table_id (FK → TABLE)
├── is_shaded (Boolean)
├── table_cell_combination_id (links to COMBINATION/Cell_ class)
└── [related: CELL_POSITION]

CELL_POSITION
├── cell_id (FK → TABLE_CELL)
└── axis_ordinate_id (FK → AXIS_ORDINATE)
```

### 4.2 Key Relationships for Interactive Report

**Finding the datapoint for a cell:**
```python
# Given a clicked cell
cell = TABLE_CELL.objects.get(cell_id=clicked_cell_id)

# The combination_id maps to the Cell_ class
combination_id = cell.table_cell_combination_id
# e.g., "F_05_01_REF_FINREP_3_0_152589_REF"

# Execute using existing infrastructure
from pybirdai.entry_points.execute_datapoint import RunExecuteDataPoint
result = RunExecuteDataPoint.run_execute_data_point(combination_id)
```

**Finding cell position (row/column):**
```python
# Get cell positions
positions = CELL_POSITION.objects.filter(cell_id=cell).select_related('axis_ordinate_id__axis_id')

# Identify row and column ordinates
for pos in positions:
    if pos.axis_ordinate_id.axis_id.orientation == 'Y':
        row_ordinate = pos.axis_ordinate_id
    else:
        column_ordinate = pos.axis_ordinate_id
```

### 4.3 New Model Considerations

**Optional: CellExecutionResult (for caching)**
```python
class CellExecutionResult(models.Model):
    """Cached execution results for cells"""
    cell = models.ForeignKey(TABLE_CELL, on_delete=models.CASCADE)
    result_value = models.CharField(max_length=255)
    executed_at = models.DateTimeField(auto_now_add=True)
    execution_duration_ms = models.IntegerField()
    session_id = models.CharField(max_length=64)  # User session
    is_valid = models.BooleanField(default=True)  # Invalidated when source data changes

    class Meta:
        indexes = [
            models.Index(fields=['cell', 'session_id']),
        ]
```

---

## 5. User Interface Design

### 5.1 Template Editor Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│ [←] Template Editor: F05_01 Custom Amendment              [Save] [Cancel]│
├──────────────────────────────────────────────────────────────────────────┤
│ ┌────────────────┐ ┌────────────────────────────┐ ┌────────────────────┐ │
│ │ AXES & ORDS    │ │     TABLE PREVIEW          │ │ ORDINATE DETAILS   │ │
│ │                │ │                            │ │                    │ │
│ │ ▼ Y-Axis (Rows)│ │ ┌──────┬──────┬──────┐    │ │ Name: Debt secur.  │ │
│ │   ▼ Assets     │ │ │      │ Col1 │ Col2 │    │ │ Code: ORD_001      │ │
│ │     • Debt sec │ │ ├──────┼──────┼──────┤    │ │ Level: 2           │ │
│ │     • Equity   │ │ │Assets│      │      │    │ │                    │ │
│ │     • Loans    │ │ │ Debt │ ■■■■ │ ■■■■ │    │ │ ── Variables ──    │ │
│ │   ▼ Liabilities│ │ │ Eqty │ ■■■■ │ ■■■■ │    │ │ ACCNTNG_CLSSFCTN   │ │
│ │     • Deposits │ │ │ Loan │ ■■■■ │ ■■■■ │    │ │ Member: 2 (FVPL)   │ │
│ │                │ │ │Liab. │ ░░░░ │ ░░░░ │    │ │                    │ │
│ │ ▼ X-Axis (Cols)│ │ │ Deps │ ░░░░ │ ░░░░ │    │ │ [+ Add Variable]   │ │
│ │   • Col1       │ │ └──────┴──────┴──────┘    │ │                    │ │
│ │   • Col2       │ │                            │ │ [Delete Ordinate]  │ │
│ │                │ │ Legend: ■=cell ░=shaded   │ │                    │ │
│ │ [+ Add Axis]   │ │                            │ │                    │ │
│ └────────────────┘ └────────────────────────────┘ └────────────────────┘ │
├──────────────────────────────────────────────────────────────────────────┤
│ Status: Draft │ Last saved: 2 min ago │ Cells: 12 │ Ordinates: 8       │
└──────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Interactive Report Viewer Layout

```
┌──────────────────────────────────────────────────────────────────────────┐
│ Interactive Report Viewer                                                │
├──────────────────────────────────────────────────────────────────────────┤
│ Template: [F_05_01_REF_FINREP_3_0    ▼] [🔍 Search]     [Execute All ▶] │
├──────────────────────────────────────────────────────────────────────────┤
│ ┌────────────────────────────────────────────────────┐ ┌────────────────┐│
│ │                                                    │ │ EXECUTION LOG  ││
│ │              F 05.01 - Loans and advances          │ │                ││
│ │                                                    │ │ ● Cell R3C2    ││
│ │ ┌─────────────┬─────────┬─────────┬─────────┐     │ │   83,491,250   ││
│ │ │             │Central  │Credit   │Other    │     │ │   [Show Lineage]│
│ │ │             │banks    │instit.  │fin corp │     │ │                ││
│ │ ├─────────────┼─────────┼─────────┼─────────┤     │ │ ● Cell R4C2    ││
│ │ │ ASSETS      │         │         │         │     │ │   1,234,567    ││
│ │ ├─────────────┼─────────┼─────────┼─────────┤     │ │   [Show Lineage]│
│ │ │ Debt sec.   │[Click]  │[Click]  │[Click]  │     │ │                ││
│ │ │ Equity inst.│83491250 │[Click]  │[Click]  │     │ │ ────────────── ││
│ │ │ Loans & adv.│[Click]  │1234567  │[Click]  │     │ │ Executed: 2    ││
│ │ ├─────────────┼─────────┼─────────┼─────────┤     │ │ Pending: 10    ││
│ │ │ LIABILITIES │░░░░░░░░░│░░░░░░░░░│░░░░░░░░░│     │ │ Errors: 0      ││
│ │ └─────────────┴─────────┴─────────┴─────────┘     │ │                ││
│ │                                                    │ │ [Clear All]    ││
│ └────────────────────────────────────────────────────┘ └────────────────┘│
├──────────────────────────────────────────────────────────────────────────┤
│ Click any blue cell to execute its datapoint. Shaded cells are N/A.     │
└──────────────────────────────────────────────────────────────────────────┘
```

### 5.3 Cell States (CSS Classes)

```css
/* Executable cell - not yet executed */
.cell-executable {
    background: #ffffff;
    border: 2px solid #2196F3;
    cursor: pointer;
}
.cell-executable:hover {
    background: #e3f2fd;
    box-shadow: 0 2px 4px rgba(33,150,243,0.3);
}

/* Cell being executed */
.cell-loading {
    background: #fff3e0;
    border: 2px solid #ff9800;
}
.cell-loading::after {
    content: '';
    /* spinner animation */
}

/* Executed cell with result */
.cell-calculated {
    background: #e8f5e9;
    border: 2px solid #4caf50;
    font-weight: bold;
}

/* Cell with execution error */
.cell-error {
    background: #ffebee;
    border: 2px solid #f44336;
}

/* Shaded/disabled cell */
.cell-shaded {
    background: #e0e0e0;
    color: #9e9e9e;
    cursor: not-allowed;
}

/* Header cell */
.cell-header {
    background: #f5f5f5;
    font-weight: bold;
    text-align: left;
}

/* Abstract header (spanning) */
.cell-header-abstract {
    background: #eeeeee;
    font-weight: bold;
    border-bottom: 2px solid #bdbdbd;
}
```

---

## 6. API Specifications

### 6.1 Template Editor APIs

#### GET /api/table-amendment/{table_id}/hierarchical-structure/

**Description:** Returns the table structure with explicit hierarchy information.

**Response:**
```json
{
    "table_id": "TBL_123",
    "name": "F05_01 Amendment",
    "code": "F_05_01_CUSTOM",
    "axes": {
        "Y": [
            {
                "axis_id": "AXIS_001",
                "name": "Row Axis",
                "ordinates": [
                    {
                        "ordinate_id": "ORD_001",
                        "name": "Assets",
                        "code": "R010",
                        "level": 0,
                        "is_abstract_header": true,
                        "children": [
                            {
                                "ordinate_id": "ORD_002",
                                "name": "Debt securities",
                                "code": "R020",
                                "level": 1,
                                "is_abstract_header": false,
                                "ordinate_items": [
                                    {
                                        "variable_id": "VAR_001",
                                        "variable_name": "ACCNTNG_CLSSFCTN",
                                        "member_id": "MBR_002",
                                        "member_name": "2"
                                    }
                                ],
                                "children": []
                            }
                        ]
                    }
                ]
            }
        ],
        "X": [/* similar structure */]
    },
    "preview_grid": {
        "headers": {/* colspan/rowspan info for rendering */},
        "cells": [/* cell matrix for preview */]
    }
}
```

#### PUT /api/table-amendment/ordinate/{ordinate_id}/reparent/

**Description:** Move an ordinate to a new parent.

**Request:**
```json
{
    "new_parent_id": "ORD_005",  // null for root level
    "new_order": 2
}
```

**Response:**
```json
{
    "success": true,
    "updated_ordinates": [
        {"ordinate_id": "ORD_002", "level": 2, "path": "001.005.002"}
    ]
}
```

### 6.2 Interactive Report APIs

#### GET /api/report/templates/

**Description:** List available templates for the report viewer.

**Query Parameters:**
- `framework`: Filter by framework (FINREP, COREP, etc.)
- `version`: Filter by version
- `search`: Search by name/code
- `has_executable_cells`: Boolean, filter to templates with datapoints

**Response:**
```json
{
    "templates": [
        {
            "table_id": "F_05_01_REF_FINREP_3_0",
            "code": "F 05.01",
            "name": "Loans and advances by product and by counterparty sector",
            "framework": "FINREP",
            "version": "3.0",
            "cell_count": 156,
            "executable_cell_count": 120,
            "row_count": 12,
            "column_count": 13
        }
    ],
    "total": 250,
    "page": 1,
    "page_size": 50
}
```

#### GET /api/report/{table_id}/render/

**Description:** Get the full table structure for rendering.

**Response:**
```json
{
    "table_id": "F_05_01_REF_FINREP_3_0",
    "name": "F 05.01",
    "description": "Loans and advances by product and by counterparty sector",
    "column_headers": [
        {
            "levels": [
                [
                    {"text": "Breakdown by counterparty", "colspan": 4, "rowspan": 1}
                ],
                [
                    {"text": "Central banks", "colspan": 1, "rowspan": 1, "ordinate_id": "ORD_C1"},
                    {"text": "Credit institutions", "colspan": 1, "rowspan": 1, "ordinate_id": "ORD_C2"},
                    {"text": "Other financial", "colspan": 1, "rowspan": 1, "ordinate_id": "ORD_C3"},
                    {"text": "Non-financial", "colspan": 1, "rowspan": 1, "ordinate_id": "ORD_C4"}
                ]
            ]
        }
    ],
    "row_headers": [
        {
            "text": "ASSETS",
            "rowspan": 3,
            "colspan": 1,
            "level": 0,
            "is_abstract": true,
            "ordinate_id": "ORD_R1"
        },
        {
            "text": "Debt securities",
            "rowspan": 1,
            "colspan": 1,
            "level": 1,
            "is_abstract": false,
            "ordinate_id": "ORD_R2"
        }
    ],
    "rows": [
        {
            "row_ordinate_id": "ORD_R2",
            "row_index": 0,
            "cells": [
                {
                    "cell_id": "CELL_001",
                    "column_ordinate_id": "ORD_C1",
                    "column_index": 0,
                    "is_shaded": false,
                    "is_executable": true,
                    "datapoint_id": "F_05_01_REF_FINREP_3_0_152589_REF",
                    "cached_value": null
                },
                {
                    "cell_id": "CELL_002",
                    "column_ordinate_id": "ORD_C2",
                    "column_index": 1,
                    "is_shaded": true,
                    "is_executable": false,
                    "datapoint_id": null,
                    "cached_value": null
                }
            ]
        }
    ],
    "metadata": {
        "total_cells": 156,
        "executable_cells": 120,
        "shaded_cells": 36
    }
}
```

#### POST /api/report/cell/{cell_id}/execute/

**Description:** Execute the datapoint for a specific cell.

**Request:**
```json
{
    "force_refresh": false,  // Ignore cache
    "include_lineage": false  // Include lineage summary in response
}
```

**Response:**
```json
{
    "success": true,
    "cell_id": "CELL_001",
    "datapoint_id": "F_05_01_REF_FINREP_3_0_152589_REF",
    "result": {
        "value": "83491250",
        "formatted_value": "83,491,250",
        "data_type": "numeric"
    },
    "execution": {
        "duration_ms": 1234,
        "timestamp": "2026-01-26T14:30:00Z"
    },
    "lineage_summary": {  // Only if include_lineage=true
        "source_tables": ["Equity_instruments_security", "Debt_securities"],
        "rows_processed": 1523,
        "trail_id": "TRAIL_ABC123"
    }
}
```

**Error Response:**
```json
{
    "success": false,
    "cell_id": "CELL_001",
    "error": {
        "code": "EXECUTION_ERROR",
        "message": "Failed to execute datapoint: Cell class not found",
        "details": "Cell_F_05_01_REF_FINREP_3_0_152589_REF not in generated code"
    }
}
```

#### POST /api/report/{table_id}/execute-all/

**Description:** Execute all executable cells in the template.

**Request:**
```json
{
    "parallel": true,  // Execute cells in parallel
    "max_concurrent": 5,  // Max parallel executions
    "skip_cached": true  // Skip cells with cached results
}
```

**Response (streaming):** Server-Sent Events (SSE)
```
event: progress
data: {"completed": 1, "total": 120, "cell_id": "CELL_001", "result": "83491250"}

event: progress
data: {"completed": 2, "total": 120, "cell_id": "CELL_002", "result": "1234567"}

...

event: complete
data: {"completed": 120, "total": 120, "errors": 0, "duration_ms": 45000}
```

#### GET /api/report/cell/{cell_id}/lineage/

**Description:** Get detailed lineage for an executed cell.

**Response:**
```json
{
    "cell_id": "CELL_001",
    "datapoint_id": "F_05_01_REF_FINREP_3_0_152589_REF",
    "trail_id": "TRAIL_ABC123",
    "lineage": {
        "source_tables": [
            {
                "table_name": "Equity_instruments_security",
                "rows_used": 45,
                "fields_accessed": ["CRRYNG_AMNT", "ACCNTNG_CLSSFCTN", "INSTTTNL_SCTR"]
            }
        ],
        "filters_applied": [
            {
                "field": "ACCNTNG_CLSSFCTN",
                "operator": "IN",
                "values": ["2"]
            },
            {
                "field": "INSTTTNL_SCTR",
                "operator": "IN",
                "values": ["S121", "S126", "S127", "S128", "S129"]
            }
        ],
        "aggregation": {
            "function": "SUM",
            "field": "CRRYNG_AMNT"
        },
        "calculation_path": [
            "Cell_F_05_01_REF_FINREP_3_0_152589_REF.init()",
            "Cell_F_05_01_REF_FINREP_3_0_152589_REF.calc_referenced_items()",
            "Cell_F_05_01_REF_FINREP_3_0_152589_REF.metric_value()"
        ]
    },
    "visualization_url": "/pybirdai/lineage/view/TRAIL_ABC123/"
}
```

---

## 7. Technical Implementation

### 7.1 Backend Implementation

#### 7.1.1 Cell Execution Service

**Location:** `pybirdai/services/cell_execution_service.py`

```python
from typing import Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime
import time

from pybirdai.models import TABLE_CELL
from pybirdai.entry_points.execute_datapoint import RunExecuteDataPoint

@dataclass
class CellExecutionResult:
    success: bool
    value: Optional[str]
    error: Optional[str]
    duration_ms: int
    timestamp: datetime

class CellExecutionService:
    """Service for executing datapoints from cell references."""

    @staticmethod
    def execute_cell(cell_id: str, include_lineage: bool = False) -> CellExecutionResult:
        """Execute the datapoint associated with a table cell."""
        start_time = time.time()

        try:
            # Get the cell and its combination ID
            cell = TABLE_CELL.objects.get(cell_id=cell_id)

            if not cell.table_cell_combination_id:
                return CellExecutionResult(
                    success=False,
                    value=None,
                    error="Cell has no associated datapoint",
                    duration_ms=0,
                    timestamp=datetime.now()
                )

            if cell.is_shaded:
                return CellExecutionResult(
                    success=False,
                    value=None,
                    error="Cell is shaded/disabled",
                    duration_ms=0,
                    timestamp=datetime.now()
                )

            # Execute the datapoint
            datapoint_id = cell.table_cell_combination_id
            result = RunExecuteDataPoint.run_execute_data_point(datapoint_id)

            duration_ms = int((time.time() - start_time) * 1000)

            return CellExecutionResult(
                success=True,
                value=result,
                error=None,
                duration_ms=duration_ms,
                timestamp=datetime.now()
            )

        except TABLE_CELL.DoesNotExist:
            return CellExecutionResult(
                success=False,
                value=None,
                error=f"Cell not found: {cell_id}",
                duration_ms=0,
                timestamp=datetime.now()
            )
        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            return CellExecutionResult(
                success=False,
                value=None,
                error=str(e),
                duration_ms=duration_ms,
                timestamp=datetime.now()
            )

    @staticmethod
    def is_cell_executable(cell: TABLE_CELL) -> bool:
        """Check if a cell can be executed."""
        return (
            cell.table_cell_combination_id is not None
            and not cell.is_shaded
        )
```

#### 7.1.2 Table Rendering Service

**Location:** `pybirdai/services/table_rendering_service.py`

```python
from typing import List, Dict, Any
from collections import defaultdict

from pybirdai.models import TABLE, AXIS, AXIS_ORDINATE, TABLE_CELL, CELL_POSITION

class TableRenderingService:
    """Service for rendering table structures with hierarchical headers."""

    @staticmethod
    def render_table(table_id: str) -> Dict[str, Any]:
        """Generate a renderable table structure."""
        table = TABLE.objects.get(table_id=table_id)

        # Get axes
        axes = AXIS.objects.filter(table_id=table).order_by('orientation', 'order')

        y_axes = [a for a in axes if a.orientation in ('Y', '2')]
        x_axes = [a for a in axes if a.orientation in ('X', '1')]

        # Build ordinate trees
        row_ordinates = TableRenderingService._build_ordinate_tree(y_axes)
        col_ordinates = TableRenderingService._build_ordinate_tree(x_axes)

        # Get cells with positions
        cells = TABLE_CELL.objects.filter(table_id=table)
        positions = CELL_POSITION.objects.filter(
            cell_id__in=cells
        ).select_related('cell_id', 'axis_ordinate_id')

        # Build cell lookup: (row_ord_id, col_ord_id) -> cell
        cell_lookup = TableRenderingService._build_cell_lookup(positions)

        # Get leaf ordinates (non-abstract)
        row_leaves = TableRenderingService._get_leaf_ordinates(row_ordinates)
        col_leaves = TableRenderingService._get_leaf_ordinates(col_ordinates)

        # Build header structure
        column_headers = TableRenderingService._build_column_headers(col_ordinates)
        row_headers = TableRenderingService._build_row_headers(row_ordinates)

        # Build data rows
        rows = []
        for row_ord in row_leaves:
            row_cells = []
            for col_ord in col_leaves:
                cell = cell_lookup.get((row_ord.axis_ordinate_id, col_ord.axis_ordinate_id))
                row_cells.append({
                    'cell_id': cell.cell_id if cell else None,
                    'column_ordinate_id': col_ord.axis_ordinate_id,
                    'is_shaded': cell.is_shaded if cell else True,
                    'is_executable': (
                        cell is not None
                        and cell.table_cell_combination_id
                        and not cell.is_shaded
                    ),
                    'datapoint_id': cell.table_cell_combination_id if cell else None,
                })
            rows.append({
                'row_ordinate_id': row_ord.axis_ordinate_id,
                'cells': row_cells
            })

        return {
            'table_id': table.table_id,
            'name': table.name,
            'code': table.code,
            'column_headers': column_headers,
            'row_headers': row_headers,
            'rows': rows,
            'metadata': {
                'total_cells': len(cells),
                'executable_cells': sum(
                    1 for c in cells
                    if c.table_cell_combination_id and not c.is_shaded
                ),
                'shaded_cells': sum(1 for c in cells if c.is_shaded)
            }
        }

    @staticmethod
    def _build_ordinate_tree(axes: List[AXIS]) -> List[Dict]:
        """Build hierarchical ordinate tree from axes."""
        result = []
        for axis in axes:
            ordinates = AXIS_ORDINATE.objects.filter(
                axis_id=axis
            ).order_by('level', 'order')

            # Build tree structure
            ordinate_dict = {o.axis_ordinate_id: o for o in ordinates}
            children_map = defaultdict(list)
            roots = []

            for o in ordinates:
                if o.parent_axis_ordinate_id:
                    children_map[o.parent_axis_ordinate_id.axis_ordinate_id].append(o)
                else:
                    roots.append(o)

            def build_node(ord_obj):
                return {
                    'ordinate_id': ord_obj.axis_ordinate_id,
                    'name': ord_obj.name,
                    'code': ord_obj.code,
                    'level': ord_obj.level,
                    'is_abstract_header': ord_obj.is_abstract_header,
                    'children': [
                        build_node(c)
                        for c in children_map.get(ord_obj.axis_ordinate_id, [])
                    ]
                }

            result.extend([build_node(r) for r in roots])

        return result

    @staticmethod
    def _get_leaf_ordinates(tree: List[Dict]) -> List[AXIS_ORDINATE]:
        """Get all leaf (non-abstract) ordinates in order."""
        leaves = []

        def traverse(nodes):
            for node in nodes:
                if node['children']:
                    traverse(node['children'])
                else:
                    leaves.append(AXIS_ORDINATE.objects.get(
                        axis_ordinate_id=node['ordinate_id']
                    ))

        traverse(tree)
        return leaves

    @staticmethod
    def _build_cell_lookup(positions) -> Dict:
        """Build lookup from (row_ord, col_ord) to cell."""
        cell_positions = defaultdict(dict)

        for pos in positions:
            cell = pos.cell_id
            ordinate = pos.axis_ordinate_id
            axis = ordinate.axis_id

            if axis.orientation in ('Y', '2'):
                cell_positions[cell.cell_id]['row'] = ordinate.axis_ordinate_id
            else:
                cell_positions[cell.cell_id]['col'] = ordinate.axis_ordinate_id

        lookup = {}
        for cell_id, coords in cell_positions.items():
            if 'row' in coords and 'col' in coords:
                lookup[(coords['row'], coords['col'])] = TABLE_CELL.objects.get(
                    cell_id=cell_id
                )

        return lookup
```

### 7.2 Frontend Implementation

#### 7.2.1 Interactive Report Component

**Location:** `pybirdai/static/js/interactive_report.js`

```javascript
class InteractiveReportViewer {
    constructor(containerId, tableId) {
        this.container = document.getElementById(containerId);
        this.tableId = tableId;
        this.executedCells = new Map();
        this.executionQueue = [];
        this.isExecutingAll = false;

        this.init();
    }

    async init() {
        this.showLoading();
        try {
            const data = await this.fetchTableStructure();
            this.renderTable(data);
            this.attachEventListeners();
        } catch (error) {
            this.showError(error.message);
        }
    }

    async fetchTableStructure() {
        const response = await fetch(`/api/report/${this.tableId}/render/`);
        if (!response.ok) throw new Error('Failed to load table structure');
        return response.json();
    }

    renderTable(data) {
        const html = `
            <div class="report-header">
                <h2>${data.name}</h2>
                <p>${data.code}</p>
            </div>
            <div class="report-table-container">
                <table class="report-table">
                    ${this.renderColumnHeaders(data.column_headers)}
                    ${this.renderBody(data.row_headers, data.rows)}
                </table>
            </div>
            <div class="report-stats">
                Total: ${data.metadata.total_cells} |
                Executable: ${data.metadata.executable_cells} |
                Shaded: ${data.metadata.shaded_cells}
            </div>
        `;
        this.container.innerHTML = html;
    }

    renderColumnHeaders(headers) {
        // Render multi-level column headers with proper colspan
        return headers.levels.map(level => `
            <tr class="header-row">
                <th class="corner-cell"></th>
                ${level.map(h => `
                    <th colspan="${h.colspan}" rowspan="${h.rowspan}"
                        class="cell-header ${h.is_abstract ? 'cell-header-abstract' : ''}">
                        ${h.text}
                    </th>
                `).join('')}
            </tr>
        `).join('');
    }

    renderBody(rowHeaders, rows) {
        return rows.map((row, rowIndex) => `
            <tr>
                ${this.renderRowHeader(rowHeaders[rowIndex])}
                ${row.cells.map(cell => this.renderCell(cell)).join('')}
            </tr>
        `).join('');
    }

    renderCell(cell) {
        if (!cell.cell_id) {
            return '<td class="cell-empty"></td>';
        }

        const classes = ['report-cell'];

        if (cell.is_shaded) {
            classes.push('cell-shaded');
        } else if (cell.is_executable) {
            classes.push('cell-executable');
        }

        const cachedValue = this.executedCells.get(cell.cell_id);
        if (cachedValue) {
            classes.push('cell-calculated');
        }

        return `
            <td class="${classes.join(' ')}"
                data-cell-id="${cell.cell_id}"
                data-datapoint-id="${cell.datapoint_id || ''}"
                data-executable="${cell.is_executable}">
                ${cachedValue || (cell.is_executable ? '' : '')}
            </td>
        `;
    }

    attachEventListeners() {
        // Cell click handler
        this.container.querySelectorAll('.cell-executable').forEach(cell => {
            cell.addEventListener('click', (e) => this.handleCellClick(e));
        });

        // Execute all button
        const executeAllBtn = document.getElementById('execute-all-btn');
        if (executeAllBtn) {
            executeAllBtn.addEventListener('click', () => this.executeAll());
        }
    }

    async handleCellClick(event) {
        const cell = event.target;
        const cellId = cell.dataset.cellId;
        const datapointId = cell.dataset.datapointId;

        if (!datapointId || cell.classList.contains('cell-loading')) {
            return;
        }

        // Show loading state
        cell.classList.add('cell-loading');
        cell.innerHTML = '<span class="spinner"></span>';

        try {
            const result = await this.executeCell(cellId);

            if (result.success) {
                cell.classList.remove('cell-loading');
                cell.classList.add('cell-calculated');
                cell.innerHTML = this.formatValue(result.result.value);
                this.executedCells.set(cellId, result.result.formatted_value);

                // Update execution log
                this.addToExecutionLog(cellId, result);
            } else {
                cell.classList.remove('cell-loading');
                cell.classList.add('cell-error');
                cell.title = result.error.message;
            }
        } catch (error) {
            cell.classList.remove('cell-loading');
            cell.classList.add('cell-error');
            cell.title = error.message;
        }
    }

    async executeCell(cellId) {
        const response = await fetch(`/api/report/cell/${cellId}/execute/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': this.getCsrfToken()
            },
            body: JSON.stringify({ force_refresh: false })
        });
        return response.json();
    }

    formatValue(value) {
        // Format numeric values with thousands separator
        const num = parseFloat(value);
        if (!isNaN(num)) {
            return num.toLocaleString();
        }
        return value;
    }

    async executeAll() {
        if (this.isExecutingAll) return;

        this.isExecutingAll = true;
        const executableCells = this.container.querySelectorAll('.cell-executable:not(.cell-calculated)');

        const progressBar = document.getElementById('progress-bar');
        let completed = 0;
        const total = executableCells.length;

        // Use SSE for batch execution
        const eventSource = new EventSource(`/api/report/${this.tableId}/execute-all/stream/`);

        eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);

            if (data.cell_id) {
                const cell = this.container.querySelector(`[data-cell-id="${data.cell_id}"]`);
                if (cell) {
                    cell.classList.remove('cell-loading', 'cell-executable');
                    cell.classList.add('cell-calculated');
                    cell.innerHTML = this.formatValue(data.result);
                    this.executedCells.set(data.cell_id, data.result);
                }
                completed++;
                progressBar.value = (completed / total) * 100;
            }
        };

        eventSource.addEventListener('complete', () => {
            eventSource.close();
            this.isExecutingAll = false;
        });

        eventSource.onerror = () => {
            eventSource.close();
            this.isExecutingAll = false;
        };
    }

    getCsrfToken() {
        return document.querySelector('[name=csrfmiddlewaretoken]')?.value || '';
    }
}
```

#### 7.2.2 Hierarchical Ordinate Tree Component

**Location:** `pybirdai/static/js/ordinate_tree.js`

```javascript
class OrdinateTreeEditor {
    constructor(containerId, axisOrientation) {
        this.container = document.getElementById(containerId);
        this.orientation = axisOrientation;
        this.selectedOrdinate = null;
        this.onSelectionChange = null;
        this.onStructureChange = null;

        this.init();
    }

    init() {
        this.container.innerHTML = `
            <div class="ordinate-tree-header">
                <span class="axis-label">${this.orientation === 'Y' ? 'Rows' : 'Columns'}</span>
                <button class="btn-add-ordinate" title="Add ordinate">+</button>
            </div>
            <div class="ordinate-tree-content" data-orientation="${this.orientation}">
            </div>
        `;

        this.treeContent = this.container.querySelector('.ordinate-tree-content');
        this.attachEvents();
    }

    loadOrdinates(ordinates) {
        this.ordinates = ordinates;
        this.renderTree();
    }

    renderTree() {
        this.treeContent.innerHTML = this.renderNodes(this.ordinates, 0);
        this.attachNodeEvents();
    }

    renderNodes(nodes, depth) {
        if (!nodes || nodes.length === 0) return '';

        return nodes.map(node => `
            <div class="tree-node"
                 data-ordinate-id="${node.ordinate_id}"
                 data-level="${depth}"
                 draggable="true">
                <div class="tree-node-content" style="padding-left: ${depth * 20}px">
                    <span class="expand-icon ${node.children?.length ? 'has-children' : 'no-children'}">
                        ${node.children?.length ? '▼' : '•'}
                    </span>
                    <span class="node-icon ${node.is_abstract_header ? 'abstract' : 'leaf'}">
                        ${node.is_abstract_header ? '📁' : '📄'}
                    </span>
                    <span class="node-name">${node.name}</span>
                    ${node.ordinate_items?.length ? `
                        <span class="node-variable">(${node.ordinate_items[0].variable_name})</span>
                    ` : ''}
                </div>
                <div class="tree-node-children">
                    ${this.renderNodes(node.children, depth + 1)}
                </div>
            </div>
        `).join('');
    }

    attachNodeEvents() {
        // Click to select
        this.treeContent.querySelectorAll('.tree-node-content').forEach(content => {
            content.addEventListener('click', (e) => {
                e.stopPropagation();
                const node = content.closest('.tree-node');
                this.selectNode(node.dataset.ordinateId);
            });
        });

        // Expand/collapse
        this.treeContent.querySelectorAll('.expand-icon.has-children').forEach(icon => {
            icon.addEventListener('click', (e) => {
                e.stopPropagation();
                const node = icon.closest('.tree-node');
                node.classList.toggle('collapsed');
                icon.textContent = node.classList.contains('collapsed') ? '▶' : '▼';
            });
        });

        // Drag and drop for reordering
        this.setupDragAndDrop();
    }

    selectNode(ordinateId) {
        // Remove previous selection
        this.treeContent.querySelectorAll('.selected').forEach(el => {
            el.classList.remove('selected');
        });

        // Add selection to new node
        const node = this.treeContent.querySelector(`[data-ordinate-id="${ordinateId}"]`);
        if (node) {
            node.querySelector('.tree-node-content').classList.add('selected');
            this.selectedOrdinate = ordinateId;

            if (this.onSelectionChange) {
                this.onSelectionChange(ordinateId);
            }
        }
    }

    setupDragAndDrop() {
        const nodes = this.treeContent.querySelectorAll('.tree-node');

        nodes.forEach(node => {
            node.addEventListener('dragstart', (e) => {
                e.dataTransfer.setData('text/plain', node.dataset.ordinateId);
                node.classList.add('dragging');
            });

            node.addEventListener('dragend', () => {
                node.classList.remove('dragging');
            });

            node.addEventListener('dragover', (e) => {
                e.preventDefault();
                node.classList.add('drag-over');
            });

            node.addEventListener('dragleave', () => {
                node.classList.remove('drag-over');
            });

            node.addEventListener('drop', (e) => {
                e.preventDefault();
                node.classList.remove('drag-over');

                const draggedId = e.dataTransfer.getData('text/plain');
                const targetId = node.dataset.ordinateId;

                if (draggedId !== targetId) {
                    this.handleReorder(draggedId, targetId);
                }
            });
        });
    }

    async handleReorder(draggedId, targetId) {
        // Emit event for parent to handle API call
        if (this.onStructureChange) {
            this.onStructureChange({
                type: 'reorder',
                draggedId,
                targetId
            });
        }
    }
}
```

---

## 8. Security Considerations

### 8.1 Authentication & Authorization

- All API endpoints require authentication
- User must have appropriate permissions to execute datapoints
- Audit logging for all cell executions

### 8.2 Rate Limiting

- Individual cell execution: 60 requests/minute per user
- Batch execution: 5 requests/minute per user
- Prevent DoS through expensive datapoint calculations

### 8.3 Input Validation

- Validate cell_id and table_id parameters
- Sanitize any user-provided filter parameters
- Validate that cells belong to accessible tables

### 8.4 Data Access Control

- Users can only execute datapoints for tables they have access to
- Lineage data respects same access controls
- Session isolation for cached results

---

## 9. Testing Strategy

### 9.1 Unit Tests

- Cell execution service with mocked datapoint execution
- Table rendering service with fixture data
- Ordinate tree building algorithms

### 9.2 Integration Tests

- End-to-end cell execution through API
- Table rendering with real database records
- Batch execution with cancellation

### 9.3 UI Tests

- Cell click execution flow
- Batch execution progress display
- Ordinate tree drag-and-drop

### 9.4 Performance Tests

- Render 100x100 cell table in < 1 second
- Execute 500 cells in batch within reasonable time
- Concurrent user load testing

---

## 10. Future Enhancements

### 10.1 Phase 2 Features

- **Comparison Mode:** Compare executed values against expected/previous values
- **Export to Excel:** Download executed report as Excel file with formatting
- **Report Scheduling:** Schedule batch execution and notification
- **Conditional Formatting:** Color cells based on value ranges

### 10.2 Phase 3 Features

- **What-If Analysis:** Modify input data and re-execute
- **Cross-Report Linking:** Click cell to see related cells in other reports
- **Collaborative Annotations:** Add notes to cells
- **Version History:** Track execution results over time

---

## 11. Appendix

### 11.1 FINREP F05_01 Example Structure

```
Table: F 05.01 - Loans and advances by product and by counterparty sector

Y-Axis (Rows):
├── [H] ASSETS
│   ├── Debt securities at amortised cost
│   ├── Debt securities at fair value through OCI
│   ├── Loans and advances
│   │   ├── On demand and short notice
│   │   ├── Credit card debt
│   │   ├── Trade receivables
│   │   ├── Finance leases
│   │   └── Other term loans
│   └── ...
└── [H] LIABILITIES
    └── ...

X-Axis (Columns):
├── [H] Breakdown by counterparty sector
│   ├── Central banks
│   ├── General governments
│   ├── Credit institutions
│   ├── Other financial corporations
│   ├── Non-financial corporations
│   └── Households

Cell Example:
- Row: "Debt securities at amortised cost" (R020)
- Column: "Central banks" (C010)
- Datapoint ID: F_05_01_REF_FINREP_3_0_152589_REF
- Filter: ACCNTNG_CLSSFCTN in ['2'], INSTTTNL_SCTR in ['S121']
- Aggregation: SUM(CRRYNG_AMNT)
```

### 11.2 Glossary

| Term | Definition |
|------|------------|
| AXIS | A dimension of a table (rows or columns) |
| ORDINATE | A header element within an axis |
| CELL | Intersection of a row and column ordinate |
| CELL_POSITION | Mapping of a cell to its row and column ordinates |
| COMBINATION | The filter/calculation definition for a cell |
| DATAPOINT | An executable cell calculation |
| FINREP | Financial Reporting framework |
| DPM | Data Point Model |
| BIRD | Banks' Integrated Reporting Dictionary |

### 11.3 URL Reference (New Endpoints)

```
# Template Editor (Enhanced)
GET  /api/table-amendment/{table_id}/hierarchical-structure/
PUT  /api/table-amendment/ordinate/{ordinate_id}/reparent/

# Interactive Report Viewer
GET  /api/report/templates/
GET  /api/report/{table_id}/render/
POST /api/report/cell/{cell_id}/execute/
POST /api/report/{table_id}/execute-all/
GET  /api/report/{table_id}/execute-all/stream/  (SSE)
GET  /api/report/cell/{cell_id}/lineage/

# Views
GET  /pybirdai/report/viewer/                     # Template selection
GET  /pybirdai/report/viewer/{table_id}/          # Interactive report view
```

---

**Document Revision History:**

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-26 | PyBIRD AI Team | Initial specification |

---

*End of Specification*
