# Branch Improvements: `feature-improve-clone-mode-fix`

Code review findings and proposed fixes.

---

## Phase 1: Critical Fixes

| # | Task | File(s) | Details |
|---|------|---------|---------|
| 1a | Replace `date_parser.parse()` | `pybirdai/utils/clone_mode/process_metadata.py` | Use `datetime.fromisoformat(s.replace('Z', '+00:00'))` instead of dateutil |
| 1b | Replace `relativedelta` | `pybirdai/process_steps/derivation_generation/generate_derivation_from_csv.py` | Add custom `add_months()` helper using `calendar.monthrange()` |
| 2 | Fix SQL injection | `pybirdai/utils/clone_mode/import_from_metadata_export.py` | Replace f-string SQL with parameterized queries or Django ORM |
| 3 | Add tests | `pybirdai/tests/clone_mode/` | Integration tests for `process_metadata.py` core functions |

### 1a. Replace dateutil parser

**Current code:**
```python
from dateutil import parser as date_parser
completed_at = date_parser.parse(completed_at_str)
```

**Replacement:**
```python
from datetime import datetime
completed_at = datetime.fromisoformat(completed_at_str.replace('Z', '+00:00'))
```

### 1b. Replace relativedelta

**Current code:**
```python
from dateutil.relativedelta import relativedelta
result = date_field - relativedelta(months=3)
```

**Replacement:**
```python
import calendar

def add_months(date, months):
    """Add months to a date, handling overflow correctly."""
    month = date.month - 1 + months
    year = date.year + month // 12
    month = month % 12 + 1
    day = min(date.day, calendar.monthrange(year, month)[1])
    return date.replace(year=year, month=month, day=day)

result = add_months(date_field, -3)
```

### 2. SQL Injection Fix

**Current code (vulnerable):**
```python
cursor.execute(f"DELETE FROM {table_name};")
cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
```

**Fix:** Use Django ORM or validate table names against a whitelist.

---

## Phase 2: High Priority Fixes

| # | Task | File(s) | Details |
|---|------|---------|---------|
| 4 | Replace `print()` with logging | `pybirdai/utils/clone_mode/export_with_ids.py` | Use `logger.info()` instead of `print()` |
| 5 | Improve error handling | `pybirdai/utils/clone_mode/process_metadata.py` | Add specific exception handling instead of generic `except Exception` |
| 6 | Add transaction isolation | `pybirdai/utils/clone_mode/process_metadata.py` | Wrap workflow state restoration in `transaction.atomic()` |
| 7 | Consolidate duplicate code | `pybirdai/management/commands/load_clone_state.py`, `save_clone_state.py` | Move shared GitHub download logic to `GitHubService` |

---

## Phase 3: Medium Priority Improvements

| # | Task | File(s) | Details |
|---|------|---------|---------|
| 8 | Database portability | `pybirdai/utils/clone_mode/process_metadata.py` | Replace `sqlite_master` queries with Django ORM |
| 9 | Extract constants | Multiple files | Replace magic numbers like `[:100]`, `chunk_size=8192` with named constants |
| 10 | Refactor long functions | `pybirdai/utils/clone_mode/process_metadata.py` | Break `restore_workflow_states()` (185 lines) into smaller functions |

---

## Issues Summary

### Critical (Fix Before Merge)
- Missing `python-dateutil` dependency → **Replace with built-in datetime**
- SQL injection vulnerability
- Missing test coverage for core clone mode functionality

### High Priority
- Print statements instead of logging
- Generic exception handling swallows errors silently
- Race condition in workflow state restoration
- Duplicate GitHub download code

### Medium Priority
- SQLite-specific queries limit database portability
- Magic numbers reduce code readability
- Overly long functions (>100 lines)

### Low Priority
- Inconsistent import styles
- Missing type hints
- Incomplete docstrings

---

## Positive Observations

- Comprehensive docstrings in many new files
- Good error handling with meaningful messages in management commands
- JSON schema documentation in `save_clone_state.py`
- GitHub service consolidation reduces duplication
- Well-structured test files
