# Git: Ignoring Local Changes to Tracked Files

## Overview

This document explains how to tell git to ignore local changes to specific tracked files using the `--assume-unchanged` flag. This is useful when you need to modify files locally (e.g., configuration files, temporary changes) but don't want those changes to appear in `git status` or be accidentally committed.

## Current Configuration

The following files are marked as assume-unchanged in this repository:

### Configuration Files
- `birds_nest/pybirdai/admin.py`
- `birds_nest/pybirdai/models/bird_data_model.py`

### Tmp Files (resources and results directories)
- `birds_nest/resources/dpm_metrics_configuration/tmp`
- `birds_nest/results/database_configuration_files/tmp`
- `birds_nest/results/generated_hierarchy_warnings/tmp`
- `birds_nest/results/generated_mapping_warnings/tmp`

Note: Additional tmp files may exist but are not tracked by git

## Usage

### Mark a file as assume-unchanged

To tell git to ignore changes to a file:

```bash
git update-index --assume-unchanged <file_path>
```

**Example:**
```bash
git update-index --assume-unchanged birds_nest/pybirdai/admin.py
```

### Mark all tmp files in resources and results

To mark all tmp files in the resources and results directories:

```bash
find birds_nest/resources birds_nest/results -type f -name "*tmp*" 2>/dev/null | while read file; do git update-index --assume-unchanged "$file"; done
```

### Remove assume-unchanged flag

To revert and make git track changes again:

```bash
git update-index --no-assume-unchanged <file_path>
```

**Example:**
```bash
git update-index --no-assume-unchanged birds_nest/pybirdai/admin.py
```

### List all assume-unchanged files

To see which files are currently marked as assume-unchanged:

```bash
git ls-files -v | grep '^h'
```

The files starting with 'h' (lowercase) are assumed unchanged.

## Important Notes

1. **Local only:** The assume-unchanged flag is local to your repository and is not shared with other developers
2. **Not for ignoring files:** This is different from `.gitignore` - the file is still tracked by git, but local changes are ignored
3. **Potential issues:** Be careful when pulling changes - if the remote version of an assume-unchanged file is updated, you may encounter merge conflicts
4. **Best practices:** Use this for temporary local modifications, not as a permanent solution

## Use Cases

- Local database configuration changes
- Temporary debugging modifications
- Environment-specific settings that shouldn't be committed
- Generated files that are tracked but frequently regenerated locally

## Alternative: Skip Worktree

For more persistent ignoring of tracked files (especially useful for files that shouldn't be modified locally):

```bash
git update-index --skip-worktree <file_path>
```

To undo:
```bash
git update-index --no-skip-worktree <file_path>
```

**Difference:** `--skip-worktree` is intended for files you want to keep in the repository but never modify locally, while `--assume-unchanged` is a performance optimization that tells git not to check for changes.
