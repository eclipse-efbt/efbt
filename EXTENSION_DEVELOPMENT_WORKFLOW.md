# Extension Development Workflow Demo

This document demonstrates the three main commands for managing PyBIRD AI extensions: creating, packaging, and fetching extensions.

## Prerequisites

Make sure you're in the Django project directory:
```bash
cd birds_nest
```

## 1. Creating a New Extension

Creates a new extension structure with templates and basic configuration.

**With uv (recommended):**
```bash
uv run python manage.py create_extension --name test_analytics
```

**Alternative with standard Python:**
```bash
python manage.py create_extension --name test_analytics
```

**What this does:**
- Creates extension directory structure in `extensions/`
- Generates template files (views, models, URLs)
- Sets up basic configuration files
- Prepares the extension for development

## 2. Packaging an Extension

Packages your extension into a distributable format and optionally publishes to GitHub.

**With uv (recommended):**
```bash
uv run python manage.py package_extension --name test_analytics --token <GITHUB_TOKEN> --repo-name bird-test-analytics --private --github-user benjamin-arfa
```

**Alternative with standard Python:**
```bash
python manage.py package_extension --name test_analytics --token <GITHUB_TOKEN> --repo-name bird-test-analytics --private --github-user benjamin-arfa
```

**Parameters:**
- `--name`: Name of the extension to package
- `--token`: GitHub personal access token for repository creation/push
- `--repo-name`: Name for the GitHub repository
- `--private`: Creates a private repository (omit for public)
- `--github-user`: GitHub username or organization

**What this does:**
- Analyzes extension dependencies
- Generates packaging files (pyproject.toml, README, etc.)
- Creates installation scripts
- Optionally creates and pushes to GitHub repository

## 3. Fetching an Extension

Downloads and installs an extension from a repository.

**With uv (recommended):**
```bash
uv run python manage.py fetch_extension --repo <repo_url> --name test_analytics --token <GITHUB_TOKEN> --uv --force
```

**Alternative with standard Python:**
```bash
python manage.py fetch_extension --repo <repo_url> --name test_analytics --token <GITHUB_TOKEN> --force
```

**Parameters:**
- `--repo`: Repository URL (GitHub, GitLab, etc.)
- `--name`: Local name for the extension
- `--token`: GitHub token (if accessing private repositories)
- `--uv`: Use uv for dependency management (recommended)
- `--force`: Overwrite existing extension if it exists

**What this does:**
- Clones the repository
- Installs extension dependencies
- Integrates the extension into your Django project
- Updates project configuration files

## Notes

- Replace `<GITHUB_TOKEN>` with your actual GitHub personal access token
- Replace `<repo_url>` with the actual repository URL
- The `--uv` flag is recommended for better dependency management
- Extensions are stored in the `extensions/` directory
