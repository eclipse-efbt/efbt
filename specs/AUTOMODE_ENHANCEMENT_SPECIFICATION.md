# Automode Configuration Enhancement Specification

## Overview

This specification describes improvements to the PyBIRD AI automode functionality to eliminate manual configuration steps and provide users with a streamlined, configurable data source selection interface.

## Current State Analysis

### Current Manual Steps Required
1. **Context Configuration**: Manual editing of `context.py` to set `ldm_or_il` flag + server restart
2. **ELDM/EIL Files**: Manual SQLDeveloper export and file placement in `resources/ldm/` or `resources/il/`
3. **Technical Export Files**: Manual download from BIRD website to `resources/technical_export/`
4. **Configuration Files**: Manual setup of joins configuration in `resources/joins_configuration/`
5. **Extra Variables**: Manual placement of variable files in `resources/extra_variables/` (LDM only)

### Current Data Source Options
- **BIRD Website** (automated): ECB API download of metadata
- **GitHub Repository** (automated): FreeBIRD community repository files
- **Manual Upload** (web interface): Individual file uploads
- **Manual File Copy** (filesystem): Direct file placement

## Proposed Enhancement

### Goal
Transform automode from a semi-manual process into a fully configurable, automated workflow where users select their preferred data sources through a web interface.

## Functional Requirements

### 1. Data Source Configuration Interface

#### 1.1 Technical Export Data Source Selection
**Requirement**: Users must be able to choose the source for technical export files.

**Implementation**:
- Radio button selection:
  - `BIRD Website` (current ECB API endpoint)
  - `GitHub Repository` (configurable repository URL)
- When GitHub is selected, provide text input for repository URL
- Default to FreeBIRD community repository: `https://github.com/regcommunity/FreeBIRD`

#### 1.2 Configuration Files Data Source Selection
**Requirement**: Users must be able to choose the source for configuration files (joins configuration, extra variables).

**Implementation**:
- Radio button selection:
  - `Manual Upload` (existing file upload interface)
  - `GitHub Repository` (configurable repository URL)
- When GitHub is selected, provide text input for repository URL
- Support fetching:
  - Joins configuration files (`joins_configuration/`)
  - Extra variables files (`extra_variables/`)
  - LDM export files (`ldm/`)

#### 1.3 Data Model Selection
**Requirement**: Users must be able to select ELDM vs EIL without manual code editing.

**Implementation**:
- Radio button selection:
  - `ELDM` (Logical Data Model)
  - `EIL` (Input Layer)
- Remove requirement for manual `context.py` editing
- No server restart required

### 2. Configuration Persistence

#### 2.1 Configuration Storage
**Requirement**: Store user configuration choices for reuse.

**Implementation**:
- Create Django model for automode configuration
- Fields:
  - `data_model_type` (ELDM/EIL)
  - `technical_export_source` (BIRD_WEBSITE/GITHUB)
  - `technical_export_github_url`
  - `config_files_source` (MANUAL/GITHUB)
  - `config_files_github_url`
  - `created_at`, `updated_at`

#### 2.2 Configuration Validation
**Requirement**: Validate user inputs before processing.

**Implementation**:
- GitHub URL validation (valid repository format)
- Repository accessibility checks
- Required file existence verification

### 3. Automated File Fetching

#### 3.1 Enhanced GitHub Integration
**Requirement**: Fetch files from user-specified GitHub repositories.

**Implementation**:
- Extend existing `GitHubFileFetcher` class
- Support configurable repository URLs
- Fetch specific file types based on configuration:
  - Technical export files → `resources/technical_export/`
  - Joins configuration → `resources/joins_configuration/`
  - Extra variables → `resources/extra_variables/`
  - LDM exports → `resources/ldm/`

#### 3.2 Error Handling
**Requirement**: Graceful handling of network and repository access errors.

**Implementation**:
- Repository access validation
- File existence checks
- Fallback to manual upload on failure
- Clear error messaging to users

### 4. User Interface Updates

#### 4.1 Automode Configuration Page
**Requirement**: Replace manual instruction text with interactive configuration form.

**Implementation**:
- Remove paragraphs describing manual steps (lines 21-28 in automode.html)
- Add configuration form before task grid
- Form sections:
  1. Data Model Selection (ELDM/EIL)
  2. Technical Export Source (BIRD Website/GitHub)
  3. Configuration Files Source (Manual/GitHub)
- Real-time form validation
- Configuration preview/summary

#### 4.2 Progressive Disclosure
**Requirement**: Show relevant options based on user selections.

**Implementation**:
- Show GitHub URL input only when GitHub option selected
- Display file type mappings for selected repositories
- Show progress indicators during file fetching

### 5. Workflow Integration

#### 5.1 Automated Setup Flow
**Requirement**: Execute complete setup based on user configuration.

**Implementation**:
1. User selects configuration options
2. System validates choices
3. Automated file fetching based on selections
4. Context configuration without manual editing
5. Database setup execution
6. Transformation creation

#### 5.2 Progress Tracking
**Requirement**: Provide visibility into automated setup progress.

**Implementation**:
- Progress bar for multi-step operations
- Status messages for each setup phase
- Error reporting with actionable next steps

## Technical Implementation

### 1. New Django Components

#### 1.1 Models
```python
class AutomodeConfiguration(models.Model):
    data_model_type = models.CharField(choices=[('ELDM', 'ELDM'), ('EIL', 'EIL')])
    technical_export_source = models.CharField(choices=[('BIRD_WEBSITE', 'BIRD Website'), ('GITHUB', 'GitHub')])
    technical_export_github_url = models.URLField(blank=True)
    config_files_source = models.CharField(choices=[('MANUAL', 'Manual Upload'), ('GITHUB', 'GitHub')])
    config_files_github_url = models.URLField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

#### 1.2 Forms
```python
class AutomodeConfigurationForm(forms.ModelForm):
    # Form validation and rendering logic
```

#### 1.3 Views
- `AutomodeConfigurationView` - Configuration form handling
- `AutomodeExecutionView` - Automated setup execution
- Enhanced existing automode views

### 2. Enhanced Services

#### 2.1 Configuration Service
```python
class AutomodeConfigurationService:
    def apply_configuration(self, config: AutomodeConfiguration)
    def validate_github_repository(self, url: str)
    def fetch_files_from_source(self, config: AutomodeConfiguration)
```

#### 2.2 Enhanced GitHub Fetcher
```python
class ConfigurableGitHubFileFetcher(GitHubFileFetcher):
    def __init__(self, repository_url: str)
    def fetch_configuration_files(self, target_directory: str)
    def fetch_technical_exports(self, target_directory: str)
```

### 3. Updated Templates
- Enhanced `automode.html` with configuration form
- New configuration form partial templates
- Progress tracking templates

## Migration Strategy

### Phase 1: Backend Infrastructure
1. Create new Django models and migrations
2. Implement configuration services
3. Enhance GitHub fetching capabilities

### Phase 2: User Interface
1. Update automode template with configuration form
2. Add JavaScript for progressive disclosure
3. Implement progress tracking

### Phase 3: Integration
1. Connect configuration to existing automode workflow
2. Update context management to use configuration
3. Remove manual configuration requirements

### Phase 4: Testing & Documentation
1. Update user documentation
2. Add automated tests for configuration scenarios
3. User acceptance testing

## Benefits

1. **Elimination of Manual Steps**: No more code editing or server restarts
2. **Flexible Data Sources**: Support for custom GitHub repositories
3. **Improved User Experience**: Web-based configuration vs. manual file management
4. **Reduced Documentation Dependency**: Self-contained configuration process
5. **Enhanced Automation**: True "automode" functionality
6. **Configuration Persistence**: Reusable settings for repeated operations

## Success Criteria

1. Users can complete automode setup without manual file operations
2. Configuration choices are persisted and reusable
3. GitHub repository integration works with custom URLs
4. Error handling provides clear guidance for resolution
5. Setup time reduced by >50% compared to manual process



## Dependencies

- Existing `GitHubFileFetcher` class
- Existing `BirdEcbWebsiteClient` class
- Django forms and models framework
- JavaScript for progressive UI disclosure

## Risks & Mitigation

1. **GitHub API Rate Limits**: Implement caching and error handling
2. **Repository Access Issues**: Provide clear error messages and fallback options
3. **User Configuration Errors**: Comprehensive validation and preview functionality
4. **Breaking Changes**: Maintain backward compatibility during transition