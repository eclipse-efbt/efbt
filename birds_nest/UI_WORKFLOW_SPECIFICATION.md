# PyBIRD AI - 6-Task Workflow UI Specification

## Overview

This document specifies a redesigned user interface for PyBIRD AI that organizes the BIRD data processing pipeline into 6 ordered tasks, each with dedicated screens for Do, Review, and Compare operations. The new UI maintains backward compatibility with existing backend services while providing a more intuitive, step-by-step workflow.

## Current System Analysis

### Existing Backend Services
- **AutomodeConfigurationService**: Handles configuration and file fetching
- **Entry Points**: 22 modules in `pybirdai/entry_points/` for various processing steps
- **Process Steps**: Lower-level modules in `pybirdai/process_steps/`
- **Views**: 140+ endpoints handling current automode and step-by-step workflows

### Current Automode Flow
1. Configuration (data sources, GitHub repos, when to stop)
2. Resource fetching (technical exports, configuration files)
3. Database creation (requires manual server restart)
4. SMCubes rules creation
5. Python code generation
6. Full execution with testing

## New 6-Task Workflow Architecture

### Task Hierarchy
```
1. Resource Download
   ├── Do: Configure and fetch resources
   ├── Review: Validate downloaded files
   └── Compare: Compare with previous versions

2. Database Creation
   ├── Do: Create Django models and database
   ├── Review: Verify database schema
   └── Compare: Compare with reference schema

3. SMCubes Core Creation
   ├── Do: Import data model and create report content
   ├── Review: Validate imported data structures
   └── Compare: Compare with expected model

4. SMCubes Transformation Rules Creation
   ├── Do: Generate transformation rules and filters
   ├── Review: Validate transformation logic
   └── Compare: Compare with reference rules

5. Python Transformation Rules Creation
   ├── Do: Generate executable Python code
   ├── Review: Review generated code quality
   └── Compare: Compare with previous versions

6. Full Execution with Test Suite
   ├── Do: Run complete pipeline with tests
   ├── Review: Analyze test results and reports
   └── Compare: Compare outputs with baselines
```

## UI Design Specifications

### Main Dashboard
- **Task Progress Indicator**: Visual progress bar showing completed tasks (1-6)
- **Task Status Grid**: 6x3 grid showing status of each Do/Review/Compare operation
- **Quick Actions**: Jump to any available task or automode execution
- **System Health**: Database status, file system status, configuration status

### Task Status States
- ✅ **Completed**: Task finished successfully
- 🔄 **In Progress**: Task currently running
- ⏸️ **Paused**: Task paused (e.g., waiting for server restart)
- ❌ **Failed**: Task failed with errors
- ⏳ **Pending**: Task waiting for prerequisites
- 🔄 **Invalidated**: Task needs re-run due to upstream changes

### Navigation Structure
```
/workflow/
├── dashboard/                 # Main task overview
├── task1/                    # Resource Download
│   ├── do/                   # Configure and execute download
│   ├── review/               # Review downloaded files
│   └── compare/              # Compare versions
├── task2/                    # Database Creation
│   ├── do/                   # Create database (with restart handling)
│   ├── review/               # Review database schema
│   └── compare/              # Compare schema
├── task3/                    # SMCubes Core Creation
│   ├── do/                   # Import data model
│   ├── review/               # Review imported structures
│   └── compare/              # Compare models
├── task4/                    # SMCubes Transformation Rules
│   ├── do/                   # Generate rules
│   ├── review/               # Review transformation logic
│   └── compare/              # Compare rules
├── task5/                    # Python Transformation Rules
│   ├── do/                   # Generate Python code
│   ├── review/               # Review code quality
│   └── compare/              # Compare versions
├── task6/                    # Full Execution
│   ├── do/                   # Run tests and execution
│   ├── review/               # Review results
│   └── compare/              # Compare outputs
└── automode/                 # Enhanced automode interface
```

## Detailed Task Specifications

### Task 1: Resource Download

#### Do Screen
**Purpose**: Configure data sources and download required files
**Backend Integration**: `AutomodeConfigurationService.fetch_files_from_source()`

**Subtasks**:
1. **Configuration Setup**
   - Data model type selection (ELDM/EIL)
   - Technical export source (BIRD Website/GitHub/Manual)
   - Configuration files source (GitHub/Manual)
   - GitHub authentication (if required)

2. **Technical Export Download**
   - BIRD website metadata download
   - GitHub repository file fetching
   - Manual file upload interface

3. **Configuration Files Download**
   - Join configuration files
   - Extra variables files
   - LDM/IL model files

**UI Elements**:
- Configuration form (reusing existing `AutomodeConfigurationSessionForm`)
- Progress indicators for each download operation
- File validation status
- Download logs and error reporting

#### Review Screen
**Purpose**: Validate downloaded files and configuration
**Backend Integration**: File system validation, CSV structure checking

**Features**:
- File count summaries by category
- CSV file structure validation
- Data quality checks (completeness, format)
- Configuration review panel
- Missing files detection

#### Compare Screen
**Purpose**: Compare current download with previous versions
**Backend Integration**: File comparison utilities

**Features**:
- File diff viewer
- Metadata comparison
- Version history
- Change impact analysis

### Task 2: Database Creation

#### Do Screen
**Purpose**: Create Django models and database schema
**Backend Integration**: `RunAutomodeDatabaseSetup.run_automode_database_setup()`

**Subtasks**:
1. **Model Generation**
   - Generate Django models from LDM/EIL
   - Create migration files
   - Validate model relationships

2. **Database Setup**
   - Run database migrations
   - Create initial data structures
   - Handle server restart requirement

3. **Post-Restart Operations**
   - Continue setup after restart
   - Verify database integrity
   - Populate initial metadata

**UI Elements**:
- Model generation progress
- Migration status display
- Server restart handling interface
- Database connection status
- Error logging and recovery options

**Special Handling**:
- Clear restart instructions
- Automatic detection of restart completion
- Continuation workflow management

#### Review Screen
**Purpose**: Verify database schema and data integrity
**Backend Integration**: Database introspection, model validation

**Features**:
- Database schema browser
- Table relationship visualization
- Data integrity checks
- Migration history review

#### Compare Screen
**Purpose**: Compare database schema with reference
**Backend Integration**: Schema comparison utilities

**Features**:
- Schema diff viewer
- Table structure comparison
- Data migration impact analysis

### Task 3: SMCubes Core Creation

#### Do Screen
**Purpose**: Import data model and create report content structures
**Backend Integration**: Existing SMCubes creation entry points

**Subtasks**:
1. **Data Model Import**
   - Import LDM/EIL hierarchies
   - Process semantic integrations
   - Create cube structures

2. **Report Template Creation**
   - Generate report templates
   - Create cube mappings
   - Setup variable mappings

**UI Elements**:
- Import progress tracking
- Hierarchy tree visualization
- Error handling and retry mechanisms
- Validation status indicators

#### Review Screen
**Purpose**: Validate imported data structures
**Backend Integration**: Existing review endpoints in `report_views.py`

**Features**:
- Hierarchy browser
- Semantic integration review
- Cube structure validation
- Missing elements detection

#### Compare Screen
**Purpose**: Compare imported structures with reference
**Backend Integration**: Comparison utilities

**Features**:
- Structure diff viewer
- Mapping comparison
- Hierarchy change analysis

### Task 4: SMCubes Transformation Rules Creation

#### Do Screen
**Purpose**: Generate transformation rules and filters
**Backend Integration**: `create_filters.py`, `create_joins_metadata.py`

**Subtasks**:
1. **Filter Creation**
   - Generate SMCubes filters
   - Create filter metadata
   - Validate filter logic

2. **Join Rules Creation**
   - Generate join metadata
   - Create executable joins
   - Validate join relationships

#### Review Screen
**Purpose**: Validate transformation logic
**Backend Integration**: Filter and join review utilities

**Features**:
- Filter rule browser
- Join relationship visualization
- Logic validation checks
- Performance analysis

#### Compare Screen
**Purpose**: Compare transformation rules
**Backend Integration**: Rule comparison utilities

**Features**:
- Rule diff viewer
- Logic change analysis
- Performance comparison

### Task 5: Python Transformation Rules Creation

#### Do Screen
**Purpose**: Generate executable Python code
**Backend Integration**: Python code generation entry points

**Subtasks**:
1. **Code Generation**
   - Generate Python filters
   - Create executable transformations
   - Generate test cases

2. **Code Validation**
   - Syntax checking
   - Logic validation
   - Performance optimization

#### Review Screen
**Purpose**: Review generated code quality
**Backend Integration**: Code analysis utilities

**Features**:
- Code browser with syntax highlighting
- Quality metrics display
- Test coverage analysis
- Performance benchmarks

#### Compare Screen
**Purpose**: Compare code versions
**Backend Integration**: Code diff utilities

**Features**:
- Side-by-side code comparison
- Logic change highlighting
- Performance impact analysis

### Task 6: Full Execution with Test Suite

#### Do Screen
**Purpose**: Run complete pipeline with comprehensive testing
**Backend Integration**: `execute_datapoint.py`, test execution utilities

**Subtasks**:
1. **Pipeline Execution**
   - Run data processing pipeline
   - Execute all transformations
   - Generate reports

2. **Test Suite Execution**
   - Run automated tests
   - Validate outputs
   - Generate test reports

#### Review Screen
**Purpose**: Analyze test results and reports
**Backend Integration**: Test result analysis, report viewers

**Features**:
- Test result dashboard
- Report visualization
- Error analysis
- Performance metrics

#### Compare Screen
**Purpose**: Compare outputs with baselines
**Backend Integration**: Output comparison utilities

**Features**:
- Result diff viewer
- Performance comparison
- Quality metrics comparison
- Regression analysis

## Enhanced Automode Interface

### Configuration Screen
**Purpose**: Configure automated execution up to specified task
**Backend Integration**: Enhanced `AutomodeConfigurationService`

**Features**:
- Task selection (execute tasks 1-N)
- Configuration inheritance from manual runs
- Execution strategy selection
- Scheduling options

### Execution Screen
**Purpose**: Monitor automated execution progress
**Backend Integration**: Real-time progress tracking

**Features**:
- Multi-task progress visualization
- Real-time logging
- Pause/resume capabilities
- Error handling and recovery

### Results Screen
**Purpose**: Review automode execution results
**Backend Integration**: Result aggregation services

**Features**:
- Execution summary
- Jump to specific task review screens
- Error analysis
- Next steps recommendations

## Task Dependency Management

### Dependency Rules
1. **Sequential Dependencies**: Each task depends on successful completion of previous tasks
2. **Invalidation Cascade**: Re-running an earlier task invalidates all later tasks
3. **Partial Recovery**: Failed tasks can be resumed without restarting entire pipeline

### Status Management
- **Task State Tracking**: Persistent storage of task completion states
- **Dependency Validation**: Automatic checking of prerequisite completion
- **Invalidation Handling**: Clear notification when tasks need re-execution

### Database Schema Extensions
```sql
-- Task execution tracking
CREATE TABLE workflow_task_execution (
    id SERIAL PRIMARY KEY,
    task_number INTEGER NOT NULL,
    subtask_name VARCHAR(100),
    operation_type VARCHAR(20) -- 'do', 'review', 'compare'
    status VARCHAR(20), -- 'pending', 'running', 'completed', 'failed', 'invalidated'
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    execution_data JSON,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Task dependencies
CREATE TABLE workflow_task_dependencies (
    id SERIAL PRIMARY KEY,
    task_number INTEGER NOT NULL,
    depends_on_task INTEGER NOT NULL,
    dependency_type VARCHAR(50) -- 'sequential', 'optional', 'conditional'
);
```

## Implementation Strategy

### Phase 1: Core Infrastructure
1. Create new workflow views and URLs
2. Implement task state management
3. Create base templates for Do/Review/Compare screens
4. Integrate with existing backend services

### Phase 2: Task Implementation
1. Implement Task 1 (Resource Download) screens
2. Implement Task 2 (Database Creation) with restart handling
3. Implement remaining tasks sequentially
4. Add comprehensive error handling

### Phase 3: Enhanced Features
1. Implement comparison utilities
2. Add advanced automode features
3. Create task dependency validation
4. Add performance monitoring

### Phase 4: Polish and Testing
1. UI/UX refinements
2. Comprehensive testing
3. Documentation updates
4. User training materials

## Technical Requirements

### Backend Changes
- New view classes for workflow management
- Task state persistence layer
- Enhanced error handling and recovery
- Real-time progress tracking APIs

### Frontend Changes
- New template hierarchy for workflow screens
- JavaScript for progress tracking and real-time updates
- Enhanced navigation and breadcrumb system
- Responsive design for different screen sizes

### Integration Points
- Maintain compatibility with existing entry points
- Preserve existing automode functionality
- Support migration from old UI to new workflow
- Backward compatibility for existing configurations

## Success Criteria

### User Experience
- Clear understanding of current position in workflow
- Easy navigation between tasks and operations
- Comprehensive error reporting and recovery guidance
- Intuitive progress tracking and status visualization

### System Reliability
- Robust error handling at each task level
- Reliable restart and recovery mechanisms
- Comprehensive logging and debugging capabilities
- Performance monitoring and optimization

### Maintainability
- Clean separation between UI and business logic
- Modular task implementation
- Comprehensive test coverage
- Clear documentation and code organization

## Conclusion

This specification provides a comprehensive roadmap for transforming PyBIRD AI into a more user-friendly, task-oriented workflow system while maintaining all existing functionality and backend services. The new UI will provide better visibility into the complex BIRD data processing pipeline and make the system more accessible to users at different technical levels.