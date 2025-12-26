# DPM Dual Source Implementation Plan

## Overview
Add two starting points to the DPM workflow:
1. **EBA Source** - Current 6-step flow (download from EBA website)
2. **GitHub Source** - New 4-step flow (import from regcommunity packages like `regcommunity/FreeBIRD_IL_66_C07`)

## Completed Tasks

### 1. Model Updates (`pybirdai/models/workflow_model.py`)
- Added `EBA_STEP_CHOICES` (6 steps) and `GITHUB_STEP_CHOICES` (4 steps)
- Added `SOURCE_TYPE_CHOICES` ('eba', 'github')
- Added new fields to `DPMProcessExecution`:
  - `source_type` - CharField to track data source
  - `github_package_url` - URLField for GitHub repo URL
  - `github_branch` - CharField for branch name
  - `github_step_statuses` - JSONField for 4-step progress tracking
- Added helper methods:
  - `get_step_choices()` - Returns step choices based on source type
  - `get_step_name_for_source()` - Get step name for given step number
  - `get_total_steps()` - Returns 4 or 6 based on source type
  - `get_or_create_for_github()` - Factory method for GitHub executions

### 2. URL Routes (`pybirdai/urls.py`)
Added new routes after line 455:
```python
path("workflow/dpm/github/execute/<int:step_number>/", ...)
path("workflow/dpm/github/status/", ...)
path("workflow/dpm/github/review/<int:step_number>/", ...)
path("workflow/dpm/github/configure/", ...)
path("workflow/dpm/github/validate-package/", ...)
```

### 3. GitHub Execution Views (`pybirdai/views/workflow/dpm/github_execution.py`)
Created new file with:
- `execute_github_dpm_step()` - Main execution handler for 4-step flow
- `_execute_github_step1_import()` - Import data from GitHub package
- `_execute_github_step2_structure_links()` - Create joins metadata
- `_execute_github_step3_generate_code()` - Generate Python code
- `_execute_github_step4_run_tests()` - Run test suite
- `get_github_dpm_status()` - Get workflow status
- `workflow_github_dpm_review()` - Review page for steps
- `configure_github_dpm_source()` - Configure GitHub source
- `validate_github_dpm_package()` - Validate package before import
- `get_github_dpm_task_grid()` - Build task grid for 4 steps

### 4. Module Exports Updated
- Updated `pybirdai/views/workflow/dpm/__init__.py`
- Updated `pybirdai/views/workflow/__init__.py`

### 5. DPM Dashboard Template Started (`pybirdai/templates/pybirdai/workflow/dpm_workflow.html`)
- Added subtab buttons for EBA Source and GitHub Source
- Started wrapping EBA content in `dpm-source-content` div

---

## Remaining Tasks

### 4. Complete DPM Dashboard Template Updates

**File:** `pybirdai/templates/pybirdai/workflow/dpm_workflow.html`

**Changes needed:**
1. Close the EBA source content div after the existing DPM Task Grid
2. Add GitHub Source content panel with:
   - GitHub package configuration section (URL input, branch selector)
   - 4-step task grid for GitHub workflow
   - Configure button to open GitHub config modal

**Template structure:**
```html
<div class="dpm-source-tabs">
    <button class="dpm-source-tab active" data-source="eba">EBA Source</button>
    <button class="dpm-source-tab" data-source="github">GitHub Source</button>
</div>

<!-- EBA Source Content (existing, wrapped) -->
<div class="dpm-source-content active" id="dpm-eba-content">
    <!-- Framework Selection Panel -->
    <!-- DPM Task Grid (6 steps) -->
</div>

<!-- GitHub Source Content (new) -->
<div class="dpm-source-content" id="dpm-github-content">
    <!-- GitHub Package Configuration -->
    <div class="github-config-panel">
        <h4>GitHub Package Source</h4>
        <div class="config-row">
            <label>Repository URL:</label>
            <input type="text" id="github-dpm-url" placeholder="https://github.com/regcommunity/FreeBIRD_IL_66_C07">
        </div>
        <div class="config-row">
            <label>Branch:</label>
            <input type="text" id="github-dpm-branch" value="main">
        </div>
        <button onclick="openGitHubDPMConfigModal()">Configure</button>
    </div>

    <!-- GitHub DPM Task Grid (4 steps) -->
    <div class="task-grid github-dpm-grid">
        <!-- Step headers -->
        <div class="task-card"><h3>Step</h3></div>
        <div class="task-card"><h3>Do</h3></div>
        <div class="task-card"><h3>Review</h3></div>

        <!-- Step 1: Import Data -->
        <!-- Step 2: Generate Structure Links -->
        <!-- Step 3: Generate Executable Code -->
        <!-- Step 4: Run Tests -->
    </div>
</div>
```

### 5. Create GitHub Source Step Templates

**Directory:** `pybirdai/templates/pybirdai/workflow/dpm_workflow/github_source/`

**Files to create:**
1. `review.html` - Generic review template for GitHub DPM steps
2. `step_1_import.html` - Import data step review
3. `step_2_structure_links.html` - Structure links step review
4. `step_3_generate_code.html` - Code generation step review
5. `step_4_run_tests.html` - Test execution step review

**Template content for `review.html`:**
```html
{% extends "pybirdai/workflow/base.html" %}
{% block workflow_content %}
<div class="review-container">
    <h2>GitHub DPM Step {{ step_number }}: {{ step_name }}</h2>

    <div class="step-status">
        {% if execution %}
            <p>Status: <span class="status-{{ execution.status }}">{{ execution.status }}</span></p>
            {% if execution.error_message %}
                <div class="error-message">{{ execution.error_message }}</div>
            {% endif %}
            {% if execution.execution_data %}
                <div class="execution-details">
                    <h4>Execution Details</h4>
                    <pre>{{ execution.execution_data|json_script:"exec-data" }}</pre>
                </div>
            {% endif %}
        {% else %}
            <p>This step has not been executed yet.</p>
        {% endif %}
    </div>

    <div class="actions">
        <a href="{% url 'pybirdai:workflow_dashboard' %}" class="btn">Back to Dashboard</a>
    </div>
</div>
{% endblock %}
```

### 6. Create GitHub Configuration Modal

**File:** `pybirdai/templates/pybirdai/workflow/modals/github_dpm_config_modal.html`

**Content:**
```html
<!-- GitHub DPM Configuration Modal -->
<div id="github-dpm-config-modal" class="modal" style="display: none;">
    <div class="modal-overlay" onclick="closeGitHubDPMConfigModal()"></div>
    <div class="modal-content">
        <div class="modal-header">
            <h3>Configure GitHub DPM Source</h3>
            <button class="close-btn" onclick="closeGitHubDPMConfigModal()">&times;</button>
        </div>
        <div class="modal-body">
            <div class="form-group">
                <label for="github-dpm-repo-url">GitHub Repository URL</label>
                <input type="text" id="github-dpm-repo-url"
                       placeholder="https://github.com/regcommunity/FreeBIRD_IL_66_C07"
                       class="form-control">
                <small>Enter a regcommunity package URL (e.g., FreeBIRD_IL_66_C07)</small>
            </div>

            <div class="form-group">
                <label for="github-dpm-branch">Branch</label>
                <input type="text" id="github-dpm-branch-input" value="main" class="form-control">
            </div>

            <div class="form-group">
                <label for="github-dpm-token">GitHub Token (optional)</label>
                <input type="password" id="github-dpm-token"
                       placeholder="ghp_xxxx (for private repos)"
                       class="form-control">
                <small>Only required for private repositories</small>
            </div>

            <div id="github-dpm-validation-result" style="display: none;">
                <!-- Validation result will be shown here -->
            </div>
        </div>
        <div class="modal-footer">
            <button class="btn btn-secondary" onclick="closeGitHubDPMConfigModal()">Cancel</button>
            <button class="btn btn-primary" onclick="validateGitHubDPMPackage()">Validate</button>
            <button class="btn btn-success" onclick="saveGitHubDPMConfig()" disabled id="save-github-dpm-config-btn">
                Save Configuration
            </button>
        </div>
    </div>
</div>
```

**Include in dashboard.html:**
```html
{% include 'pybirdai/workflow/modals/github_dpm_config_modal.html' %}
```

### 7. Update CSS for Subtabs Styling

**File:** `pybirdai/static/pybirdai/css/dashboard/dashboard.css`

**Add styles:**
```css
/* DPM Source Subtabs */
.dpm-source-tabs {
    display: flex;
    gap: 8px;
    margin-bottom: 20px;
    padding: 4px;
    background: #f1f5f9;
    border-radius: 10px;
}

.dpm-source-tab {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    padding: 12px 20px;
    border: none;
    background: transparent;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 500;
    color: #64748b;
    cursor: pointer;
    transition: all 0.2s ease;
}

.dpm-source-tab:hover {
    background: rgba(255, 255, 255, 0.5);
    color: #334155;
}

.dpm-source-tab.active {
    background: #ffffff;
    color: #1e40af;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.dpm-source-tab .tab-icon {
    font-size: 16px;
}

.dpm-source-tab .tab-badge {
    font-size: 11px;
    padding: 2px 8px;
    background: #e2e8f0;
    border-radius: 12px;
    color: #475569;
}

.dpm-source-tab.active .tab-badge {
    background: #dbeafe;
    color: #1e40af;
}

/* DPM Source Content */
.dpm-source-content {
    display: none;
}

.dpm-source-content.active {
    display: block;
    animation: fadeIn 0.3s ease;
}

/* GitHub Config Panel */
.github-config-panel {
    background: #ffffff;
    padding: 24px;
    border-radius: 12px;
    margin-bottom: 20px;
    border: 1px solid #e5e7eb;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
}

.github-config-panel h4 {
    margin: 0 0 16px 0;
    font-size: 17px;
    font-weight: 600;
    color: #111827;
}

.github-config-panel .config-row {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 12px;
}

.github-config-panel .config-row label {
    min-width: 120px;
    font-weight: 500;
    color: #374151;
}

.github-config-panel .config-row input {
    flex: 1;
    padding: 10px 14px;
    border: 1.5px solid #d1d5db;
    border-radius: 8px;
    font-size: 14px;
}

.github-config-panel .config-row input:focus {
    outline: none;
    border-color: #2563eb;
    box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1);
}

/* GitHub DPM Task Grid */
.github-dpm-grid {
    grid-template-columns: 2fr 1fr 1fr;
}
```

### 8. Update JavaScript for Tab Switching and GitHub Flow

**File:** `pybirdai/static/pybirdai/js/dashboard/workflow-execution.js`

**Add functions:**
```javascript
// Switch DPM source tab
function switchDPMSourceTab(source) {
    // Update tab buttons
    document.querySelectorAll('.dpm-source-tab').forEach(tab => {
        tab.classList.toggle('active', tab.dataset.source === source);
    });

    // Update content panels
    document.querySelectorAll('.dpm-source-content').forEach(content => {
        content.classList.toggle('active', content.dataset.source === source);
    });

    // Save preference to localStorage
    localStorage.setItem('dpm_source_tab', source);
}

// Execute GitHub DPM step
function executeGitHubDPMStep(stepNumber) {
    console.log('executeGitHubDPMStep() called for step:', stepNumber);

    const btn = document.getElementById(`github-dpm-step-${stepNumber}-btn`);
    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]')?.value;

    if (!csrfToken) {
        alert('Error: CSRF token not found! Please refresh the page.');
        return;
    }

    // Get GitHub configuration
    const githubUrl = document.getElementById('github-dpm-url')?.value || '';
    const githubBranch = document.getElementById('github-dpm-branch')?.value || 'main';
    const githubToken = document.getElementById('github-dpm-token')?.value || '';

    if (!githubUrl && stepNumber === 1) {
        alert('Please configure a GitHub package URL first.');
        openGitHubDPMConfigModal();
        return;
    }

    // Update button state
    if (btn) {
        btn.disabled = true;
        btn.textContent = '⏳ Running...';
        btn.classList.remove('pending', 'completed', 'failed');
        btn.classList.add('running');
    }

    // Create form data
    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', csrfToken);
    formData.append('github_url', githubUrl);
    formData.append('github_branch', githubBranch);
    if (githubToken) {
        formData.append('github_token', githubToken);
    }

    // Make AJAX request
    fetch(`/pybirdai/workflow/dpm/github/execute/${stepNumber}/`, {
        method: 'POST',
        body: formData,
        headers: {
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => response.json())
    .then(data => {
        console.log('GitHub DPM step response:', data);

        if (data.success && data.status === 'completed') {
            if (btn) {
                btn.textContent = '✓ Done';
                btn.classList.remove('running');
                btn.classList.add('completed');
            }
            alert(`GitHub DPM Step ${stepNumber} completed successfully!`);
            setTimeout(() => location.reload(), 1500);
        } else {
            if (btn) {
                btn.textContent = '✗ Failed';
                btn.classList.remove('running');
                btn.classList.add('failed');
                btn.disabled = false;
            }
            alert(`GitHub DPM Step ${stepNumber} failed: ${data.error || 'Unknown error'}`);
        }
    })
    .catch(error => {
        console.error('GitHub DPM step execution error:', error);
        if (btn) {
            btn.textContent = '✗ Failed';
            btn.classList.remove('running');
            btn.classList.add('failed');
            btn.disabled = false;
        }
        alert(`Error executing GitHub DPM Step ${stepNumber}: ${error.message}`);
    });
}

// GitHub DPM Config Modal functions
function openGitHubDPMConfigModal() {
    document.getElementById('github-dpm-config-modal').style.display = 'block';
    document.body.classList.add('modal-open');
}

function closeGitHubDPMConfigModal() {
    document.getElementById('github-dpm-config-modal').style.display = 'none';
    document.body.classList.remove('modal-open');
}

function validateGitHubDPMPackage() {
    const repoUrl = document.getElementById('github-dpm-repo-url').value;
    const token = document.getElementById('github-dpm-token').value;
    const resultDiv = document.getElementById('github-dpm-validation-result');
    const saveBtn = document.getElementById('save-github-dpm-config-btn');

    if (!repoUrl) {
        resultDiv.innerHTML = '<div class="alert alert-warning">Please enter a repository URL</div>';
        resultDiv.style.display = 'block';
        return;
    }

    resultDiv.innerHTML = '<div class="alert alert-info">Validating...</div>';
    resultDiv.style.display = 'block';

    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', document.querySelector('[name=csrfmiddlewaretoken]').value);
    formData.append('github_url', repoUrl);
    if (token) formData.append('github_token', token);

    fetch('/pybirdai/workflow/dpm/github/validate-package/', {
        method: 'POST',
        body: formData,
        headers: { 'X-Requested-With': 'XMLHttpRequest' }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success && data.valid) {
            resultDiv.innerHTML = `<div class="alert alert-success">✓ Valid package (${data.source_type})</div>`;
            saveBtn.disabled = false;
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">✗ ${data.error || 'Invalid package'}</div>`;
            saveBtn.disabled = true;
        }
    })
    .catch(error => {
        resultDiv.innerHTML = `<div class="alert alert-danger">✗ Validation failed: ${error.message}</div>`;
        saveBtn.disabled = true;
    });
}

function saveGitHubDPMConfig() {
    const repoUrl = document.getElementById('github-dpm-repo-url').value;
    const branch = document.getElementById('github-dpm-branch-input').value || 'main';
    const token = document.getElementById('github-dpm-token').value;

    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', document.querySelector('[name=csrfmiddlewaretoken]').value);
    formData.append('github_url', repoUrl);
    formData.append('github_branch', branch);
    if (token) formData.append('github_token', token);

    fetch('/pybirdai/workflow/dpm/github/configure/', {
        method: 'POST',
        body: formData,
        headers: { 'X-Requested-With': 'XMLHttpRequest' }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            // Update the UI with saved config
            document.getElementById('github-dpm-url').value = repoUrl;
            document.getElementById('github-dpm-branch').value = branch;
            closeGitHubDPMConfigModal();
            alert('GitHub configuration saved successfully!');
        } else {
            alert('Failed to save configuration: ' + (data.error || 'Unknown error'));
        }
    })
    .catch(error => {
        alert('Error saving configuration: ' + error.message);
    });
}

// Restore DPM source tab on page load
document.addEventListener('DOMContentLoaded', function() {
    const savedSource = localStorage.getItem('dpm_source_tab');
    if (savedSource) {
        switchDPMSourceTab(savedSource);
    }
});
```

### 9. Update Dashboard View to Include GitHub DPM Grid

**File:** `pybirdai/views/workflow/dashboard.py`

**Add to context:**
```python
from .dpm.github_execution import get_github_dpm_task_grid

# In workflow_dashboard function, add:
github_dpm_task_grid = get_github_dpm_task_grid(workflow_session) if workflow_session else []

context = {
    # ... existing context ...
    'github_dpm_task_grid': github_dpm_task_grid,
}
```

### 10. Database Migration

After all code changes are complete:
```bash
cd birds_nest
python manage.py makemigrations pybirdai
python manage.py migrate
```

---

## Testing Checklist

1. [ ] Model migration runs successfully
2. [ ] EBA Source tab shows existing 6-step workflow
3. [ ] GitHub Source tab shows new 4-step workflow
4. [ ] Tab switching persists across page reloads
5. [ ] GitHub config modal opens and closes properly
6. [ ] Package validation works for regcommunity repos
7. [ ] Step 1 (Import Data) fetches from GitHub
8. [ ] Step 2 (Generate Structure Links) creates joins metadata
9. [ ] Step 3 (Generate Code) creates Python files
10. [ ] Step 4 (Run Tests) executes test suite
11. [ ] Review pages show correct step information
12. [ ] Error handling works for invalid URLs
13. [ ] Progress tracking works for both workflows

---

## File Summary

### Modified Files:
- `pybirdai/models/workflow_model.py` - Added GitHub fields to DPMProcessExecution
- `pybirdai/urls.py` - Added GitHub DPM URL routes
- `pybirdai/views/workflow/__init__.py` - Added GitHub DPM exports
- `pybirdai/views/workflow/dpm/__init__.py` - Added GitHub DPM exports
- `pybirdai/templates/pybirdai/workflow/dpm_workflow.html` - Added subtabs
- `pybirdai/templates/pybirdai/workflow/dashboard.html` - Include new modal
- `pybirdai/static/pybirdai/css/dashboard/dashboard.css` - Subtab styles
- `pybirdai/static/pybirdai/js/dashboard/workflow-execution.js` - Tab switching & execution
- `pybirdai/views/workflow/dashboard.py` - Add github_dpm_task_grid to context

### New Files:
- `pybirdai/views/workflow/dpm/github_execution.py` - GitHub DPM views
- `pybirdai/templates/pybirdai/workflow/dpm_workflow/github_source/review.html`
- `pybirdai/templates/pybirdai/workflow/modals/github_dpm_config_modal.html`
