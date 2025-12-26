// coding=UTF-8
// Copyright (c) 2025 Bird Software Solutions Ltd
// SPDX-License-Identifier: EPL-2.0

/**
 * Workflow Execution Functions
 * Handles: DPM steps, ANCRDT steps, BIRD tasks, workflow toggles
 */

// Accordion toggle function
function toggleWorkflow(workflowId) {
    const content = document.getElementById(workflowId + '-content');
    const toggle = document.getElementById(workflowId + '-toggle');

    if (!content || !toggle) {
        console.error('Workflow content or toggle not found:', workflowId);
        return;
    }

    if (content.classList.contains('collapsed')) {
        content.classList.remove('collapsed');
        toggle.textContent = '▼';
        localStorage.setItem('workflow_' + workflowId + '_expanded', 'true');
    } else {
        content.classList.add('collapsed');
        toggle.textContent = '▶';
        localStorage.setItem('workflow_' + workflowId + '_expanded', 'false');
    }
}

// Execute DPM step
function executeDPMStep(stepNumber, params = {}) {
    console.log('executeDPMStep() called for step:', stepNumber);

    // For Step 2, show table selection modal first (unless already coming from modal)
    if (stepNumber === 2 && !params.selected_tables) {
        console.log('Step 2 clicked - showing table selection modal');
        showDPMTableSelectionModal(2);
        return;
    }

    // Find button
    const btn = document.getElementById(`dpm-step-${stepNumber}-btn`);
    const fromModal = params.fromModal;

    if (!btn && !fromModal) {
        console.error('DPM button not found for step:', stepNumber);
        alert('Error: Button not found!');
        return;
    }

    // Log if button not found but proceeding anyway (from modal)
    if (!btn && fromModal) {
        console.log('Button not found, but proceeding from modal');
    }

    // Get CSRF token
    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]')?.value;
    if (!csrfToken) {
        alert('Error: CSRF token not found! Please refresh the page.');
        return;
    }

    // Update button state (only if button exists)
    if (btn) {
        btn.disabled = true;
        btn.textContent = '⏳ Running...';
        btn.classList.remove('pending', 'completed', 'failed');
        btn.classList.add('running');
    }

    // Create form data
    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', csrfToken);

    // For Step 1, collect selected frameworks
    if (stepNumber === 1) {
        const frameworkSelect = window.frameworkTomSelect;
        if (frameworkSelect) {
            const selectedFrameworks = frameworkSelect.getValue();
            if (Array.isArray(selectedFrameworks)) {
                selectedFrameworks.forEach(fw => formData.append('frameworks', fw));
            } else if (selectedFrameworks) {
                formData.append('frameworks', selectedFrameworks);
            }
        }
    }

    // For Step 2, add selected tables from modal
    if (stepNumber === 2 && params.selected_tables) {
        formData.append('selected_tables', JSON.stringify(params.selected_tables));
    }

    // Add output layer parameters if provided
    if (params.framework) {
        formData.append('framework', params.framework);
    }
    if (params.version) {
        formData.append('version', params.version);
    }
    if (params.table_code) {
        formData.append('table_code', params.table_code);
    }
    if (params.table_codes) {
        formData.append('table_codes', params.table_codes);
    }

    // Make AJAX request
    fetch(`/pybirdai/workflow/dpm/execute/${stepNumber}/`, {
        method: 'POST',
        body: formData,
        headers: {
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        console.log('DPM step response:', data);

        if (data.success && data.status === 'completed') {
            // Update button state if it exists
            if (btn) {
                btn.textContent = '✓ Done';
                btn.classList.remove('running');
                btn.classList.add('completed');
            }
            alert(`DPM Step ${stepNumber} completed successfully!`);

            // Check if this is Step 2 from the modal - if so, redirect to Output Layer Mapping
            if (stepNumber === 2 && params && params.fromModal) {
                setTimeout(() => {
                    window.location.href = '/pybirdai/output-layer-mapping/step1/';
                }, 1500);
            } else {
                // Refresh page after success to update status
                setTimeout(() => {
                    location.reload();
                }, 1500);
            }
        } else if (data.success && data.status === 'partial') {
            // Partial success - some tables succeeded, some failed
            if (btn) {
                btn.textContent = '⚠ Partial';
                btn.classList.remove('running');
                btn.classList.add('paused');
            }

            const details = data.details || {};
            const processed = details.processed || 0;
            const errors = details.errors || 0;

            let errorDetails = '';
            if (details.error_list && details.error_list.length > 0) {
                errorDetails = '\n\nFailed tables:\n';
                details.error_list.forEach(err => {
                    errorDetails += `- ${err.table_code}: ${err.error}\n`;
                });
            }

            alert(`DPM Step ${stepNumber} partially completed:\n${processed} table(s) processed successfully\n${errors} table(s) failed${errorDetails}`);

            // Check if this is Step 2 from the modal - if so, redirect to Output Layer Mapping
            if (stepNumber === 2 && params && params.fromModal) {
                setTimeout(() => {
                    window.location.href = '/pybirdai/output-layer-mapping/step1/';
                }, 1500);
            } else {
                // Refresh page after partial success to update status
                setTimeout(() => {
                    location.reload();
                }, 1500);
            }
        } else {
            // Update button state if it exists
            if (btn) {
                btn.textContent = '✗ Failed';
                btn.classList.remove('running');
                btn.classList.add('failed');
                btn.disabled = false;
            }

            // Build detailed error message
            let errorMsg = data.error || 'Unknown error';
            if (data.details && data.details.length > 0) {
                errorMsg += '\n\nDetails:\n';
                data.details.forEach(err => {
                    errorMsg += `- ${err.table_code}: ${err.error}\n`;
                });
            }

            alert(`DPM Step ${stepNumber} failed: ${errorMsg}`);
        }
    })
    .catch(error => {
        console.error('DPM step execution error:', error);
        // Update button state if it exists
        if (btn) {
            btn.textContent = '✗ Failed';
            btn.classList.remove('running');
            btn.classList.add('failed');
            btn.disabled = false;
        }
        alert(`Error executing DPM Step ${stepNumber}: ${error.message}`);
    });
}

// Execute AnaCredit step
function executeANCRDTStep(stepNumber) {
    console.log('executeANCRDTStep() called for step:', stepNumber);

    const btn = document.getElementById(`ancrdt-step-${stepNumber}-btn`);
    if (!btn) {
        console.error('AnaCredit button not found for step:', stepNumber);
        alert('Error: Button not found!');
        return;
    }

    // Get CSRF token
    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]')?.value;
    if (!csrfToken) {
        alert('Error: CSRF token not found! Please refresh the page.');
        return;
    }

    // Update button state
    btn.disabled = true;
    btn.textContent = '⏳ Running...';
    btn.classList.remove('pending', 'completed', 'failed');
    btn.classList.add('running');

    // Create form data
    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', csrfToken);

    // Make AJAX request
    fetch(`/pybirdai/workflow/ancrdt/execute/${stepNumber}/`, {
        method: 'POST',
        body: formData,
        headers: {
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => {
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        console.log('AnaCredit step response:', data);

        if (data.success && data.status === 'completed') {
            btn.textContent = '✓ Done';
            btn.classList.remove('running');
            btn.classList.add('completed');
            alert(`AnaCredit Step ${stepNumber} completed successfully!`);

            // Refresh page after success to update status
            setTimeout(() => {
                location.reload();
            }, 1500);
        } else {
            btn.textContent = '✗ Failed';
            btn.classList.remove('running');
            btn.classList.add('failed');
            btn.disabled = false;
            alert(`AnaCredit Step ${stepNumber} failed: ${data.error || 'Unknown error'}`);
        }
    })
    .catch(error => {
        console.error('AnaCredit step execution error:', error);
        btn.textContent = '✗ Failed';
        btn.classList.remove('running');
        btn.classList.add('failed');
        btn.disabled = false;
        alert(`Error executing AnaCredit Step ${stepNumber}: ${error.message}`);
    });
}

// Execute Complete ANCRDT Flow (BIRD Task 1 + All ANCRDT Steps)
function executeCompleteANCRDTFlow() {
    console.log('executeCompleteANCRDTFlow() called');

    const btn = document.getElementById('quick-ancrdt-btn');
    const statusDiv = document.getElementById('quick-ancrdt-status');

    if (!btn || !statusDiv) {
        console.error('ANCRDT quick action button or status div not found!');
        alert('Error: UI elements not found!');
        return;
    }

    // Get CSRF token
    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]')?.value;
    if (!csrfToken) {
        alert('Error: CSRF token not found! Please refresh the page.');
        return;
    }

    // Disable button and show initial status
    btn.disabled = true;
    btn.textContent = '⏳ Running...';
    statusDiv.style.display = 'block';
    statusDiv.style.background = '#e3f2fd';
    statusDiv.style.color = '#1976d2';
    statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Fetching ANCRDT artifacts...';

    // Step 0: Fetch ANCRDT artifacts, then execute Steps 1-4
    fetchANCRDTArtifacts(csrfToken)
        .then(() => {
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">✅</span>Artifacts fetched. Starting ANCRDT Step 1...';
            return executeANCRDTStepSequential(1, csrfToken);
        })
        .then(() => {
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">✅</span>ANCRDT Step 1 completed. Starting Step 2...';
            return executeANCRDTStepSequential(2, csrfToken);
        })
        .then(() => {
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">✅</span>ANCRDT Step 2 completed. Starting Step 3...';
            return executeANCRDTStepSequential(3, csrfToken);
        })
        .then(() => {
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">✅</span>ANCRDT Step 3 completed. Starting Step 4 (Tests)...';
            return executeANCRDTStepSequential(4, csrfToken);
        })
        .then(() => {
            // All steps completed successfully
            statusDiv.style.background = '#d4edda';
            statusDiv.style.color = '#155724';
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">✅</span>🎉 Complete ANCRDT Flow finished successfully!';
            btn.disabled = false;
            btn.textContent = 'Run Complete ANCRDT Flow';

            // Refresh page after success to update status
            setTimeout(() => {
                location.reload();
            }, 3000);
        })
        .catch(error => {
            // Handle errors
            console.error('Complete ANCRDT Flow error:', error);
            statusDiv.style.background = '#f8d7da';
            statusDiv.style.color = '#721c24';
            statusDiv.innerHTML = `<span style="display: inline-block; margin-right: 8px;">❌</span>Error: ${error.message}`;
            btn.disabled = false;
            btn.textContent = 'Run Complete ANCRDT Flow';
        });
}

// Helper function to fetch ANCRDT artifacts from GitHub
// This uses the dedicated ANCRDT fetch endpoint that fetches only ANCRDT framework files
function fetchANCRDTArtifacts(csrfToken) {
    return new Promise((resolve, reject) => {
        const formData = new FormData();
        formData.append('csrfmiddlewaretoken', csrfToken);

        // Use the dedicated ANCRDT fetch endpoint
        fetch('/pybirdai/api/workflow/fetch-ancrdt/', {
            method: 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            return response.json();
        })
        .then(data => {
            if (data.success) {
                console.log('ANCRDT artifacts fetched successfully:', data);
                resolve(data);
            } else {
                reject(new Error(data.error || 'Failed to fetch ANCRDT artifacts'));
            }
        })
        .catch(error => {
            reject(error);
        });
    });
}

// Helper function to execute BIRD Task 1
function executeBIRDTask1(csrfToken) {
    return new Promise((resolve, reject) => {
        const formData = new FormData();
        formData.append('csrfmiddlewaretoken', csrfToken);

        fetch('/pybirdai/workflow/task/1/do/', {
            method: 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            return response.json();
        })
        .then(data => {
            if (data.success) {
                resolve(data);
            } else {
                reject(new Error(data.error || 'BIRD Task 1 failed'));
            }
        })
        .catch(error => {
            reject(new Error(`BIRD Task 1 execution failed: ${error.message}`));
        });
    });
}

// Helper function to execute ANCRDT steps sequentially
function executeANCRDTStepSequential(stepNumber, csrfToken) {
    return new Promise((resolve, reject) => {
        const formData = new FormData();
        formData.append('csrfmiddlewaretoken', csrfToken);

        fetch(`/pybirdai/workflow/ancrdt/execute/${stepNumber}/`, {
            method: 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            return response.json();
        })
        .then(data => {
            if (data.success && data.status === 'completed') {
                resolve(data);
            } else {
                reject(new Error(data.error || `ANCRDT Step ${stepNumber} failed`));
            }
        })
        .catch(error => {
            reject(new Error(`ANCRDT Step ${stepNumber} execution failed: ${error.message}`));
        });
    });
}

// Helper function to execute DPM steps sequentially
function executeDPMStepSequential(stepNumber, csrfToken, params = {}) {
    return new Promise((resolve, reject) => {
        const formData = new FormData();
        formData.append('csrfmiddlewaretoken', csrfToken);

        // For steps 1 and 2, collect selected frameworks
        if (stepNumber === 1 || stepNumber === 2) {
            const frameworkSelect = window.frameworkTomSelect;
            if (frameworkSelect) {
                const selectedFrameworks = frameworkSelect.getValue();
                if (Array.isArray(selectedFrameworks)) {
                    selectedFrameworks.forEach(fw => formData.append('frameworks', fw));
                } else if (selectedFrameworks) {
                    formData.append('frameworks', selectedFrameworks);
                }
            }
        }

        // Add optional parameters if provided
        if (params.framework) {
            formData.append('framework', params.framework);
        }
        if (params.version) {
            formData.append('version', params.version);
        }
        if (params.table_code) {
            formData.append('table_code', params.table_code);
        }
        if (params.table_codes) {
            formData.append('table_codes', params.table_codes);
        }

        fetch(`/pybirdai/workflow/dpm/execute/${stepNumber}/`, {
            method: 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            return response.json();
        })
        .then(data => {
            console.log(`DPM Step ${stepNumber} response:`, data);

            if (data.status === 'awaiting_selection') {
                // Step 1 Phase A completed - show table selection modal
                console.log('DPM Step 1 Phase A completed, showing table selection modal');
                showDPMTableSelectionModal(1);
                resolve({
                    success: true,
                    status: 'awaiting_selection',
                    table_count: data.table_count,
                    message: 'Awaiting table selection from user'
                });
            } else if (data.success && (data.status === 'completed' || data.status === 'partial')) {
                resolve(data);
            } else {
                console.error('DPM Step execution failed:', data);
                reject(new Error(data.error || `DPM Step ${stepNumber} failed`));
            }
        })
        .catch(error => {
            reject(new Error(`DPM Step ${stepNumber} execution failed: ${error.message}`));
        });
    });
}

// Execute BIRD Task 1 + DPM Steps 1-2, then redirect to manual workflow
function executeBIRDTask1ThenDPMManual() {
    const btn = document.getElementById('bird-task1-dpm-manual-btn');
    const statusDiv = document.getElementById('bird-task1-dpm-status');
    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]').value;

    if (!btn || !statusDiv || !csrfToken) {
        console.error('Missing required elements or CSRF token');
        return;
    }

    // Disable button and show progress
    btn.disabled = true;
    btn.textContent = '⏳ Running Task 1...';
    statusDiv.style.display = 'block';
    statusDiv.style.background = '#e3f2fd';
    statusDiv.style.color = '#1976d2';
    statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Executing BIRD Task 1 (SMCubes Core Creation)...';

    // Execute BIRD Task 1
    executeBIRDTask1(csrfToken)
        .then(() => {
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">✅</span>BIRD Task 1 completed. Starting DPM Step 1...';
            btn.textContent = '⏳ Running DPM Step 1...';
            return executeDPMStepSequential(1, csrfToken);
        })
        .then((step1Result) => {
            // Step 1 completed - show table selection modal for Step 2
            statusDiv.style.background = '#d4edda';
            statusDiv.style.color = '#155724';
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">✅</span>DPM Step 1 completed. Please select tables from the modal to continue.';
            btn.textContent = 'Run DPM Workflow until Output Layer Generation';
            btn.disabled = false;

            // Show the table selection modal for Step 2
            showDPMTableSelectionModal(null);
        })
        .catch(error => {
            // Handle errors
            console.error('BIRD Task 1 + DPM Manual Flow error:', error);
            statusDiv.style.background = '#f8d7da';
            statusDiv.style.color = '#721c24';
            statusDiv.innerHTML = `<span style="display: inline-block; margin-right: 8px;">❌</span>Error: ${error.message}`;
            btn.disabled = false;
            btn.textContent = 'Run DPM Workflow until Output Layer Generation';
        });
}

// Restore accordion state on page load
document.addEventListener('DOMContentLoaded', function() {
    // Restore accordion states from localStorage
    ['dpm', 'ancrdt'].forEach(workflowId => {
        const expanded = localStorage.getItem('workflow_' + workflowId + '_expanded');
        // Auto-expand if there's an active task
        const hasActiveTask = document.querySelector(`[data-workflow="${workflowId}"] .operation-btn.running`);

        if (expanded === 'true' || hasActiveTask) {
            const content = document.getElementById(workflowId + '-content');
            const toggle = document.getElementById(workflowId + '-toggle');
            if (content && toggle && content.classList.contains('collapsed')) {
                toggleWorkflow(workflowId);
            }
        }
    });

    // Restore DPM source tab state from localStorage
    const savedDPMSource = localStorage.getItem('dpm_source_tab');
    if (savedDPMSource) {
        switchDPMSourceTab(savedDPMSource);
    }
});

// ===========================================
// DPM Source Tab Switching
// ===========================================

/**
 * Switch between EBA Source and GitHub Source tabs for DPM workflow
 * @param {string} source - 'eba' or 'github'
 */
function switchDPMSourceTab(source) {
    console.log('Switching DPM source tab to:', source);

    // Update tab buttons
    document.querySelectorAll('.dpm-source-tab').forEach(tab => {
        if (tab.dataset.source === source) {
            tab.classList.add('active');
        } else {
            tab.classList.remove('active');
        }
    });

    // Update content panels
    document.querySelectorAll('.dpm-source-content').forEach(content => {
        if (content.dataset.source === source) {
            content.classList.add('active');
        } else {
            content.classList.remove('active');
        }
    });

    // Save preference to localStorage
    localStorage.setItem('dpm_source_tab', source);
}

// ===========================================
// GitHub DPM Step Execution
// ===========================================

/**
 * Execute a GitHub DPM workflow step
 * @param {number} stepNumber - The step number to execute (1-4)
 */
function executeGitHubDPMStep(stepNumber) {
    console.log('executeGitHubDPMStep() called for step:', stepNumber);

    const btn = document.getElementById(`github-dpm-step-${stepNumber}-btn`);
    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]')?.value;

    if (!csrfToken) {
        alert('Error: CSRF token not found! Please refresh the page.');
        return;
    }

    // Get GitHub configuration from the inline inputs
    const githubUrl = document.getElementById('github-dpm-url')?.value || '';
    const githubBranch = document.getElementById('github-dpm-branch')?.value || 'main';
    const githubToken = document.getElementById('github-dpm-token')?.value || '';

    // For Step 1, require a GitHub URL
    if (!githubUrl && stepNumber === 1) {
        alert('Please configure a GitHub package URL first.\n\nEnter a repository URL like:\nhttps://github.com/regcommunity/FreeBIRD_COREP');
        openGitHubDPMConfigModal();
        return;
    }

    // Update button state
    if (btn) {
        btn.disabled = true;
        btn.textContent = '\u23F3 Running...';
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
    .then(response => {
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        console.log('GitHub DPM step response:', data);

        if (data.success && data.status === 'completed') {
            if (btn) {
                btn.textContent = '\u2713 Done';
                btn.classList.remove('running');
                btn.classList.add('completed');
            }
            alert(`GitHub DPM Step ${stepNumber} completed successfully!`);
            setTimeout(() => location.reload(), 1500);
        } else {
            if (btn) {
                btn.textContent = '\u2717 Failed';
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
            btn.textContent = '\u2717 Failed';
            btn.classList.remove('running');
            btn.classList.add('failed');
            btn.disabled = false;
        }
        alert(`Error executing GitHub DPM Step ${stepNumber}: ${error.message}`);
    });
}

// ===========================================
// GitHub DPM Config Modal Functions
// ===========================================

/**
 * Open the GitHub DPM configuration modal
 */
function openGitHubDPMConfigModal() {
    const modal = document.getElementById('github-dpm-config-modal');
    if (modal) {
        modal.style.display = 'block';
        document.body.classList.add('modal-open');

        // Pre-fill modal inputs with current values from inline inputs
        const urlInput = document.getElementById('github-dpm-url');
        const branchInput = document.getElementById('github-dpm-branch');

        if (urlInput && document.getElementById('github-dpm-repo-url')) {
            document.getElementById('github-dpm-repo-url').value = urlInput.value;
        }
        if (branchInput && document.getElementById('github-dpm-branch-input')) {
            document.getElementById('github-dpm-branch-input').value = branchInput.value;
        }
    } else {
        console.error('GitHub DPM config modal not found');
        alert('Configuration modal not available. Please enter the GitHub URL directly.');
    }
}

/**
 * Close the GitHub DPM configuration modal
 */
function closeGitHubDPMConfigModal() {
    const modal = document.getElementById('github-dpm-config-modal');
    if (modal) {
        modal.style.display = 'none';
        document.body.classList.remove('modal-open');
    }
}

/**
 * Validate a GitHub DPM package before import
 */
function validateGitHubDPMPackage() {
    const repoUrl = document.getElementById('github-dpm-repo-url')?.value || '';
    const token = document.getElementById('github-dpm-token')?.value || '';
    const resultDiv = document.getElementById('github-dpm-validation-result');
    const saveBtn = document.getElementById('save-github-dpm-config-btn');
    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]')?.value;

    if (!repoUrl) {
        if (resultDiv) {
            resultDiv.innerHTML = '<div class="alert alert-warning">Please enter a repository URL</div>';
            resultDiv.style.display = 'block';
        }
        return;
    }

    if (!csrfToken) {
        alert('Error: CSRF token not found! Please refresh the page.');
        return;
    }

    if (resultDiv) {
        resultDiv.innerHTML = '<div class="alert alert-info">\u23F3 Validating...</div>';
        resultDiv.style.display = 'block';
    }

    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', csrfToken);
    formData.append('github_url', repoUrl);
    if (token) {
        formData.append('github_token', token);
    }

    fetch('/pybirdai/workflow/dpm/github/validate-package/', {
        method: 'POST',
        body: formData,
        headers: {
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => response.json())
    .then(data => {
        console.log('Validation result:', data);
        if (data.success && data.valid) {
            if (resultDiv) {
                resultDiv.innerHTML = `<div class="alert alert-success">\u2713 Valid package (${data.source_type || 'regcommunity'})</div>`;
            }
            if (saveBtn) {
                saveBtn.disabled = false;
            }
        } else {
            if (resultDiv) {
                resultDiv.innerHTML = `<div class="alert alert-danger">\u2717 ${data.error || 'Invalid package'}</div>`;
            }
            if (saveBtn) {
                saveBtn.disabled = true;
            }
        }
    })
    .catch(error => {
        console.error('Validation error:', error);
        if (resultDiv) {
            resultDiv.innerHTML = `<div class="alert alert-danger">\u2717 Validation failed: ${error.message}</div>`;
        }
        if (saveBtn) {
            saveBtn.disabled = true;
        }
    });
}

/**
 * Save GitHub DPM configuration
 */
function saveGitHubDPMConfig() {
    const repoUrl = document.getElementById('github-dpm-repo-url')?.value || '';
    const branch = document.getElementById('github-dpm-branch-input')?.value || 'main';
    const token = document.getElementById('github-dpm-token')?.value || '';
    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]')?.value;

    if (!csrfToken) {
        alert('Error: CSRF token not found! Please refresh the page.');
        return;
    }

    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', csrfToken);
    formData.append('github_url', repoUrl);
    formData.append('github_branch', branch);
    if (token) {
        formData.append('github_token', token);
    }

    fetch('/pybirdai/workflow/dpm/github/configure/', {
        method: 'POST',
        body: formData,
        headers: {
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => response.json())
    .then(data => {
        console.log('Save config result:', data);
        if (data.success) {
            // Update the inline inputs with saved config
            const urlInput = document.getElementById('github-dpm-url');
            const branchInput = document.getElementById('github-dpm-branch');

            if (urlInput) {
                urlInput.value = repoUrl;
            }
            if (branchInput) {
                branchInput.value = branch;
            }

            closeGitHubDPMConfigModal();
            alert('GitHub configuration saved successfully!');
        } else {
            alert('Failed to save configuration: ' + (data.error || 'Unknown error'));
        }
    })
    .catch(error => {
        console.error('Save config error:', error);
        alert('Error saving configuration: ' + error.message);
    });
}
