// coding=UTF-8
// Copyright (c) 2025 Bird Software Solutions Ltd
// SPDX-License-Identifier: EPL-2.0

/**
 * Workflow Execution Functions
 * Handles: ANCRDT steps, BIRD tasks, workflow toggles
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
    statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Starting Complete ANCRDT Flow...';

    // Step 1: Execute BIRD Task 1 (Import Input Model)
    executeBIRDTask1(csrfToken)
        .then(() => {
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">✅</span>BIRD Task 1 completed. Starting ANCRDT Step 0...';
            return executeANCRDTStepSequential(0, csrfToken);
        })
        .then(() => {
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">✅</span>ANCRDT Step 0 completed. Starting Step 1...';
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

// Restore accordion state on page load
document.addEventListener('DOMContentLoaded', function() {
    // Restore accordion states from localStorage
    ['ancrdt'].forEach(workflowId => {
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
});
