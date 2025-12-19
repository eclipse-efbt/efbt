// coding=UTF-8
// Copyright (c) 2025 Bird Software Solutions Ltd
// SPDX-License-Identifier: EPL-2.0

/**
 * Session Management Functions
 * Handles: Clone import, reset session (partial/full)
 */

// Clone import function
function startCloneImport() {
    console.log('startCloneImport() called');

    const btn = document.getElementById('clone-btn');
    const statusDiv = document.getElementById('clone-status');

    if (!btn || !statusDiv) {
        console.error('Clone button or status div not found!');
        alert('Error: UI elements not found!');
        return;
    }

    // Get CSRF token
    const automodeForm = document.getElementById('automode-form');
    let csrfToken = '';

    if (automodeForm) {
        const csrfInput = automodeForm.querySelector('[name=csrfmiddlewaretoken]');
        if (csrfInput) {
            csrfToken = csrfInput.value;
        }
    }

    if (!csrfToken) {
        alert('Error: CSRF token not found! Please refresh the page.');
        return;
    }

    // Update UI
    btn.disabled = true;
    btn.textContent = 'Importing CSVs...';
    statusDiv.style.display = 'block';
    statusDiv.style.background = '#e3f2fd';
    statusDiv.style.color = '#1976d2';
    statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Cloning Previous smcubes database setup from github';

    // Create form data
    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', csrfToken);

    // Make AJAX request to clone endpoint
    fetch('/pybirdai/workflow/clone-import/', {
        method: 'POST',
        body: formData,
        headers: {
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => {
        console.log('Clone import response received:', response.status);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        console.log('Clone import response data:', data);
        if (data.success) {
            statusDiv.style.background = '#d4edda';
            statusDiv.style.color = '#155724';
            let message = `<span style="display: inline-block; margin-right: 8px;">✅</span>${data.message}`;
            if (data.details) {
                message += `<br><small>${data.details}</small>`;
            }
            statusDiv.innerHTML = message;

            // Re-enable button after success
            setTimeout(() => {
                btn.disabled = false;
                btn.textContent = 'Clone (beta version)';
                // Optionally refresh to show updated data
                if (data.refresh_recommended) {
                    setTimeout(() => {
                        location.reload();
                    }, 2000);
                }
            }, 3000);
        } else {
            statusDiv.style.background = '#f8d7da';
            statusDiv.style.color = '#721c24';
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Clone import failed: ' + (data.error || data.message || 'Unknown error');

            btn.disabled = false;
            btn.textContent = 'Clone (beta version)';
        }
    })
    .catch(error => {
        console.error('Clone import error:', error);
        statusDiv.style.background = '#f8d7da';
        statusDiv.style.color = '#721c24';
        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Error: ' + error.message;

        btn.disabled = false;
        btn.textContent = 'Clone (beta version)';
    });
}

// Reset session functions
function resetSessionPartial() {
    if (!confirm('Are you sure you want to reset tasks 1-4? This will clear all progress from Task 1 onwards while keeping the database setup (Tasks 1-2) intact.')) {
        return;
    }

    doResetSession('partial');
}

function resetSessionFull() {
    if (!confirm('Are you sure you want to reset the entire workflow session? This will clear ALL progress including database setup and return to the beginning.')) {
        return;
    }

    doResetSession('full');
}

// Reset database function - Full database wipe (removes ALL framework data and input model)
function resetDatabase() {
    if (!confirm('WARNING: This will delete ALL data from the database including:\n\n- All framework data (FINREP, ANCRDT, DPM, etc.)\n- All input model data (DOMAIN, VARIABLE, MEMBER)\n- All cubes, mappings, and transformations\n\nThis action cannot be undone. Are you sure you want to continue?')) {
        return;
    }

    // Second confirmation for safety
    if (!confirm('FINAL CONFIRMATION: You are about to DELETE THE ENTIRE DATABASE.\n\nClick OK to proceed with database reset.')) {
        return;
    }

    const btn = document.getElementById('reset-database-btn');
    const statusDiv = document.getElementById('reset-database-status');

    if (!btn || !statusDiv) {
        console.error('Reset database button or status div not found!');
        alert('Error: UI elements not found!');
        return;
    }

    // Get CSRF token
    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]').value;

    if (!csrfToken) {
        alert('Error: CSRF token not found! Please refresh the page.');
        return;
    }

    // Update UI
    btn.disabled = true;
    btn.textContent = 'Resetting Database...';
    statusDiv.style.display = 'block';
    statusDiv.style.background = '#ffebee';
    statusDiv.style.color = '#c62828';
    statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Deleting all database data... This may take a moment.';

    // Create form data
    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', csrfToken);

    // Make AJAX request
    fetch('/pybirdai/workflow/reset-database/', {
        method: 'POST',
        body: formData,
        headers: {
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => {
        console.log('Reset database response received:', response.status);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        console.log('Reset database response data:', data);
        if (data.success) {
            statusDiv.style.background = '#d4edda';
            statusDiv.style.color = '#155724';
            let message = `<span style="display: inline-block; margin-right: 8px;">✅</span>${data.message}`;
            if (data.details) {
                message += `<br><small>${JSON.stringify(data.details)}</small>`;
            }
            statusDiv.innerHTML = message;

            // Re-enable button and refresh page after delay
            setTimeout(() => {
                btn.disabled = false;
                btn.textContent = 'Reset Database';
                statusDiv.innerHTML += '<br><small>Refreshing page to reflect changes...</small>';

                // Refresh page to show updated state
                setTimeout(() => {
                    location.reload();
                }, 2000);
            }, 3000);
        } else {
            statusDiv.style.background = '#f8d7da';
            statusDiv.style.color = '#721c24';
            statusDiv.innerHTML = `<span style="display: inline-block; margin-right: 8px;">❌</span>Reset failed: ${data.message || data.error || 'Unknown error'}`;

            btn.disabled = false;
            btn.textContent = 'Reset Database';
        }
    })
    .catch(error => {
        console.error('Reset database error:', error);
        statusDiv.style.background = '#f8d7da';
        statusDiv.style.color = '#721c24';
        statusDiv.innerHTML = `<span style="display: inline-block; margin-right: 8px;">❌</span>Error: ${error.message}`;

        btn.disabled = false;
        btn.textContent = 'Reset Database';
    });
}

function doResetSession(type) {
    const isPartial = type === 'partial';
    const btnId = isPartial ? 'reset-session-partial-btn' : 'reset-session-full-btn';
    const url = isPartial ? '/pybirdai/workflow/reset-session-partial/' : '/pybirdai/workflow/reset-session-full/';

    const btn = document.getElementById(btnId);
    const statusDiv = document.getElementById('reset-session-status');

    if (!btn || !statusDiv) {
        console.error('Reset session button or status div not found!');
        alert('Error: UI elements not found!');
        return;
    }

    // Get CSRF token
    const csrfToken = document.querySelector('[name=csrfmiddlewaretoken]').value;

    if (!csrfToken) {
        alert('Error: CSRF token not found! Please refresh the page.');
        return;
    }

    // Update UI
    btn.disabled = true;
    btn.textContent = `Resetting ${isPartial ? 'Tasks 1-4' : 'All Tasks'}...`;
    statusDiv.style.display = 'block';
    statusDiv.style.background = '#e3f2fd';
    statusDiv.style.color = '#1976d2';
    statusDiv.innerHTML = `<span style="display: inline-block; margin-right: 8px;">⏳</span>Resetting ${isPartial ? 'tasks 1-4' : 'entire workflow session'}...`;

    // Create form data
    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', csrfToken);

    // Make AJAX request
    fetch(url, {
        method: 'POST',
        body: formData,
        headers: {
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => {
        console.log('Reset session response received:', response.status);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        console.log('Reset session response data:', data);
        if (data.success) {
            statusDiv.style.background = '#d4edda';
            statusDiv.style.color = '#155724';
            let message = `<span style="display: inline-block; margin-right: 8px;">✅</span>${data.message}`;

            if (data.details) {
                const details = data.details;
                let detailsText = '';
                if (details.removed_markers && details.removed_markers.length > 0) {
                    detailsText += `<br><small>Removed markers: ${details.removed_markers.join(', ')}</small>`;
                }
                if (details.removed_directories && details.removed_directories.length > 0) {
                    detailsText += `<br><small>Cleaned directories: ${details.removed_directories.length} items</small>`;
                }
                if (details.deleted_executions) {
                    detailsText += `<br><small>Deleted executions: ${details.deleted_executions}</small>`;
                }
                message += detailsText;
            }

            statusDiv.innerHTML = message;

            // Re-enable button and refresh page after delay
            setTimeout(() => {
                btn.disabled = false;
                btn.textContent = isPartial ? 'Reset Tasks 1-4' : 'Reset Everything';
                statusDiv.innerHTML += '<br><small>Refreshing page to reflect changes...</small>';

                // Refresh page to show updated state
                setTimeout(() => {
                    location.reload();
                }, 2000);
            }, 3000);
        } else {
            statusDiv.style.background = '#f8d7da';
            statusDiv.style.color = '#721c24';
            statusDiv.innerHTML = `<span style="display: inline-block; margin-right: 8px;">❌</span>Reset failed: ${data.message || data.error || 'Unknown error'}`;

            btn.disabled = false;
            btn.textContent = isPartial ? 'Reset Tasks 1-4' : 'Reset Everything';
        }
    })
    .catch(error => {
        console.error('Reset session error:', error);
        statusDiv.style.background = '#f8d7da';
        statusDiv.style.color = '#721c24';
        statusDiv.innerHTML = `<span style="display: inline-block; margin-right: 8px;">❌</span>Error: ${error.message}`;

        btn.disabled = false;
        btn.textContent = isPartial ? 'Reset Tasks 1-4' : 'Reset Everything';
    });
}
