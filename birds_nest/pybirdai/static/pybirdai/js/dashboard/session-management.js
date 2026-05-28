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
    btn.textContent = 'Cloning...';
    statusDiv.style.display = 'block';
    statusDiv.style.background = '#e3f2fd';
    statusDiv.style.color = '#1976d2';
    statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Starting clone import...';

    // Create form data
    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', csrfToken);

    // Start clone import in the background, then poll for progress.
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
            return response.json()
                .catch(() => ({}))
                .then(data => {
                    throw new Error(data.message || data.error || `HTTP ${response.status}: ${response.statusText}`);
                });
        }
        return response.json();
    })
    .then(data => {
        console.log('Clone import response data:', data);
        if (data.success && data.status === 'started') {
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Clone import running in background... (0s elapsed)';
            pollCloneImportStatus(statusDiv, btn);
        } else if (data.status === 'already_running') {
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Clone import is already running...';
            pollCloneImportStatus(statusDiv, btn);
        } else {
            statusDiv.style.background = '#f8d7da';
            statusDiv.style.color = '#721c24';
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Clone import failed: ' + (data.error || data.message || 'Unknown error');

            btn.disabled = false;
            btn.textContent = 'Clone';
        }
    })
    .catch(error => {
        console.error('Clone import error:', error);
        statusDiv.style.background = '#f8d7da';
        statusDiv.style.color = '#721c24';
        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Error: ' + error.message;

        btn.disabled = false;
        btn.textContent = 'Clone';
    });
}

// Function to poll clone import status
function pollCloneImportStatus(statusDiv, btn) {
    const pollInterval = 2000; // Poll every 2 seconds
    let pollCount = 0;
    const maxPolls = 900; // Maximum 30 minutes (900 * 2 seconds)

    function checkStatus() {
        fetch('/pybirdai/workflow/clone-import-status/', {
            method: 'GET',
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => response.json())
        .then(data => {
            console.log('Clone import status:', data);

            if (data.success && data.clone_import_status) {
                const status = data.clone_import_status;
                const elapsedTime = Math.round(status.elapsed_time || 0);

                if (status.running) {
                    statusDiv.style.background = '#e3f2fd';
                    statusDiv.style.color = '#1976d2';

                    let message = `<span style="display: inline-block; margin-right: 8px;">⏳</span>${status.message || 'Clone import running...'} (${elapsedTime}s elapsed)`;
                    if (status.completed_steps && status.completed_steps.length > 0) {
                        message += `<br><small>Completed: ${status.completed_steps.join(', ')}</small>`;
                    }
                    statusDiv.innerHTML = message;

                    pollCount++;
                    if (pollCount < maxPolls) {
                        setTimeout(checkStatus, pollInterval);
                    } else {
                        statusDiv.style.background = '#fff3cd';
                        statusDiv.style.color = '#856404';
                        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⚠️</span>Clone import is taking longer than expected. Please check server logs.';
                        btn.disabled = false;
                        btn.textContent = 'Clone';
                    }
                } else if (status.completed) {
                    const result = status.result || {};

                    if (status.success) {
                        statusDiv.style.background = '#d4edda';
                        statusDiv.style.color = '#155724';

                        let message = `<span style="display: inline-block; margin-right: 8px;">✅</span>${result.message || status.message || 'Clone import completed successfully'} (${elapsedTime}s)`;
                        if (result.details) {
                            message += `<br><small>${result.details}</small>`;
                        }
                        statusDiv.innerHTML = message;

                        setTimeout(() => {
                            btn.disabled = false;
                            btn.textContent = 'Clone';

                            if (result.refresh_recommended) {
                                setTimeout(() => {
                                    location.reload();
                                }, 2000);
                            }
                        }, 3000);
                    } else {
                        statusDiv.style.background = '#f8d7da';
                        statusDiv.style.color = '#721c24';

                        let errorMessage = result.error || status.error || result.message || status.message || 'Unknown error';
                        if (result.details && typeof result.details === 'string') {
                            errorMessage += `<br><small>${result.details}</small>`;
                        }

                        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Clone import failed: ' + errorMessage;
                        btn.disabled = false;
                        btn.textContent = 'Clone';
                    }
                } else {
                    pollCount++;
                    if (pollCount < maxPolls) {
                        setTimeout(checkStatus, pollInterval);
                    } else {
                        statusDiv.style.background = '#fff3cd';
                        statusDiv.style.color = '#856404';
                        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⚠️</span>Clone import status did not start. Please check server logs.';
                        btn.disabled = false;
                        btn.textContent = 'Clone';
                    }
                }
            } else {
                statusDiv.style.background = '#f8d7da';
                statusDiv.style.color = '#721c24';
                statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Error checking clone import status';

                btn.disabled = false;
                btn.textContent = 'Clone';
            }
        })
        .catch(error => {
            console.error('Clone import status polling error:', error);

            pollCount++;
            if (pollCount < maxPolls) {
                statusDiv.style.background = '#fff3cd';
                statusDiv.style.color = '#856404';
                statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⚠️</span>Clone status check failed; retrying...';
                setTimeout(checkStatus, pollInterval);
            } else {
                statusDiv.style.background = '#f8d7da';
                statusDiv.style.color = '#721c24';
                statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Error checking clone import status: ' + error.message;

                btn.disabled = false;
                btn.textContent = 'Clone';
            }
        });
    }

    setTimeout(checkStatus, pollInterval);
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
