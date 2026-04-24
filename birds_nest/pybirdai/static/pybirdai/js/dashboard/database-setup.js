// coding=UTF-8
// Copyright (c) 2025 Bird Software Solutions Ltd
// SPDX-License-Identifier: EPL-2.0

/**
 * Database Setup and Migration Functions
 * Handles: Database setup, migration, polling, server restart detection
 */

// Simple database setup starter function (available immediately)
function startDatabaseSetup() {
    console.log('startDatabaseSetup() called directly!');

    const btn = document.getElementById('database-setup-btn');
    const statusDiv = document.getElementById('database-setup-status');

    if (!btn) {
        console.error('Database setup button not found!');
        alert('Error: Database setup button not found!');
        return;
    }

    if (!statusDiv) {
        console.error('Database setup status div not found!');
        alert('Error: Database setup status div not found!');
        return;
    }

    console.log('Button found:', btn);
    console.log('Status div found:', statusDiv);

    // Get CSRF token
    const databaseSetupForm = document.getElementById('database-setup-form');
    let csrfToken = '';

    if (databaseSetupForm) {
        const csrfInput = databaseSetupForm.querySelector('[name=csrfmiddlewaretoken]');
        if (csrfInput) {
            csrfToken = csrfInput.value;
            console.log('CSRF token found:', csrfToken.substring(0, 10) + '...');
        } else {
            console.error('CSRF input not found in form!');
        }
    } else {
        console.error('Database setup form not found!');
    }

    if (!csrfToken) {
        alert('Error: CSRF token not found! Please refresh the page.');
        return;
    }

    // Start the actual database setup
    doDatabaseSetup(btn, statusDiv, csrfToken);
}

function doDatabaseSetup(btn, statusDiv, csrfToken) {
    console.log('Starting actual database setup...');

    // Update UI
    btn.disabled = true;
    statusDiv.style.display = 'block';
    statusDiv.style.background = '#e3f2fd';
    statusDiv.style.color = '#1976d2';
    statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Starting artefact retrieval and setup preparation...';

    // Create form data
    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', csrfToken);

    console.log('Sending AJAX request to /pybirdai/workflow/database-setup/');

    // Make AJAX request
    fetch('/pybirdai/workflow/database-setup/', {
        method: 'POST',
        body: formData,
        headers: {
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => {
        console.log('Database setup response received:', response.status, response.statusText);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        console.log('Database setup response data:', data);
        if (data.success && data.status === 'started') {
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Artefact retrieval started in background. Polling for status...';
            pollDatabaseSetupStatus(statusDiv, btn);
        } else {
            statusDiv.style.background = '#f8d7da';
            statusDiv.style.color = '#721c24';
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Failed to start: ' + (data.message || 'Unknown error');
            btn.disabled = false;
            btn.textContent = 'Retrieve Artifacts';
        }
    })
    .catch(error => {
        console.error('Database setup error:', error);
        statusDiv.style.background = '#f8d7da';
        statusDiv.style.color = '#721c24';
        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Error: ' + error.message;
        btn.disabled = false;
        btn.textContent = 'Retrieve Artifacts';
    });
}

// Simple migration starter function (available immediately)
function startMigration() {
    console.log('startMigration() called directly!');

    const btn = document.getElementById('migration-btn');
    const statusDiv = document.getElementById('migration-status');

    if (!btn) {
        console.error('Migration button not found!');
        alert('Error: Migration button not found!');
        return;
    }

    if (!statusDiv) {
        console.error('Migration status div not found!');
        alert('Error: Migration status div not found!');
        return;
    }

    console.log('Button found:', btn);
    console.log('Status div found:', statusDiv);

    // Get CSRF token
    const migrationForm = document.getElementById('migration-form');
    let csrfToken = '';

    if (migrationForm) {
        const csrfInput = migrationForm.querySelector('[name=csrfmiddlewaretoken]');
        if (csrfInput) {
            csrfToken = csrfInput.value;
            console.log('CSRF token found:', csrfToken.substring(0, 10) + '...');
        } else {
            console.error('CSRF input not found in form!');
        }
    } else {
        console.error('Migration form not found!');
    }

    if (!csrfToken) {
        alert('Error: CSRF token not found! Please refresh the page.');
        return;
    }

    // Start the actual migration
    doMigration(btn, statusDiv, csrfToken);
}

function doMigration(btn, statusDiv, csrfToken) {
    console.log('Starting actual migration...');

    // Update UI
    btn.disabled = true;
    statusDiv.style.display = 'block';
    statusDiv.style.background = '#e3f2fd';
    statusDiv.style.color = '#1976d2';
    statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Starting database setup...';

    // Create form data
    const formData = new FormData();
    formData.append('csrfmiddlewaretoken', csrfToken);

    console.log('Sending AJAX request to /pybirdai/workflow/setup-database-models/');

    // Make AJAX request
    fetch('/pybirdai/workflow/setup-database-models/', {
        method: 'POST',
        body: formData,
        headers: {
            'X-Requested-With': 'XMLHttpRequest'
        }
    })
    .then(response => {
        console.log('Migration response received:', response.status, response.statusText);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        return response.json();
    })
    .then(data => {
        console.log('Migration response data:', data);
        if (data.success && data.status === 'started') {
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Database setup started in background. Polling for status...';
            pollMigrationStatus(statusDiv, btn);
        } else {
            statusDiv.style.background = '#f8d7da';
            statusDiv.style.color = '#721c24';
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Failed to start: ' + (data.message || 'Unknown error');
            btn.disabled = false;
        }
    })
    .catch(error => {
        console.error('Migration error:', error);
        statusDiv.style.background = '#f8d7da';
        statusDiv.style.color = '#721c24';
        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Error: ' + error.message;
        btn.disabled = false;
    });
}

// Function to poll database setup status
function pollDatabaseSetupStatus(statusDiv, btn) {
    const pollInterval = 2000; // Poll every 2 seconds
    let pollCount = 0;
    const maxPolls = 300; // Maximum 10 minutes (300 * 2 seconds)

    function checkStatus() {
        fetch('/pybirdai/workflow/database-setup-status/', {
            method: 'GET',
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => response.json())
        .then(data => {
            console.log('Database setup status:', data);

            if (data.success && data.database_setup_status) {
                const status = data.database_setup_status;

                if (status.running) {
                    // Still running - update status message
                    const elapsedTime = Math.round(status.elapsed_time || 0);
                    let message = `<span style="display: inline-block; margin-right: 8px;">⏳</span>${status.message || 'Artefact Retrieval Running...'} (${elapsedTime}s elapsed)`;
                    if (status.current_task) {
                        message += ` - Processing`;
                    }
                    statusDiv.innerHTML = message;

                    // Continue polling
                    pollCount++;
                    if (pollCount < maxPolls) {
                        setTimeout(checkStatus, pollInterval);
                    } else {
                        // Timeout
                        statusDiv.style.background = '#fff3cd';
                        statusDiv.style.color = '#856404';
                        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⚠️</span>Database setup is taking longer than expected. Please check server logs.';
                        btn.disabled = false;
                    }
                } else if (status.completed) {
                    // Setup completed
                    console.log('Database setup completed! Status object:', status);

                    if (status.success) {
                        statusDiv.style.background = '#d4edda';
                        statusDiv.style.color = '#155724';
                        const elapsedTime = Math.round(status.elapsed_time || 0);
                        let message = `<span style="display: inline-block; margin-right: 8px;">✅</span>${status.message || 'Database setup completed successfully'} (${elapsedTime}s)`;

                        if (status.completed_tasks && status.completed_tasks.length > 0) {
                            message += `<br>Completed: ${status.completed_tasks.join(', ')}`;
                        }

                        // Check for server restart requirement
                        const serverRestartRequired = status.server_restart_required === true;
                        const messageIndicatesRestart = status.message && (
                            status.message.includes('restart required') ||
                            status.message.includes('Server restart required') ||
                            status.message.includes('server restart')
                        );

                        console.log('Restart check:', {
                            serverRestartRequired,
                            messageIndicatesRestart,
                            message: status.message,
                            hasRestartInfo: !!status.restart_info,
                            hasEstimatedTime: !!status.estimated_restart_time
                        });

                        if (serverRestartRequired || messageIndicatesRestart) {
                            console.log('🔄 Database setup completed - server restart required detected!');
                            message += '<br><strong>🔄 Server restart in progress...</strong>';
                            if (status.restart_info) {
                                message += `<br><small>${status.restart_info}</small>`;
                            } else if (status.estimated_restart_time) {
                                message += `<br><small>${status.estimated_restart_time}</small>`;
                            }
                            statusDiv.innerHTML = message;

                            // Handle server restart scenario
                            handleServerRestart(statusDiv, btn);
                        } else {
                            console.log('✅ Database setup completed - no server restart required');
                            statusDiv.innerHTML = message;

                            // Regular refresh for non-restart scenarios
                            setTimeout(() => {
                                console.log('Refreshing page after successful setup (no restart)');
                                location.reload();
                            }, 3000);
                        }
                    } else {
                        statusDiv.style.background = '#f8d7da';
                        statusDiv.style.color = '#721c24';
                        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Database setup failed: ' + (status.error || status.message || 'Unknown error');

                        btn.disabled = false;
                    }
                }
            } else {
                // Error getting status
                statusDiv.style.background = '#f8d7da';
                statusDiv.style.color = '#721c24';
                statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Error checking database setup status';

                btn.disabled = false;
            }
        })
        .catch(error => {
            console.error('Database setup status polling error:', error);

            // Check if this might be a server restart scenario
            if (error.name === 'TypeError' || error.message.includes('Failed to fetch') || error.message.includes('NetworkError') || error.name === 'TimeoutError') {
                console.log('🔄 Connection lost during database setup polling - likely server restart!');
                statusDiv.style.background = '#e3f2fd';
                statusDiv.style.color = '#1976d2';
                statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">🔄</span>✅ Database setup completed! Connection lost - server is restarting...';

                // Switch to restart detection mode
                handleServerRestart(statusDiv, btn);
            } else {
                // Other error - refresh page to reset state
                setTimeout(() => {
                    console.log('Refreshing page due to database setup polling error');
                    location.reload();
                }, 2000);
            }
        });
    }

    // Start polling
    setTimeout(checkStatus, pollInterval);
}

// Function to poll migration status (Step 2b: Setup Database Models)
function pollMigrationStatus(statusDiv, btn) {
    const pollInterval = 2000; // Poll every 2 seconds
    let pollCount = 0;
    const maxPolls = 450; // Maximum 15 minutes (450 * 2 seconds)

    function checkStatus() {
        fetch('/pybirdai/workflow/setup-database-models-status/', {
            method: 'GET',
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => response.json())
        .then(data => {
            console.log('Migration status:', data);

            if (data.success && data.migration_status) {
                const status = data.migration_status;

                if (status.running) {
                    // Still running - update status message
                    const elapsedTime = Math.round(status.elapsed_time || 0);
                    statusDiv.innerHTML = `<span style="display: inline-block; margin-right: 8px;">⏳</span>Database setup running in background... (${elapsedTime}s elapsed)`;

                    // Continue polling
                    pollCount++;
                    if (pollCount < maxPolls) {
                        setTimeout(checkStatus, pollInterval);
                    } else {
                        // Timeout
                        statusDiv.style.background = '#fff3cd';
                        statusDiv.style.color = '#856404';
                        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⚠️</span>Database setup is taking longer than expected. Please check server logs.';
                        btn.disabled = false;
                    }
                } else if (status.completed) {
                    // Migration completed
                    console.log('Migration completed! Status object:', status);

                    if (status.success) {
                        statusDiv.style.background = '#d4edda';
                        statusDiv.style.color = '#155724';
                        const elapsedTime = Math.round(status.elapsed_time || 0);
                        let message = `<span style="display: inline-block; margin-right: 8px;">✅</span>${status.message || 'Database migrations completed successfully'} (${elapsedTime}s)`;

                        // Check for server restart requirement
                        const serverRestartRequired = status.server_restart_required === true;
                        const messageIndicatesRestart = status.message && (
                            status.message.includes('restart required') ||
                            status.message.includes('Server restart required') ||
                            status.message.includes('server restart')
                        );

                        console.log('Migration restart check:', {
                            serverRestartRequired,
                            messageIndicatesRestart,
                            message: status.message
                        });

                        if (serverRestartRequired || messageIndicatesRestart) {
                            console.log('🔄 Database setup completed - server restart required detected!');
                            message += '<br><strong>🔄 Server restart in progress...</strong>';
                            if (status.restart_info) {
                                message += `<br><small>${status.restart_info}</small>`;
                            } else if (status.estimated_restart_time) {
                                message += `<br><small>${status.estimated_restart_time}</small>`;
                            }
                            statusDiv.innerHTML = message;

                            // Handle server restart scenario
                            handleServerRestart(statusDiv, btn);
                        } else {
                            console.log('✅ Database setup completed - no server restart required');
                            statusDiv.innerHTML = message + '<br>The database is now ready for use.';

                            // Regular refresh for non-restart scenarios
                            setTimeout(() => {
                                console.log('Refreshing page after successful migration (no restart)');
                                location.reload();
                            }, 3000);
                        }
                    } else {
                        statusDiv.style.background = '#f8d7da';
                        statusDiv.style.color = '#721c24';
                        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Migration failed: ' + (status.error || status.message || 'Unknown error');

                        btn.disabled = false;
                    }
                }
            } else {
                // Error getting status
                statusDiv.style.background = '#f8d7da';
                statusDiv.style.color = '#721c24';
                statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Error checking migration status';

                btn.disabled = false;
            }
        })
        .catch(error => {
            console.error('Migration status polling error:', error);

            // Check if this might be a server restart scenario
            if (error.name === 'TypeError' || error.message.includes('Failed to fetch') || error.message.includes('NetworkError') || error.name === 'TimeoutError') {
                console.log('🔄 Connection lost during migration polling - likely server restart!');
                statusDiv.style.background = '#e3f2fd';
                statusDiv.style.color = '#1976d2';
                statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">🔄</span>✅ Database setup completed! Connection lost - server is restarting...';

                // Switch to restart detection mode
                handleServerRestart(statusDiv, btn);
            } else {
                // Other error - refresh page to reset state
                setTimeout(() => {
                    console.log('Refreshing page due to migration polling error');
                    location.reload();
                }, 2000);
            }
        });
    }

    // Start polling
    setTimeout(checkStatus, pollInterval);
}

// Function to poll automode status
function pollAutomodeStatus(statusDiv, btn, targetTask) {
    const pollInterval = 2000; // Poll every 2 seconds
    let pollCount = 0;
    const maxPolls = 600; // Maximum 20 minutes (600 * 2 seconds) for automode

    function checkStatus() {
        fetch('/pybirdai/workflow/automode-status/', {
            method: 'GET',
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => response.json())
        .then(data => {
            console.log('Automode status:', data);

            if (data.success && data.automode_status) {
                const status = data.automode_status;

                if (status.running) {
                    // Still running - update status message
                    const elapsedTime = Math.round(status.elapsed_time || 0);
                    let message = `<span style="display: inline-block; margin-right: 8px;">⏳</span>${status.message || 'Automode running...'} (${elapsedTime}s elapsed)`;

                    if (status.current_task) {
                        message += ` - Currently on Task ${status.current_task}`;
                        if (status.target_task) {
                            message += ` of ${status.target_task}`;
                        }
                    }

                    if (status.completed_tasks && status.completed_tasks.length > 0) {
                        message += `<br>Completed: ${status.completed_tasks.join(', ')}`;
                    }

                    statusDiv.innerHTML = message;

                    // Continue polling
                    pollCount++;
                    if (pollCount < maxPolls) {
                        setTimeout(checkStatus, pollInterval);
                    } else {
                        // Timeout
                        statusDiv.style.background = '#fff3cd';
                        statusDiv.style.color = '#856404';
                        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⚠️</span>Automode is taking longer than expected. Please check server logs.';
                        btn.disabled = false;
                        btn.textContent = 'Run Automode (from Task 1)';
                    }
                } else if (status.completed) {
                    // Automode completed
                    if (status.success) {
                        statusDiv.style.background = '#d4edda';
                        statusDiv.style.color = '#155724';
                        const elapsedTime = Math.round(status.elapsed_time || 0);
                        statusDiv.innerHTML = `<span style="display: inline-block; margin-right: 8px;">✅</span>${status.message || 'Automode completed successfully'} (${elapsedTime}s)`;

                        // Refresh page after success to show updated task status
                        setTimeout(() => {
                            location.reload();
                        }, 3000);
                    } else {
                        statusDiv.style.background = '#f8d7da';
                        statusDiv.style.color = '#721c24';
                        let errorMsg = status.message || 'Automode failed';

                        if (status.task_errors && status.task_errors.length > 0) {
                            errorMsg += '<br>Errors: ' + status.task_errors.map(e => `Task ${e.task}: ${e.error}`).join('<br>');
                        }

                        statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>' + errorMsg;

                        btn.disabled = false;
                        btn.textContent = 'Run Automode (from Task 1)';
                    }
                }
            } else {
                // Error getting status
                statusDiv.style.background = '#f8d7da';
                statusDiv.style.color = '#721c24';
                statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Error checking automode status';

                btn.disabled = false;
                btn.textContent = 'Run Automode (from Task 1)';
            }
        }).catch(error => {
            console.error('Automode status polling error:', error);

            // Refresh page on error to reset state
            setTimeout(() => {
                console.log('Refreshing page due to automode polling error');
                location.reload();
            }, 2000);
        });
    }

    // Start polling
    setTimeout(checkStatus, pollInterval);
}

// Function to handle server restart detection and UI refresh
function handleServerRestart(statusDiv, btn) {
    console.log('handleServerRestart() called - starting server restart detection');

    // Show the overlay
    const overlay = document.getElementById('server-restart-overlay');
    if (overlay) {
        overlay.style.display = 'flex';
    }

    // Update UI immediately to show restart is happening
    statusDiv.style.background = '#e3f2fd';
    statusDiv.style.color = '#1976d2';
    statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">🔄</span>✅ Database setup completed! Server is restarting due to file changes...';

    // Disable button immediately to prevent double-clicks during restart
    btn.disabled = true;
    btn.textContent = 'Server Restarting...';

    const restartPollInterval = 5000; // Poll every 5 seconds
    let restartPollCount = 0;
    const maxRestartPolls = 60; // Maximum 5 minutes (60 * 5 seconds)
    let serverWentDown = false;
    let timerInterval = null;

    // Start timer for overlay
    let elapsedSeconds = 0;
    timerInterval = setInterval(() => {
        elapsedSeconds++;
        const timerElement = document.getElementById('overlay-timer-seconds');
        if (timerElement) {
            timerElement.textContent = elapsedSeconds;
        }
    }, 1000);

    function updateRestartMessage(seconds) {
        const minutes = Math.floor(seconds / 60);
        const remainingSeconds = seconds % 60;
        const timeStr = minutes > 0 ? `${minutes}m ${remainingSeconds}s` : `${seconds}s`;

        if (!serverWentDown) {
            statusDiv.innerHTML = `<span style="display: inline-block; margin-right: 8px;">🔄</span>✅ Database setup completed! Waiting for server restart... (${timeStr})<br><small>After restart, button will change to "Setup Database"</small>`;
        } else {
            statusDiv.innerHTML = `<span style="display: inline-block; margin-right: 8px;">🔄</span>Server is restarting... (${timeStr})<br><small>Page will refresh to show "Setup Database" button</small>`;
        }
    }

    function checkServerStatus() {
        restartPollCount++;
        const pollElapsedSeconds = Math.floor(restartPollCount * 5);

        // Update message with elapsed time
        updateRestartMessage(pollElapsedSeconds);

        // Try to ping the server
        fetch('/pybirdai/workflow/', {
            method: 'GET',
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            },
            signal: AbortSignal.timeout(4000) // 4 second timeout
        })
        .then(response => {
            if (response.ok) {
                // Server is back online
                console.log('Server restart completed - server is back online');

                // Clear timer
                if (timerInterval) {
                    clearInterval(timerInterval);
                }

                // Hide overlay
                if (overlay) {
                    overlay.style.display = 'none';
                }

                statusDiv.style.background = '#d4edda';
                statusDiv.style.color = '#155724';
                statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">✅</span>🎉 Server restart completed! Refreshing page to show "Setup Database" button...';

                // Disable the database setup button immediately to prevent double-clicks
                btn.disabled = true;
                btn.textContent = 'Refreshing...';

                // Refresh page after delay to ensure server is fully ready
                setTimeout(() => {
                    console.log('Refreshing page after server restart to show migration button');
                    location.reload();
                }, 2000);
            } else {
                // Server still restarting or responding with non-200 status
                if (restartPollCount < maxRestartPolls) {
                    setTimeout(checkServerStatus, restartPollInterval);
                } else {
                    // Timeout
                    if (timerInterval) {
                        clearInterval(timerInterval);
                    }
                    if (overlay) {
                        overlay.style.display = 'none';
                    }
                    statusDiv.style.background = '#fff3cd';
                    statusDiv.style.color = '#856404';
                    statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⚠️</span>Server restart is taking longer than expected. Please refresh the page manually.';
                    btn.disabled = false;
                    btn.textContent = 'Retrieve Artifacts and Setup Database';
                }
            }
        })
        .catch(error => {
            // Server is likely still restarting (connection refused/network error)
            if (!serverWentDown) {
                serverWentDown = true;
                console.log('Server restart detected - connection lost:', error.name);
            }

            if (restartPollCount < maxRestartPolls) {
                setTimeout(checkServerStatus, restartPollInterval);
            } else {
                // Timeout
                if (timerInterval) {
                    clearInterval(timerInterval);
                }
                if (overlay) {
                    overlay.style.display = 'none';
                }
                statusDiv.style.background = '#fff3cd';
                statusDiv.style.color = '#856404';
                statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⚠️</span>Server restart is taking longer than expected. Please refresh the page manually or check the server console.';
                btn.disabled = false;
                btn.textContent = 'Retrieve Artifacts and Setup Database';
            }
        });
    }

    // Start checking server status after initial delay
    console.log('Starting server restart detection polling...');
    setTimeout(checkServerStatus, 10000); // Check after 10 seconds

    // Fallback: force page refresh after 5 minutes if restart detection fails
    setTimeout(() => {
        if (btn.disabled && btn.textContent.includes('Restarting')) {
            console.log('Fallback: Force refreshing page after restart timeout');
            if (timerInterval) {
                clearInterval(timerInterval);
            }
            if (overlay) {
                overlay.style.display = 'none';
            }
            statusDiv.innerHTML = '<span style="display: inline-block; margin-right: 8px;">🔄</span>Restart taking longer than expected - refreshing page...';
            location.reload();
        }
    }, 300000); // 5 minutes fallback
}

// Initialize automode form handler on DOMContentLoaded
document.addEventListener('DOMContentLoaded', function() {
    // Automode form AJAX handler
    const automodeForm = document.getElementById('automode-form');
    const automodeBtn = document.getElementById('automode-btn');
    const automodeStatus = document.getElementById('automode-status');

    if (automodeForm && automodeBtn && automodeStatus) {
        automodeForm.addEventListener('submit', function(e) {
            e.preventDefault();

            const targetTask = automodeForm.querySelector('[name="target_task"]').value;

            // Disable button and show loading
            automodeBtn.disabled = true;
            automodeBtn.textContent = `Running automode to Task ${targetTask}...`;
            automodeStatus.style.display = 'block';
            automodeStatus.style.background = '#e3f2fd';
            automodeStatus.style.color = '#1976d2';
            automodeStatus.innerHTML = `<span style="display: inline-block; margin-right: 8px;">⏳</span>Running automode from Task 1 to Task ${targetTask}...`;

            // Get form data
            const formData = new FormData(automodeForm);

            // Make AJAX request
            fetch('/pybirdai/workflow/automode/', {
                method: 'POST',
                body: formData,
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                }
            })
            .then(response => response.json())
            .then(data => {
                if (data.success && data.status === 'completed') {
                    // Synchronous completion - show success immediately
                    automodeStatus.style.background = '#d4edda';
                    automodeStatus.style.color = '#155724';
                    automodeStatus.innerHTML = '<span style="display: inline-block; margin-right: 8px;">✅</span>' + data.message;

                    automodeBtn.disabled = false;
                    automodeBtn.textContent = 'Run Automode (from Task 1)';
                } else if (data.success && data.status === 'started') {
                    // Async mode - poll for status (legacy)
                    automodeStatus.style.background = '#e3f2fd';
                    automodeStatus.style.color = '#1976d2';
                    automodeStatus.innerHTML = '<span style="display: inline-block; margin-right: 8px;">⏳</span>Automode started in background. Polling for status...';

                    // Start polling for status
                    pollAutomodeStatus(automodeStatus, automodeBtn, targetTask);
                } else {
                    automodeStatus.style.background = '#f8d7da';
                    automodeStatus.style.color = '#721c24';
                    let errorMsg = 'Automode failed';
                    if (data.message) {
                        errorMsg = data.message;
                    } else if (data.errors && data.errors.length > 0) {
                        errorMsg += ': ' + data.errors.map(e => `Task ${e.task}: ${e.error}`).join(', ');
                    }
                    automodeStatus.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>' + errorMsg;

                    automodeBtn.disabled = false;
                    automodeBtn.textContent = 'Run Automode (from Task 1)';
                }
            })
            .catch(error => {
                console.error('Automode error:', error);
                automodeStatus.style.background = '#f8d7da';
                automodeStatus.style.color = '#721c24';
                automodeStatus.innerHTML = '<span style="display: inline-block; margin-right: 8px;">❌</span>Error: ' + error.message;

                automodeBtn.disabled = false;
                automodeBtn.textContent = 'Run Automode (from Task 1)';
            });
        });
    }
});
