// coding=UTF-8
// Copyright (c) 2025 Arfa Digital Consulting
// SPDX-License-Identifier: EPL-2.0

/**
 * Clone Mode Functions
 * Handles: Save/Load state to local directory or GitHub repository
 *
 * RESTRICTED MODE:
 * - Save: Only to user's pybirdai_workplace repo (personal or org)
 * - Load: From user's workspace OR default regcommunity repos
 */

const cloneMode = {
    // Cache for save targets and load sources
    _saveTargets: null,
    _loadSources: null,

    /**
     * Show status message in the modal
     */
    showStatus: function(message, type = 'info', showSpinner = false, details = null) {
        const statusDiv = document.getElementById('cloneModeStatus');
        const alertDiv = document.getElementById('cloneModeAlert');
        const spinnerDiv = document.getElementById('cloneModeSpinner');
        const messageSpan = document.getElementById('cloneModeMessage');
        const detailsDiv = document.getElementById('cloneModeDetails');

        if (!statusDiv || !alertDiv || !messageSpan) {
            console.error('Clone mode status elements not found');
            return;
        }

        // Show status area
        statusDiv.style.display = 'block';

        // Set alert type
        alertDiv.className = 'alert';
        switch (type) {
            case 'success':
                alertDiv.classList.add('alert-success');
                break;
            case 'error':
                alertDiv.classList.add('alert-danger');
                break;
            case 'warning':
                alertDiv.classList.add('alert-warning');
                break;
            default:
                alertDiv.classList.add('alert-info');
        }

        // Show/hide spinner
        if (spinnerDiv) {
            spinnerDiv.style.display = showSpinner ? 'inline-block' : 'none';
        }

        // Set message
        messageSpan.textContent = message;

        // Set details
        if (detailsDiv) {
            if (details) {
                detailsDiv.style.display = 'block';
                detailsDiv.innerHTML = details;
            } else {
                detailsDiv.style.display = 'none';
            }
        }
    },

    /**
     * Hide status message
     */
    hideStatus: function() {
        const statusDiv = document.getElementById('cloneModeStatus');
        if (statusDiv) {
            statusDiv.style.display = 'none';
        }
    },

    /**
     * Format clone state summary for display in modal
     * @param {Object} summary - The clone_state_summary object from backend
     * @returns {string} - HTML formatted summary
     */
    formatCloneStateSummary: function(summary) {
        if (!summary || typeof summary !== 'object') {
            return '';
        }

        const workflows = summary.workflows || {};
        const completedTests = summary.completed_tests || [];
        const exportStatus = summary.export_status || 'UNKNOWN';
        const lastStep = summary.last_step_completed || null;
        const nextUrl = summary.next_url || '/pybirdai/workflow/dashboard/';

        // Build test info string
        let testInfo = '';
        if (completedTests.length > 0) {
            testInfo = `This export has passed tests for: ${completedTests.join(', ')}`;
        } else {
            testInfo = 'No test workflows have been completed yet';
        }

        // Build workflow status lines - show all workflows
        let workflowLines = [];

        // MAIN workflow
        const main = workflows.main || {};
        const mainStatus = main.is_complete ? 'COMPLETE' : (main.status || 'Not started');
        const mainProgress = main.progress || '0/4';
        workflowLines.push(`MAIN: ${mainStatus} (${mainProgress} steps)`);

        // DPM workflow
        const dpm = workflows.dpm || {};
        const dpmStatus = dpm.is_complete ? 'COMPLETE' : (dpm.status || 'Not started');
        const sourceType = dpm.source_type || 'eba';
        workflowLines.push(`DPM (${sourceType}): ${dpmStatus}`);

        // ANACREDIT workflow
        const anacredit = workflows.anacredit || {};
        const anacreditStatus = anacredit.is_complete ? 'COMPLETE' : (anacredit.status || 'Not started');
        const anacreditProgress = anacredit.progress || '0/4';
        workflowLines.push(`ANACREDIT: ${anacreditStatus} (${anacreditProgress} steps)`);

        // Status color
        const statusColor = exportStatus === 'COMPLETE' ? '#28a745' : '#ffc107';

        // Build the full HTML
        const html = `
            <div class="clone-state-summary" style="font-family: monospace; font-size: 0.85em; background: #f8f9fa; border-radius: 4px; padding: 12px; margin-top: 10px;">
                <div style="text-align: center; font-weight: bold; border-bottom: 2px solid #dee2e6; padding-bottom: 8px; margin-bottom: 8px;">
                    CLONE STATE LOADED SUCCESSFULLY
                </div>
                <div style="margin-bottom: 8px;">
                    <strong>Export Status:</strong> <span style="color: ${statusColor}; font-weight: bold;">${exportStatus}</span><br>
                    <span style="color: #6c757d; font-size: 0.9em;">&nbsp;&nbsp;${testInfo}</span>
                </div>
                ${lastStep ? `
                <div style="margin-bottom: 8px;">
                    <strong>Last Completed Step:</strong> ${lastStep}
                </div>
                ` : ''}
                <div style="margin-bottom: 8px;">
                    ${workflowLines.map(l => `<span style="color: #495057;">&nbsp;&nbsp;${l}</span>`).join('<br>')}
                </div>
                <div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid #dee2e6;">
                    <strong>To continue, navigate to:</strong><br>
                    <a href="${nextUrl}" style="color: #007bff;">&nbsp;&nbsp;${nextUrl}</a>
                </div>
            </div>
        `;

        return html;
    },

    /**
     * Get CSRF token from the page
     */
    getCSRFToken: function() {
        const csrfInput = document.querySelector('[name=csrfmiddlewaretoken]');
        if (csrfInput) {
            return csrfInput.value;
        }
        // Try to get from cookie
        const cookies = document.cookie.split(';');
        for (let cookie of cookies) {
            const [name, value] = cookie.trim().split('=');
            if (name === 'csrftoken') {
                return value;
            }
        }
        return null;
    },

    /**
     * Disable/enable buttons during operations
     */
    setButtonsDisabled: function(disabled) {
        const buttons = [
            'saveLocalBtn',
            'saveGithubBtn',
            'loadLocalBtn',
            'loadGithubBtn'
        ];
        buttons.forEach(btnId => {
            const btn = document.getElementById(btnId);
            if (btn) {
                btn.disabled = disabled;
            }
        });
    },

    /**
     * Fetch allowed save targets from the server
     * @param {string} token - GitHub token
     * @returns {Promise<Object>} Save targets result
     */
    fetchSaveTargets: async function(token) {
        const csrfToken = this.getCSRFToken();

        const formData = new FormData();
        formData.append('csrfmiddlewaretoken', csrfToken);
        formData.append('token', token);

        const response = await fetch('/pybirdai/workflow/clone/get-save-targets/', {
            method: 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        });

        return await response.json();
    },

    /**
     * Fetch allowed load sources from the server
     * @param {string} token - GitHub token (optional)
     * @returns {Promise<Object>} Load sources result
     */
    fetchLoadSources: async function(token) {
        const csrfToken = this.getCSRFToken();

        const formData = new FormData();
        formData.append('csrfmiddlewaretoken', csrfToken);
        if (token) {
            formData.append('token', token);
        }

        const response = await fetch('/pybirdai/workflow/clone/get-load-sources/', {
            method: 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        });

        return await response.json();
    },

    /**
     * Populate the save target dropdown
     * @param {Array} targets - List of allowed save targets
     */
    populateSaveTargets: function(targets) {
        const select = document.getElementById('saveTargetSelect');
        const saveBtn = document.getElementById('saveGithubBtn');
        if (!select) return;

        // Clear existing options
        select.innerHTML = '';

        if (!targets || targets.length === 0) {
            select.innerHTML = '<option value="">-- No save locations available --</option>';
            select.disabled = true;
            if (saveBtn) saveBtn.disabled = true;
            return;
        }

        // Add placeholder
        const placeholder = document.createElement('option');
        placeholder.value = '';
        placeholder.textContent = '-- Select save location --';
        select.appendChild(placeholder);

        // Add targets
        targets.forEach(target => {
            const option = document.createElement('option');
            option.value = target.name; // Use owner name, not full URL
            option.textContent = target.display_name;
            option.dataset.type = target.type;
            option.dataset.repoUrl = target.repo_url;
            select.appendChild(option);
        });

        // Enable the select
        select.disabled = false;

        // Auto-select if only one option
        if (targets.length === 1) {
            select.value = targets[0].name;
            if (saveBtn) saveBtn.disabled = false;
        }

        // Enable save button when selection changes
        select.addEventListener('change', function() {
            if (saveBtn) {
                saveBtn.disabled = !this.value;
            }
        });
    },

    /**
     * Populate the load source dropdown
     * @param {Array} sources - List of allowed load sources
     */
    populateLoadSources: function(sources) {
        const select = document.getElementById('loadSourceSelect');
        if (!select) return;

        // Clear existing options but keep the structure
        select.innerHTML = '';

        // Add placeholder
        const placeholder = document.createElement('option');
        placeholder.value = '';
        placeholder.textContent = '-- Select a source --';
        select.appendChild(placeholder);

        // Group sources by type
        const userSources = sources.filter(s => s.type === 'user' || s.type === 'organization');
        const defaultSources = sources.filter(s => s.type === 'default');

        // Add user/org sources if any
        if (userSources.length > 0) {
            const userGroup = document.createElement('optgroup');
            userGroup.label = 'Your Workspaces';
            userSources.forEach(source => {
                const option = document.createElement('option');
                option.value = source.repo_url;
                option.textContent = source.name;
                option.dataset.type = source.type;
                userGroup.appendChild(option);
            });
            select.appendChild(userGroup);
        }

        // Add default sources
        if (defaultSources.length > 0) {
            const defaultGroup = document.createElement('optgroup');
            defaultGroup.label = 'Default Repositories';
            defaultSources.forEach(source => {
                const option = document.createElement('option');
                option.value = source.repo_url;
                option.textContent = source.name;
                option.dataset.type = source.type;
                defaultGroup.appendChild(option);
            });
            select.appendChild(defaultGroup);
        }
    },

    /**
     * Handle token input change for save targets
     * Debounced to avoid excessive API calls
     */
    _saveTokenTimeout: null,
    onSaveTokenInput: async function(token) {
        // Clear previous timeout
        if (this._saveTokenTimeout) {
            clearTimeout(this._saveTokenTimeout);
        }

        const select = document.getElementById('saveTargetSelect');
        const loadingDiv = document.getElementById('saveTargetsLoading');
        const saveBtn = document.getElementById('saveGithubBtn');

        if (!token || token.trim().length < 10) {
            // Reset to disabled state
            if (select) {
                select.innerHTML = '<option value="">-- Enter token to load options --</option>';
                select.disabled = true;
            }
            if (saveBtn) saveBtn.disabled = true;
            return;
        }

        // Debounce: wait 500ms after user stops typing
        this._saveTokenTimeout = setTimeout(async () => {
            try {
                // Show loading state
                if (loadingDiv) loadingDiv.style.display = 'block';
                if (select) select.disabled = true;

                const result = await this.fetchSaveTargets(token.trim());

                if (loadingDiv) loadingDiv.style.display = 'none';

                if (result.success) {
                    this._saveTargets = result.targets;
                    this.populateSaveTargets(result.targets);
                } else {
                    if (select) {
                        select.innerHTML = `<option value="">Error: ${result.error || 'Failed to load'}</option>`;
                        select.disabled = true;
                    }
                    if (saveBtn) saveBtn.disabled = true;
                }
            } catch (error) {
                console.error('Error fetching save targets:', error);
                if (loadingDiv) loadingDiv.style.display = 'none';
                if (select) {
                    select.innerHTML = '<option value="">Error loading options</option>';
                    select.disabled = true;
                }
            }
        }, 500);
    },

    /**
     * Handle token input change for load sources
     * Debounced to avoid excessive API calls
     */
    _loadTokenTimeout: null,
    onLoadTokenInput: async function(token) {
        // Clear previous timeout
        if (this._loadTokenTimeout) {
            clearTimeout(this._loadTokenTimeout);
        }

        const loadingDiv = document.getElementById('loadSourcesLoading');

        // Debounce: wait 500ms after user stops typing
        this._loadTokenTimeout = setTimeout(async () => {
            try {
                // Show loading state
                if (loadingDiv) loadingDiv.style.display = 'block';

                const result = await this.fetchLoadSources(token ? token.trim() : null);

                if (loadingDiv) loadingDiv.style.display = 'none';

                if (result.success) {
                    this._loadSources = result.sources;
                    this.populateLoadSources(result.sources);
                }
            } catch (error) {
                console.error('Error fetching load sources:', error);
                if (loadingDiv) loadingDiv.style.display = 'none';
            }
        }, 500);
    },

    /**
     * Show a confirmation dialog
     * @param {string} title - Dialog title
     * @param {string} message - Dialog message
     * @param {string} confirmText - Confirm button text
     * @param {string} cancelText - Cancel button text
     * @returns {Promise<boolean>} User's choice
     */
    showConfirmDialog: function(title, message, confirmText = 'Yes', cancelText = 'No') {
        return new Promise((resolve) => {
            // Check if we have a Bootstrap modal for confirmation
            const existingModal = document.getElementById('cloneModeConfirmModal');
            if (existingModal) {
                // Use existing modal
                document.getElementById('cloneModeConfirmTitle').textContent = title;
                document.getElementById('cloneModeConfirmMessage').innerHTML = message;
                document.getElementById('cloneModeConfirmBtn').textContent = confirmText;
                document.getElementById('cloneModeConfirmCancelBtn').textContent = cancelText;

                const modal = new bootstrap.Modal(existingModal);

                const confirmBtn = document.getElementById('cloneModeConfirmBtn');
                const cancelBtn = document.getElementById('cloneModeConfirmCancelBtn');

                const cleanup = () => {
                    confirmBtn.removeEventListener('click', handleConfirm);
                    cancelBtn.removeEventListener('click', handleCancel);
                    existingModal.removeEventListener('hidden.bs.modal', handleCancel);
                };

                const handleConfirm = () => {
                    cleanup();
                    modal.hide();
                    resolve(true);
                };

                const handleCancel = () => {
                    cleanup();
                    modal.hide();
                    resolve(false);
                };

                confirmBtn.addEventListener('click', handleConfirm);
                cancelBtn.addEventListener('click', handleCancel);
                existingModal.addEventListener('hidden.bs.modal', handleCancel);

                modal.show();
            } else {
                // Fallback to native confirm
                const result = confirm(`${title}\n\n${message}`);
                resolve(result);
            }
        });
    },

    /**
     * Save state to local directory
     */
    saveLocal: function() {
        console.log('cloneMode.saveLocal() called');

        const csrfToken = this.getCSRFToken();
        if (!csrfToken) {
            this.showStatus('CSRF token not found. Please refresh the page.', 'error');
            return;
        }

        const force = document.getElementById('saveForceLocal')?.checked || false;

        this.setButtonsDisabled(true);
        this.showStatus('Exporting database state to local directory...', 'info', true);

        const formData = new FormData();
        formData.append('csrfmiddlewaretoken', csrfToken);
        formData.append('force', force);

        fetch('/pybirdai/workflow/clone/save/local/', {
            method: 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => response.json())
        .then(data => {
            console.log('Save local response:', data);
            this.setButtonsDisabled(false);

            if (data.success) {
                let details = '';
                if (data.details) {
                    details = `<ul class="mb-0">
                        <li>Files exported: ${data.details.file_count || 'N/A'}</li>
                        <li>Total size: ${data.details.total_size || 'N/A'}</li>
                        <li>Export path: <code>${data.details.export_path || 'N/A'}</code></li>
                    </ul>`;
                }
                this.showStatus(data.message || 'Export completed successfully!', 'success', false, details);
            } else {
                this.showStatus(data.message || data.error || 'Export failed', 'error');
            }
        })
        .catch(error => {
            console.error('Save local error:', error);
            this.setButtonsDisabled(false);
            this.showStatus(`Error: ${error.message}`, 'error');
        });
    },

    /**
     * Save state to GitHub repository (RESTRICTED)
     * Uses selected target owner, not custom URL
     */
    saveGithub: async function() {
        console.log('cloneMode.saveGithub() called');

        const csrfToken = this.getCSRFToken();
        if (!csrfToken) {
            this.showStatus('CSRF token not found. Please refresh the page.', 'error');
            return;
        }

        const targetSelect = document.getElementById('saveTargetSelect');
        const targetOwner = targetSelect?.value;
        const branch = document.getElementById('saveGithubBranch')?.value?.trim() || 'main';
        const token = document.getElementById('saveGithubToken')?.value?.trim();
        const commitMessage = document.getElementById('saveCommitMessage')?.value?.trim() || 'Update clone state';
        const force = document.getElementById('saveForceGithub')?.checked || false;

        if (!token) {
            this.showStatus('Please enter your GitHub token', 'warning');
            return;
        }

        if (!targetOwner) {
            this.showStatus('Please select a save location', 'warning');
            return;
        }

        // Get the selected target details
        const selectedOption = targetSelect.options[targetSelect.selectedIndex];
        const repoUrl = selectedOption.dataset.repoUrl;

        this.setButtonsDisabled(true);
        this.showStatus('Saving to GitHub...', 'info', true);

        try {
            const formData = new FormData();
            formData.append('csrfmiddlewaretoken', csrfToken);
            formData.append('target_owner', targetOwner);
            formData.append('branch', branch);
            formData.append('token', token);
            formData.append('commit_message', commitMessage);
            formData.append('force', force);

            const response = await fetch('/pybirdai/workflow/clone/save/github/', {
                method: 'POST',
                body: formData,
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                }
            });

            const data = await response.json();
            console.log('Save GitHub response:', data);

            this.setButtonsDisabled(false);

            if (data.success) {
                let details = '';
                if (data.details) {
                    details = `<ul class="mb-0">
                        <li>Repository: <a href="${data.details.repo_url}" target="_blank">${data.details.repo_url}</a></li>
                        <li>Branch: ${data.details.branch || branch}</li>
                        <li>Commit: ${data.details.commit_sha || 'N/A'}</li>
                    </ul>`;
                }
                this.showStatus(data.message || 'Successfully saved to GitHub!', 'success', false, details);
            } else {
                this.showStatus(data.message || data.error || 'Save failed', 'error');
            }

        } catch (error) {
            console.error('Save GitHub error:', error);
            this.setButtonsDisabled(false);
            this.showStatus(`Error: ${error.message}`, 'error');
        }
    },

    /**
     * Load state from local directory
     */
    loadLocal: function() {
        console.log('cloneMode.loadLocal() called');

        const csrfToken = this.getCSRFToken();
        if (!csrfToken) {
            this.showStatus('CSRF token not found. Please refresh the page.', 'error');
            return;
        }

        const localPath = document.getElementById('loadLocalPath')?.value?.trim();
        const force = document.getElementById('loadForceLocal')?.checked || false;
        const skipCleanup = document.getElementById('loadSkipCleanupLocal')?.checked || false;

        if (!localPath) {
            this.showStatus('Please enter a local path', 'warning');
            return;
        }

        this.setButtonsDisabled(true);
        this.showStatus('Importing database state from local directory...', 'info', true);

        const formData = new FormData();
        formData.append('csrfmiddlewaretoken', csrfToken);
        formData.append('local_path', localPath);
        formData.append('force', force);
        formData.append('skip_cleanup', skipCleanup);

        fetch('/pybirdai/workflow/clone/load/local/', {
            method: 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => response.json())
        .then(data => {
            console.log('Load local response:', data);
            this.setButtonsDisabled(false);

            if (data.success) {
                let details = '';
                // Basic import stats
                if (data.details) {
                    details = `<ul class="mb-0">
                        <li>Files imported: ${data.details.file_count || 'N/A'}</li>
                        <li>Records imported: ${data.details.record_count || 'N/A'}</li>
                    </ul>`;
                }
                // Add clone state summary if available
                if (data.clone_state_summary) {
                    details += this.formatCloneStateSummary(data.clone_state_summary);
                }
                this.showStatus(data.message || 'Import completed successfully!', 'success', false, details);

                // Recommend page refresh
                if (data.refresh_recommended) {
                    setTimeout(() => {
                        if (confirm('Database state has been restored. Refresh page to see updated status?')) {
                            location.reload();
                        }
                    }, 1500);
                }
            } else {
                this.showStatus(data.message || data.error || 'Import failed', 'error');
            }
        })
        .catch(error => {
            console.error('Load local error:', error);
            this.setButtonsDisabled(false);
            this.showStatus(`Error: ${error.message}`, 'error');
        });
    },

    /**
     * Load state from GitHub repository (RESTRICTED)
     * Uses selected source from dropdown
     */
    loadGithub: async function() {
        console.log('cloneMode.loadGithub() called');

        const csrfToken = this.getCSRFToken();
        if (!csrfToken) {
            this.showStatus('CSRF token not found. Please refresh the page.', 'error');
            return;
        }

        const sourceSelect = document.getElementById('loadSourceSelect');
        const repoUrl = sourceSelect?.value;
        const branch = document.getElementById('loadGithubBranch')?.value?.trim() || 'main';
        const token = document.getElementById('loadGithubToken')?.value?.trim() || null;
        const force = document.getElementById('loadForceGithub')?.checked || false;
        const skipCleanup = document.getElementById('loadSkipCleanupGithub')?.checked || false;

        if (!repoUrl) {
            this.showStatus('Please select a load source', 'warning');
            return;
        }

        this.setButtonsDisabled(true);
        this.showStatus('Loading from GitHub...', 'info', true);

        try {
            const formData = new FormData();
            formData.append('csrfmiddlewaretoken', csrfToken);
            formData.append('repo_url', repoUrl);
            formData.append('branch', branch);
            formData.append('force', force);
            formData.append('skip_cleanup', skipCleanup);
            if (token) {
                formData.append('token', token);
            }

            const response = await fetch('/pybirdai/workflow/clone/load/github/', {
                method: 'POST',
                body: formData,
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                }
            });

            const data = await response.json();
            console.log('Load GitHub response:', data);

            this.setButtonsDisabled(false);

            if (data.success) {
                let details = '';
                // Basic import stats
                if (data.details) {
                    details = `<ul class="mb-0">
                        <li>Repository: ${data.details.repo_url || repoUrl}</li>
                        <li>Branch: ${data.details.branch || branch}</li>
                        <li>Records imported: ${data.details.record_count || 'N/A'}</li>
                    </ul>`;
                }
                // Add clone state summary if available
                if (data.clone_state_summary) {
                    details += this.formatCloneStateSummary(data.clone_state_summary);
                }
                this.showStatus(data.message || 'Successfully loaded from GitHub!', 'success', false, details);

                // Recommend page refresh
                if (data.refresh_recommended) {
                    setTimeout(() => {
                        if (confirm('Database state has been restored. Refresh page to see updated status?')) {
                            location.reload();
                        }
                    }, 1500);
                }
            } else {
                this.showStatus(data.message || data.error || 'Load failed', 'error');
            }

        } catch (error) {
            console.error('Load GitHub error:', error);
            this.setButtonsDisabled(false);
            this.showStatus(`Error: ${error.message}`, 'error');
        }
    },

    /**
     * Reset status when modal is opened
     */
    onModalOpen: function() {
        this.hideStatus();
        // Reset save targets cache
        this._saveTargets = null;
        // Load default sources (without token)
        this.onLoadTokenInput(null);
    }
};

// Reset status when modal opens and set up event listeners
document.addEventListener('DOMContentLoaded', function() {
    const modal = document.getElementById('cloneModeModal');
    if (modal) {
        modal.addEventListener('show.bs.modal', function() {
            cloneMode.onModalOpen();
        });
    }

    // Set up save token input handler
    const saveTokenInput = document.getElementById('saveGithubToken');
    if (saveTokenInput) {
        saveTokenInput.addEventListener('input', function() {
            cloneMode.onSaveTokenInput(this.value);
        });
    }

    // Set up load token input handler
    const loadTokenInput = document.getElementById('loadGithubToken');
    if (loadTokenInput) {
        loadTokenInput.addEventListener('input', function() {
            cloneMode.onLoadTokenInput(this.value);
        });
    }

    // Enable save button when target is selected
    const saveTargetSelect = document.getElementById('saveTargetSelect');
    if (saveTargetSelect) {
        saveTargetSelect.addEventListener('change', function() {
            const saveBtn = document.getElementById('saveGithubBtn');
            if (saveBtn) {
                saveBtn.disabled = !this.value;
            }
        });
    }
});
