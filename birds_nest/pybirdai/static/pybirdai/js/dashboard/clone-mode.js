// coding=UTF-8
// Copyright (c) 2025 Arfa Digital Consulting
// SPDX-License-Identifier: EPL-2.0

/**
 * Clone Mode Functions
 * Handles: Save/Load state to local directory or GitHub repository
 * Includes: Repository validation and creation flow
 */

const cloneMode = {
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
     * Validate a GitHub repository URL
     * @param {string} repoUrl - The repository URL to validate
     * @param {string} token - GitHub token (optional)
     * @param {string} operation - 'save' or 'load'
     * @returns {Promise<Object>} Validation result
     */
    validateRepo: async function(repoUrl, token, operation = 'save') {
        const csrfToken = this.getCSRFToken();

        const formData = new FormData();
        formData.append('csrfmiddlewaretoken', csrfToken);
        formData.append('repo_url', repoUrl);
        formData.append('operation', operation);
        if (token) {
            formData.append('token', token);
        }

        const response = await fetch('/pybirdai/workflow/clone/validate-repo/', {
            method: 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        });

        return await response.json();
    },

    /**
     * Create a new GitHub repository
     * @param {string} repoUrl - The desired repository URL
     * @param {string} token - GitHub token (required)
     * @param {boolean} isPrivate - Whether to create a private repo
     * @returns {Promise<Object>} Creation result
     */
    createRepo: async function(repoUrl, token, isPrivate = true) {
        const csrfToken = this.getCSRFToken();

        const formData = new FormData();
        formData.append('csrfmiddlewaretoken', csrfToken);
        formData.append('repo_url', repoUrl);
        formData.append('token', token);
        formData.append('private', isPrivate);

        const response = await fetch('/pybirdai/workflow/clone/create-repo/', {
            method: 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        });

        return await response.json();
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
     * Save state to GitHub repository with validation
     */
    saveGithub: async function() {
        console.log('cloneMode.saveGithub() called');

        const csrfToken = this.getCSRFToken();
        if (!csrfToken) {
            this.showStatus('CSRF token not found. Please refresh the page.', 'error');
            return;
        }

        const repoUrl = document.getElementById('saveGithubRepo')?.value?.trim();
        const branch = document.getElementById('saveGithubBranch')?.value?.trim() || 'main';
        const token = document.getElementById('saveGithubToken')?.value?.trim();
        const commitMessage = document.getElementById('saveCommitMessage')?.value?.trim() || 'Update clone state';
        const force = document.getElementById('saveForceGithub')?.checked || false;

        if (!repoUrl) {
            this.showStatus('Please enter a repository URL', 'warning');
            return;
        }

        this.setButtonsDisabled(true);
        this.showStatus('Validating repository...', 'info', true);

        try {
            // Step 1: Validate the repository
            const validationData = await this.validateRepo(repoUrl, token, 'save');
            console.log('Validation result:', validationData);

            if (!validationData.success) {
                this.setButtonsDisabled(false);
                this.showStatus(validationData.message || validationData.error || 'Validation failed', 'error');
                return;
            }

            const validation = validationData.validation;

            // Step 2: Handle validation results
            if (validation.action_required === 'error') {
                this.setButtonsDisabled(false);
                this.showStatus(validation.error || 'Repository validation failed', 'error');
                return;
            }

            if (validation.action_required === 'create') {
                // Repository doesn't exist but can be created
                this.setButtonsDisabled(false);

                const createConfirmed = await this.showConfirmDialog(
                    'Create New Repository?',
                    `<p>The repository <strong>${repoUrl}</strong> does not exist.</p>
                     <p>Would you like to create it?</p>
                     <p class="text-muted small">A new private repository will be created.</p>`,
                    'Create Repository',
                    'Cancel'
                );

                if (!createConfirmed) {
                    this.showStatus('Operation cancelled', 'warning');
                    return;
                }

                // Create the repository
                this.setButtonsDisabled(true);
                this.showStatus('Creating repository...', 'info', true);

                const createResult = await this.createRepo(repoUrl, token, true);
                console.log('Create result:', createResult);

                if (!createResult.success) {
                    this.setButtonsDisabled(false);
                    this.showStatus(createResult.message || createResult.error || 'Failed to create repository', 'error');
                    return;
                }

                this.showStatus('Repository created! Now pushing data...', 'info', true);
            } else {
                // Repository exists, proceed to save
                this.showStatus('Repository validated! Pushing data...', 'info', true);
            }

            // Step 3: Save to GitHub
            const formData = new FormData();
            formData.append('csrfmiddlewaretoken', csrfToken);
            formData.append('repo_url', repoUrl);
            formData.append('branch', branch);
            formData.append('commit_message', commitMessage);
            formData.append('force', force);
            if (token) {
                formData.append('token', token);
            }

            const saveResponse = await fetch('/pybirdai/workflow/clone/save/github/', {
                method: 'POST',
                body: formData,
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                }
            });

            const saveData = await saveResponse.json();
            console.log('Save GitHub response:', saveData);

            this.setButtonsDisabled(false);

            if (saveData.success) {
                let details = '';
                if (saveData.details) {
                    details = `<ul class="mb-0">
                        <li>Repository: ${saveData.details.repo_url || repoUrl}</li>
                        <li>Branch: ${saveData.details.branch || branch}</li>
                        <li>Commit: ${saveData.details.commit_sha || 'N/A'}</li>
                    </ul>`;
                }
                this.showStatus(saveData.message || 'Successfully pushed to GitHub!', 'success', false, details);
            } else {
                this.showStatus(saveData.message || saveData.error || 'GitHub push failed', 'error');
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
                if (data.details) {
                    details = `<ul class="mb-0">
                        <li>Files imported: ${data.details.file_count || 'N/A'}</li>
                        <li>Records imported: ${data.details.record_count || 'N/A'}</li>
                    </ul>`;
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
     * Load state from GitHub repository with validation
     */
    loadGithub: async function() {
        console.log('cloneMode.loadGithub() called');

        const csrfToken = this.getCSRFToken();
        if (!csrfToken) {
            this.showStatus('CSRF token not found. Please refresh the page.', 'error');
            return;
        }

        const repoUrl = document.getElementById('loadGithubRepo')?.value?.trim();
        const branch = document.getElementById('loadGithubBranch')?.value?.trim() || 'main';
        const token = document.getElementById('loadGithubToken')?.value?.trim();
        const force = document.getElementById('loadForceGithub')?.checked || false;
        const skipCleanup = document.getElementById('loadSkipCleanupGithub')?.checked || false;

        if (!repoUrl) {
            this.showStatus('Please enter a repository URL', 'warning');
            return;
        }

        this.setButtonsDisabled(true);
        this.showStatus('Validating repository...', 'info', true);

        try {
            // Step 1: Validate the repository
            const validationData = await this.validateRepo(repoUrl, token, 'load');
            console.log('Validation result:', validationData);

            if (!validationData.success) {
                this.setButtonsDisabled(false);
                this.showStatus(validationData.message || validationData.error || 'Validation failed', 'error');
                return;
            }

            const validation = validationData.validation;

            // For load operations, repo must exist
            if (!validation.exists) {
                this.setButtonsDisabled(false);
                this.showStatus(
                    validation.error || 'Repository not found. Please check the URL and try again.',
                    'error'
                );
                return;
            }

            if (!validation.valid) {
                this.setButtonsDisabled(false);
                this.showStatus(validation.error || 'Cannot access repository', 'error');
                return;
            }

            // Step 2: Load from GitHub
            this.showStatus('Repository validated! Importing data...', 'info', true);

            const formData = new FormData();
            formData.append('csrfmiddlewaretoken', csrfToken);
            formData.append('repo_url', repoUrl);
            formData.append('branch', branch);
            formData.append('force', force);
            formData.append('skip_cleanup', skipCleanup);
            if (token) {
                formData.append('token', token);
            }

            const loadResponse = await fetch('/pybirdai/workflow/clone/load/github/', {
                method: 'POST',
                body: formData,
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                }
            });

            const loadData = await loadResponse.json();
            console.log('Load GitHub response:', loadData);

            this.setButtonsDisabled(false);

            if (loadData.success) {
                let details = '';
                if (loadData.details) {
                    details = `<ul class="mb-0">
                        <li>Repository: ${loadData.details.repo_url || repoUrl}</li>
                        <li>Branch: ${loadData.details.branch || branch}</li>
                        <li>Records imported: ${loadData.details.record_count || 'N/A'}</li>
                    </ul>`;
                }
                this.showStatus(loadData.message || 'Successfully imported from GitHub!', 'success', false, details);

                // Recommend page refresh
                if (loadData.refresh_recommended) {
                    setTimeout(() => {
                        if (confirm('Database state has been restored. Refresh page to see updated status?')) {
                            location.reload();
                        }
                    }, 1500);
                }
            } else {
                this.showStatus(loadData.message || loadData.error || 'GitHub import failed', 'error');
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
    }
};

// Reset status when modal opens
document.addEventListener('DOMContentLoaded', function() {
    const modal = document.getElementById('cloneModeModal');
    if (modal) {
        modal.addEventListener('show.bs.modal', function() {
            cloneMode.onModalOpen();
        });
    }
});
