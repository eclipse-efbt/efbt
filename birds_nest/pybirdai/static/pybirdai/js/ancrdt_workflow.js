/*
 * ANCRDT Workflow Shared JavaScript
 * Copyright (c) 2025 Bird Software Solutions Ltd
 * SPDX-License-Identifier: EPL-2.0
 */

/**
 * Get CSRF token for AJAX requests
 */
function getCookie(name) {
    let cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        const cookies = document.cookie.split(';');
        for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].trim();
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

const csrftoken = getCookie('csrftoken');

/**
 * Execute an ANCRDT workflow step
 * @param {number} stepNumber - The step number to execute (0-3)
 */
function executeStep(stepNumber) {
    // Disable the execute button
    const executeButtons = document.querySelectorAll('.btn-execute');
    executeButtons.forEach(btn => {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Executing...';
    });

    // Get the URL for executing the step
    const executeUrl = `/pybirdai/workflow/ancrdt/execute/${stepNumber}/`;

    // Execute via AJAX
    fetch(executeUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrftoken
        },
        credentials: 'same-origin'
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            // Show success message
            showMessage('Step execution started successfully', 'success');
            // Reload page after short delay to show updated status
            setTimeout(() => {
                location.reload();
            }, 1000);
        } else {
            // Show error message
            showMessage(data.error || 'Failed to execute step', 'error');
            // Re-enable button
            executeButtons.forEach(btn => {
                btn.disabled = false;
                btn.innerHTML = '<i class="fas fa-play"></i> Execute';
            });
        }
    })
    .catch(error => {
        console.error('Error executing step:', error);
        showMessage('An error occurred while executing the step', 'error');
        // Re-enable button
        executeButtons.forEach(btn => {
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-play"></i> Execute';
        });
    });
}

/**
 * Show a message to the user
 * @param {string} message - The message text
 * @param {string} type - Message type: success, error, info, warning
 */
function showMessage(message, type) {
    const container = document.getElementById('message-container');
    if (!container) return;

    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type}`;
    alertDiv.textContent = message;

    container.appendChild(alertDiv);

    // Auto-remove after 5 seconds
    setTimeout(() => {
        alertDiv.remove();
    }, 5000);
}

/**
 * Switch between tabs in a tabbed interface
 * @param {string} tabName - The name/ID of the tab to switch to
 * @param {number} stepNumber - The step number for namespacing
 */
function switchTab(tabName, stepNumber) {
    // Get all tab buttons and content for this step
    const tabButtons = document.querySelectorAll('.tab-button');
    const tabContents = document.querySelectorAll('.tab-content');

    // Remove active class from all buttons and content
    tabButtons.forEach(button => button.classList.remove('active'));
    tabContents.forEach(content => content.classList.remove('active'));

    // Add active class to selected tab button
    const targetButton = Array.from(tabButtons).find(btn =>
        btn.onclick && btn.onclick.toString().includes(`'${tabName}'`)
    );
    if (targetButton) {
        targetButton.classList.add('active');
    }

    // Show selected tab content
    const targetContent = document.getElementById(`${tabName}-tab-${stepNumber}`);
    if (targetContent) {
        targetContent.classList.add('active');
    }

    // Save active tab to localStorage for persistence
    localStorage.setItem(`ancrdt-step${stepNumber}-active-tab`, tabName);
}

/**
 * Initialize tab state restoration on page load
 */
document.addEventListener('DOMContentLoaded', function() {
    // This will be called by individual pages if they need tab restoration
    // Each page can override this behavior as needed
});
