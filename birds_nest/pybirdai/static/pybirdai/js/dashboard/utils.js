// coding=UTF-8
// Copyright (c) 2025 Bird Software Solutions Ltd
// SPDX-License-Identifier: EPL-2.0

/**
 * Shared Utility Functions
 */

/**
 * Get CSRF token from cookies
 * @param {string} name - Cookie name (typically 'csrftoken')
 * @returns {string|null} - The cookie value or null
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

// Pre-fetch CSRF token for common usage
const csrftoken = getCookie('csrftoken');

/**
 * Show status message in a status div
 * @param {HTMLElement} statusDiv - The status div element
 * @param {string} message - The message to display
 * @param {string} type - Message type: 'success', 'error', 'warning', 'info'
 */
function showStatusMessage(statusDiv, message, type) {
    if (!statusDiv) return;

    const styles = {
        success: { bg: '#d4edda', color: '#155724', icon: '✅' },
        error: { bg: '#f8d7da', color: '#721c24', icon: '❌' },
        warning: { bg: '#fff3cd', color: '#856404', icon: '⚠️' },
        info: { bg: '#e3f2fd', color: '#1976d2', icon: '⏳' }
    };

    const style = styles[type] || styles.info;
    statusDiv.style.display = 'block';
    statusDiv.style.background = style.bg;
    statusDiv.style.color = style.color;
    statusDiv.innerHTML = `<span style="display: inline-block; margin-right: 8px;">${style.icon}</span>${message}`;
}

/**
 * Format elapsed time for display
 * @param {number} seconds - Elapsed seconds
 * @returns {string} - Formatted time string
 */
function formatElapsedTime(seconds) {
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    return minutes > 0 ? `${minutes}m ${remainingSeconds}s` : `${seconds}s`;
}
