// coding=UTF-8
// Copyright (c) 2025 Arfa Digital Consulting
// This program and the accompanying materials
// are made available under the terms of the Eclipse Public License 2.0
// which accompanies this distribution, and is available at
// https://www.eclipse.org/legal/epl-2.0/
//
// SPDX-License-Identifier: EPL-2.0
//
// Contributors:
//    Benjamin Arfa - initial API and implementation
//
/**
 * JavaScript for ANCRDT Execute Tables Utility
 * Handles table execution with filter parameters and modal display
 * Note: This is for the Execute Tables utility, not a workflow step.
 * Step 4 is "Run Tests" which is handled separately.
 * Version: 2.0 - Modal-only results
 */

// Store selectize instances for each filter
const selectizeInstances = {};

// Store execution state for each table
const executionStates = {};

/**
 * Format text by replacing underscores with spaces
 * @param {string} text - Text to format
 * @returns {string} Formatted text
 */
function formatDisplayText(text) {
    if (!text) return '';
    return text.replace(/_/g, ' ');
}

/**
 * Initialize Selectize on all filter dropdowns
 */
function initializeSelectize() {
    console.log('Initializing Selectize...');

    // Initialize Selectize on all filter select elements
    document.querySelectorAll('.selectize-filter').forEach(select => {
        try {
            const $select = $(select);
            const dimension = $select.data('dimension');
            const table = $select.data('table');
            const instanceKey = `${table}-${dimension}`;

            console.log(`Initializing Selectize for: ${instanceKey}`);

            // Check if already initialized
            if (select.selectize) {
                select.selectize.destroy();
            }

            // Initialize Selectize
            $select.selectize({
                plugins: ['remove_button'],
                delimiter: ',',
                persist: false,
                searchField: ['text', 'value'],
                sortField: 'text',
                maxItems: null,  // Allow unlimited selections
                closeAfterSelect: false,
                onChange: function(value) {
                    // Update filter count when selection changes
                    if (typeof updateFilterCount === 'function') {
                        updateFilterCount(table);
                    }
                },
                render: {
                    option: function(item, escape) {
                        // Extract name and code from text like "Member_Name (CODE)"
                        const parts = item.text.split(' (');
                        const name = formatDisplayText(parts[0]);
                        const code = parts[1] ? parts[1].replace(')', '') : item.value;

                        return `<div>
                            <span class="member-name">${escape(name)}</span>
                            <span class="member-code">(${escape(code)})</span>
                        </div>`;
                    },
                    item: function(item, escape) {
                        // Format selected item text by replacing underscores
                        const name = formatDisplayText(item.text.split(' (')[0]);
                        return `<div>${escape(name)}</div>`;
                    }
                }
            });

            // Store instance reference (Selectize attaches itself to the DOM element)
            selectizeInstances[instanceKey] = select.selectize;

            console.log(`Selectize initialized successfully for: ${instanceKey}`);
        } catch (error) {
            console.error(`Error initializing Selectize for ${select.id}:`, error);
        }
    });

    console.log('Selectize instances:', Object.keys(selectizeInstances));
}

/**
 * Destroy and reinitialize Selectize instances (useful when switching tables)
 */
function reinitializeSelectize() {
    // Destroy existing instances
    for (const key in selectizeInstances) {
        if (selectizeInstances[key]) {
            selectizeInstances[key].destroy();
            delete selectizeInstances[key];
        }
    }

    // Reinitialize
    initializeSelectize();
}

/**
 * Update the "View Executed Table" button state based on execution results
 * @param {string} tableName - The table name
 * @param {string} state - The state ('disabled', 'error', 'warning', 'success')
 */
function updateViewButtonState(tableName, state) {
    const btn = document.getElementById(`btn-view-${tableName}`);
    if (!btn) {
        console.error(`View button not found for table: ${tableName}`);
        return;
    }

    // Remove all state classes
    btn.classList.remove('btn-view-results-disabled', 'btn-view-results-error', 'btn-view-results-warning', 'btn-view-results-success');

    // Add appropriate state class and enable/disable
    switch (state) {
        case 'error':
            btn.classList.add('btn-view-results-error');
            btn.disabled = false;
            break;
        case 'warning':
            btn.classList.add('btn-view-results-warning');
            btn.disabled = false;
            break;
        case 'success':
            btn.classList.add('btn-view-results-success');
            btn.disabled = false;
            break;
        case 'disabled':
        default:
            btn.classList.add('btn-view-results-disabled');
            btn.disabled = true;
            break;
    }
}

/**
 * Execute an ANCRDT table with the selected filters
 * @param {string} tableName - The ANCRDT table name (cube_id)
 */
async function executeTable(tableName) {
    console.log(`Executing table: ${tableName}`);

    // Get UI elements
    const btn = document.getElementById(`btn-execute-${tableName}`);

    if (!btn) {
        console.error(`Button not found for table: ${tableName}`);
        return;
    }

    // Disable button and show loading
    btn.disabled = true;
    btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Executing...';

    try {
        // Collect filter parameters from Selectize instances
        const filters = {};

        console.log(`Looking for Selectize instances for table: ${tableName}`);
        console.log('Available instances:', Object.keys(selectizeInstances));

        // Get all Selectize instances for this table
        for (const key in selectizeInstances) {
            if (key.startsWith(`${tableName}-`)) {
                const dimension = key.replace(`${tableName}-`, '');
                const selectizeInstance = selectizeInstances[key];

                if (!selectizeInstance) {
                    console.warn(`Selectize instance not found for key: ${key}`);
                    continue;
                }

                // Get selected values (codes)
                const selectedValues = selectizeInstance.getValue();
                console.log(`Dimension ${dimension} selected values:`, selectedValues);

                // Convert to array if it's a string
                const valuesArray = typeof selectedValues === 'string'
                    ? selectedValues.split(',').filter(v => v)
                    : (Array.isArray(selectedValues) ? selectedValues : []);

                if (valuesArray.length > 0) {
                    filters[dimension] = valuesArray.join(',');
                }
            }
        }

        console.log('Collected filters:', filters);

        // Build execution URL with query parameters for filters
        const queryParams = new URLSearchParams({ format: 'json' });

        // Add filter parameters to query string
        for (const [dimension, values] of Object.entries(filters)) {
            // values is already comma-separated string like "7,8"
            queryParams.append(dimension, values);
        }

        const url = `/pybirdai/execute-ancrdt-table/${tableName}/?${queryParams.toString()}`;
        console.log(`Making request to: ${url}`);
        console.log('Filters:', filters);

        // Make AJAX request using GET (filters in query params)
        const response = await fetch(url, {
            method: 'GET',
            headers: {
                'Accept': 'application/json'
            }
        });

        console.log('Response status:', response.status);

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        console.log('Response data:', data);

        if (data.success) {
            // Store execution state
            executionStates[tableName] = {
                status: 'success',
                data: data,
                error: null
            };

            // Update button state based on row count
            if (data.rows && data.rows.length > 0) {
                updateViewButtonState(tableName, 'success');
            } else {
                updateViewButtonState(tableName, 'warning');
            }

            console.log('Execution successful');
        } else {
            // Store error state
            const errorMessage = data.error || 'Unknown error occurred';
            executionStates[tableName] = {
                status: 'error',
                data: null,
                error: errorMessage
            };
            updateViewButtonState(tableName, 'error');
            console.error('Execution failed:', errorMessage);
        }

    } catch (error) {
        console.error('Execution error:', error);
        // Store error state
        executionStates[tableName] = {
            status: 'error',
            data: null,
            error: error.message
        };
        updateViewButtonState(tableName, 'error');
    } finally {
        // Re-enable button
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-play"></i> Execute Table';
        }
        console.log('Execution complete');
    }
}

/**
 * Build results table HTML
 * @param {Array} rows - Data rows
 * @param {boolean} showAll - Show all rows (true) or limit to 5 (false)
 * @returns {string} HTML string
 */
function buildResultsTable(rows, showAll = false) {
    if (!rows || rows.length === 0) {
        return '<div style="padding: 20px; text-align: center; color: #999;">No rows to display</div>';
    }

    // Filter out columns where all values are null
    const columnsWithData = new Set();
    rows.forEach(row => {
        Object.entries(row).forEach(([key, value]) => {
            if (value !== null && value !== 'null' && value !== '' && value !== undefined) {
                columnsWithData.add(key);
            }
        });
    });

    const firstRow = rows[0];
    const displayColumns = Object.keys(firstRow).filter(key => columnsWithData.has(key));

    const displayRows = showAll ? rows : rows.slice(0, 5);
    const tableClass = showAll ? 'results-modal-table' : '';
    const wrapperClass = showAll ? 'results-modal-table-wrapper' : '';
    const wrapperStyle = showAll ? '' : 'overflow-x: auto; margin-top: 10px;';
    const tableStyle = showAll ? '' : 'width: 100%; border-collapse: collapse; font-size: 12px;';

    let html = `<div class="${wrapperClass}" style="${wrapperStyle}">`;
    html += `<table class="${tableClass}" style="${tableStyle}">`;
    html += '<thead><tr style="background: #f5f5f5;">';

    // Headers
    for (const key of displayColumns) {
        const padding = showAll ? '12px' : '8px';
        html += `<th style="padding: ${padding}; border: 1px solid #ddd; text-align: left;">${escapeHtml(key)}</th>`;
    }
    html += '</tr></thead><tbody>';

    // Rows
    displayRows.forEach((row, idx) => {
        html += `<tr style="background: ${idx % 2 === 0 ? 'white' : '#fafafa'};">`;
        const padding = showAll ? '10px 12px' : '8px';
        for (const key of displayColumns) {
            const value = row[key];
            const displayValue = value === null || value === 'null' ? '<em style="color: #999;">null</em>' : escapeHtml(String(value));
            html += `<td style="padding: ${padding}; border: 1px solid #ddd;">${displayValue}</td>`;
        }
        html += '</tr>';
    });

    html += '</tbody></table></div>';

    return html;
}

/**
 * Open results modal with execution results
 * @param {string} tableName - The table name
 */
function openResultsModal(tableName) {
    const modal = document.getElementById('results-modal');
    const title = document.getElementById('modal-table-title');
    const summary = document.getElementById('modal-results-summary');
    const tableContainer = document.getElementById('modal-results-table-container');

    // Get execution state
    const state = executionStates[tableName];
    if (!state) {
        console.error(`No execution state found for table: ${tableName}`);
        return;
    }

    // Set title
    title.textContent = `Results for ${tableName}`;

    let summaryHtml = '';
    let tableHtml = '';

    if (state.status === 'error') {
        // Error state - show error message
        summaryHtml = `
            <div style="background: #fef2f2; border: 2px solid #dc2626; border-radius: 8px; padding: 20px; margin-bottom: 20px;">
                <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 10px;">
                    <i class="fas fa-exclamation-circle" style="color: #dc2626; font-size: 24px;"></i>
                    <h3 style="margin: 0; color: #991b1b;">Execution Error</h3>
                </div>
                <div style="background: white; padding: 15px; border-radius: 4px; margin-top: 15px;">
                    <strong>Error Message:</strong><br>
                    <p style="margin: 10px 0; color: #991b1b; font-family: monospace;">${escapeHtml(state.error)}</p>
                </div>
            </div>
        `;
        tableHtml = '';
    } else if (state.status === 'success') {
        const data = state.data;

        // Build summary
        summaryHtml = '<div style="display: grid; gap: 10px; margin-bottom: 20px;">';

        if (!data.rows || data.rows.length === 0) {
            // Warning state - execution succeeded but no results
            summaryHtml += `
                <div style="background: #fef3c7; border: 2px solid #f59e0b; border-radius: 8px; padding: 20px; margin-bottom: 15px;">
                    <div style="display: flex; align-items: center; gap: 10px;">
                        <i class="fas fa-exclamation-triangle" style="color: #d97706; font-size: 24px;"></i>
                        <div>
                            <h3 style="margin: 0; color: #92400e;">Execution Succeeded - No Results</h3>
                            <p style="margin: 5px 0 0 0; color: #78350f;">The table executed successfully but returned 0 rows.</p>
                        </div>
                    </div>
                </div>
            `;
        } else {
            // Success state - show stats
            summaryHtml += `
                <div style="background: #d1fae5; border: 2px solid #10b981; border-radius: 8px; padding: 15px; margin-bottom: 15px;">
                    <div style="display: flex; align-items: center; gap: 10px;">
                        <i class="fas fa-check-circle" style="color: #059669; font-size: 24px;"></i>
                        <h3 style="margin: 0; color: #065f46;">Execution Successful</h3>
                    </div>
                </div>
            `;
        }

        summaryHtml += `<div><strong>Total Rows Generated:</strong> ${data.row_count_total || 0}</div>`;

        if (data.filters_applied && Object.keys(data.filters_applied).length > 0) {
            summaryHtml += `<div><strong>Rows After Filtering:</strong> ${data.row_count || 0}</div>`;
            summaryHtml += `<div><strong>Filters Applied:</strong> `;
            const filterPairs = Object.entries(data.filters_applied).map(([k, v]) => `${escapeHtml(k)}: ${escapeHtml(v)}`);
            summaryHtml += filterPairs.join(', ');
            summaryHtml += `</div>`;
        }

        if (data.execution_time) {
            summaryHtml += `<div><strong>Execution Time:</strong> ${data.execution_time.toFixed(3)}s</div>`;
        }

        if (data.csv_path) {
            // Build query string with filters
            let downloadUrl = `/pybirdai/download-ancrdt-csv/${encodeURIComponent(tableName)}/`;
            if (data.filters_applied && Object.keys(data.filters_applied).length > 0) {
                const queryParams = Object.entries(data.filters_applied)
                    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
                    .join('&');
                downloadUrl += `?${queryParams}`;
            }

            summaryHtml += `
                <div style="margin-top: 10px;">
                    <a href="${downloadUrl}" class="btn-download-csv" download>
                        <i class="fas fa-download"></i> Download CSV
                    </a>
                </div>`;

            // Build API URLs for sharing
            const baseUrl = window.location.origin;
            let jsonApiUrl = `${baseUrl}/pybirdai/execute-ancrdt-table/${encodeURIComponent(tableName)}/?format=json`;
            let csvApiUrl = `${baseUrl}/pybirdai/download-ancrdt-csv/${encodeURIComponent(tableName)}/`;

            if (data.filters_applied && Object.keys(data.filters_applied).length > 0) {
                const queryParams = Object.entries(data.filters_applied)
                    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
                    .join('&');
                jsonApiUrl += `&${queryParams}`;
                csvApiUrl += `?${queryParams}`;
            }

            summaryHtml += `
                <div style="margin-top: 20px; padding: 15px; background: #f8f9fa; border-radius: 6px; border: 1px solid #dee2e6;">
                    <h4 style="margin: 0 0 12px 0; font-size: 14px; font-weight: 600; color: #495057;">
                        <i class="fas fa-link"></i> API Endpoints
                    </h4>
                    <div style="margin-bottom: 12px;">
                        <label style="font-weight: 500; font-size: 12px; color: #6c757d; display: block; margin-bottom: 4px;">JSON API:</label>
                        <div style="display: flex; gap: 8px; align-items: center;">
                            <code id="json-api-url" style="flex: 1; padding: 8px; background: white; border: 1px solid #ddd; border-radius: 4px; font-size: 11px; word-break: break-all;">${jsonApiUrl}</code>
                            <button onclick="copyToClipboard('json-api-url')" class="btn-copy" style="padding: 6px 12px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; white-space: nowrap; font-size: 12px;">
                                <i class="fas fa-copy"></i> Copy
                            </button>
                        </div>
                    </div>
                    <div>
                        <label style="font-weight: 500; font-size: 12px; color: #6c757d; display: block; margin-bottom: 4px;">CSV API:</label>
                        <div style="display: flex; gap: 8px; align-items: center;">
                            <code id="csv-api-url" style="flex: 1; padding: 8px; background: white; border: 1px solid #ddd; border-radius: 4px; font-size: 11px; word-break: break-all;">${csvApiUrl}</code>
                            <button onclick="copyToClipboard('csv-api-url')" class="btn-copy" style="padding: 6px 12px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; white-space: nowrap; font-size: 12px;">
                                <i class="fas fa-copy"></i> Copy
                            </button>
                        </div>
                    </div>
                </div>`;
        }

        summaryHtml += '</div>';

        // Build table if there are rows
        if (data.rows && data.rows.length > 0) {
            tableHtml = buildResultsTable(data.rows, true);
        }
    }

    summary.innerHTML = summaryHtml;
    tableContainer.innerHTML = tableHtml;

    // Show modal
    modal.classList.add('show');
}

/**
 * Close results modal
 */
function closeResultsModal() {
    const modal = document.getElementById('results-modal');
    modal.classList.remove('show');
}

/**
 * Escape HTML to prevent XSS
 * @param {string} text - Text to escape
 * @returns {string} Escaped text
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Copy text to clipboard from an element
 * @param {string} elementId - ID of the element containing the text to copy
 */
function copyToClipboard(elementId) {
    const element = document.getElementById(elementId);
    if (!element) {
        console.error('Element not found:', elementId);
        return;
    }

    const text = element.textContent || element.innerText;

    // Use the Clipboard API if available
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(function() {
            // Visual feedback - change button text temporarily
            const button = event.target.closest('button');
            if (button) {
                const originalHtml = button.innerHTML;
                button.innerHTML = '<i class="fas fa-check"></i> Copied!';
                button.style.background = '#28a745';
                setTimeout(function() {
                    button.innerHTML = originalHtml;
                    button.style.background = '#007bff';
                }, 2000);
            }
        }).catch(function(err) {
            console.error('Failed to copy text: ', err);
            // Fallback to execCommand
            fallbackCopyToClipboard(text);
        });
    } else {
        // Fallback for older browsers
        fallbackCopyToClipboard(text);
    }
}

/**
 * Fallback copy method for older browsers
 * @param {string} text - Text to copy
 */
function fallbackCopyToClipboard(text) {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.top = '-9999px';
    textArea.style.left = '-9999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();

    try {
        const successful = document.execCommand('copy');
        if (successful) {
            const button = event.target.closest('button');
            if (button) {
                const originalHtml = button.innerHTML;
                button.innerHTML = '<i class="fas fa-check"></i> Copied!';
                button.style.background = '#28a745';
                setTimeout(function() {
                    button.innerHTML = originalHtml;
                    button.style.background = '#007bff';
                }, 2000);
            }
        } else {
            alert('Failed to copy text. Please copy manually.');
        }
    } catch (err) {
        console.error('Fallback copy failed:', err);
        alert('Failed to copy text. Please copy manually.');
    }

    document.body.removeChild(textArea);
}

/**
 * Get cookie value by name (for CSRF token)
 * @param {string} name - Cookie name
 * @returns {string|null} Cookie value
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

// Initialize Selectize when the page loads
$(document).ready(function() {
    initializeSelectize();
});

// Close modal when clicking outside of it
window.onclick = function(event) {
    const modal = document.getElementById('results-modal');
    if (event.target === modal) {
        closeResultsModal();
    }
};
