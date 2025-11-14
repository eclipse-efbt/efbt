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
 * JavaScript for ANCRDT Step 4: Table Execution
 * Handles table execution with filter parameters
 */

// Store selectize instances for each filter
const selectizeInstances = {};

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
 * Execute an ANCRDT table with the selected filters
 * @param {string} tableName - The ANCRDT table name (cube_id)
 */
async function executeTable(tableName) {
    console.log(`Executing table: ${tableName}`);

    // Get UI elements
    const btn = document.getElementById(`btn-${tableName}`);
    const resultsSection = document.getElementById(`results-${tableName}`);
    const resultsContent = document.getElementById(`results-content-${tableName}`);
    const errorSection = document.getElementById(`error-${tableName}`);
    const errorContent = document.getElementById(`error-content-${tableName}`);

    if (!btn) {
        console.error(`Button not found for table: ${tableName}`);
        return;
    }

    // Hide previous results/errors
    resultsSection?.classList.remove('show');
    errorSection?.classList.remove('show');

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
            // Show results
            displayResults(tableName, data);
            if (resultsSection) {
                resultsSection.classList.add('show');
            }
            console.log('Results displayed successfully');
        } else {
            // Show error
            const errorMessage = data.error || 'Unknown error occurred';
            if (errorContent) {
                errorContent.innerHTML = `
                    <p><strong>Error:</strong> ${escapeHtml(errorMessage)}</p>
                `;
            }
            if (errorSection) {
                errorSection.classList.add('show');
            }
            console.error('Execution failed:', errorMessage);
        }

    } catch (error) {
        console.error('Execution error:', error);
        if (errorContent) {
            errorContent.innerHTML = `
                <p><strong>Error:</strong> ${escapeHtml(error.message)}</p>
                <p style="margin-top: 10px; font-size: 12px;">Check the browser console for more details.</p>
            `;
        }
        if (errorSection) {
            errorSection.classList.add('show');
        }
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
 * Display execution results
 * @param {string} tableName - The table name
 * @param {object} data - The response data
 */
function displayResults(tableName, data) {
    const resultsContent = document.getElementById(`results-content-${tableName}`);

    let html = '<div style="display: grid; gap: 10px;">';

    // Row counts
    html += `
        <div>
            <strong>Total Rows Generated:</strong> ${data.row_count_total || 0}
        </div>
    `;

    if (data.filters_applied && Object.keys(data.filters_applied).length > 0) {
        html += `
            <div>
                <strong>Rows After Filtering:</strong> ${data.row_count || 0}
            </div>
            <div>
                <strong>Filters Applied:</strong>
                <ul style="margin: 5px 0; padding-left: 20px;">
        `;
        for (const [key, value] of Object.entries(data.filters_applied)) {
            html += `<li>${escapeHtml(key)}: ${escapeHtml(value)}</li>`;
        }
        html += `
                </ul>
            </div>
        `;
    } else {
        html += `
            <div>
                <strong>Filters Applied:</strong> None
            </div>
        `;
    }

    // CSV output
    if (data.csv_path) {
        html += `
            <div>
                <strong>CSV Output:</strong> ${escapeHtml(data.csv_path)}
            </div>
        `;
    }

    // Execution time
    if (data.execution_time) {
        html += `
            <div>
                <strong>Execution Time:</strong> ${data.execution_time.toFixed(3)}s
            </div>
        `;
    }

    // Show sample rows
    if (data.rows && data.rows.length > 0) {
        // Filter out columns where all values are null
        const columnsWithData = new Set();
        data.rows.forEach(row => {
            Object.entries(row).forEach(([key, value]) => {
                // Include column if it has at least one non-null value
                if (value !== null && value !== 'null' && value !== '' && value !== undefined) {
                    columnsWithData.add(key);
                }
            });
        });

        // Convert to array and maintain order from first row
        const firstRow = data.rows[0];
        const displayColumns = Object.keys(firstRow).filter(key => columnsWithData.has(key));

        html += `
            <div style="margin-top: 15px;">
                <strong>Sample Rows (first ${Math.min(5, data.rows.length)}):</strong>
                <div style="overflow-x: auto; margin-top: 10px;">
                    <table style="width: 100%; border-collapse: collapse; font-size: 12px;">
                        <thead>
                            <tr style="background: #f5f5f5;">
        `;

        // Table headers (only columns with data)
        for (const key of displayColumns) {
            html += `<th style="padding: 8px; border: 1px solid #ddd; text-align: left;">${escapeHtml(key)}</th>`;
        }

        html += `
                            </tr>
                        </thead>
                        <tbody>
        `;

        // Table rows (max 5, only columns with data)
        const displayRows = data.rows.slice(0, 5);
        displayRows.forEach((row, idx) => {
            html += `<tr style="background: ${idx % 2 === 0 ? 'white' : '#fafafa'};">`;
            for (const key of displayColumns) {
                const value = row[key];
                const displayValue = value === null || value === 'null' ? '<em style="color: #999;">null</em>' : escapeHtml(String(value));
                html += `<td style="padding: 8px; border: 1px solid #ddd;">${displayValue}</td>`;
            }
            html += `</tr>`;
        });

        html += `
                        </tbody>
                    </table>
                </div>
            </div>
        `;

        if (data.rows.length > 5) {
            html += `
                <div style="margin-top: 10px; font-style: italic; color: #666;">
                    Showing 5 of ${data.rows.length} total rows. Check the CSV file for complete results.
                </div>
            `;
        }
    } else {
        html += `
            <div style="margin-top: 15px; padding: 15px; background: #fff3cd; border-radius: 4px; color: #856404;">
                <i class="fas fa-info-circle"></i> No rows returned. The table execution completed but produced no results.
            </div>
        `;
    }

    html += '</div>';

    resultsContent.innerHTML = html;
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
