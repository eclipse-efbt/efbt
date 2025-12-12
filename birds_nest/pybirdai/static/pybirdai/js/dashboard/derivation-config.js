// coding=UTF-8
// Copyright (c) 2025 Arfa Digital Consulting
// This program and the accompanying materials
// are made available under the terms of the Eclipse Public License 2.0
// which accompanies this distribution, and is available at
// https://www.eclipse.org/legal/epl-2.0/
//
// SPDX-License-Identifier: EPL-2.0

// ============================================
// Derivation Configuration - Simple DOM-based (no Cytoscape)
// ============================================

let derivationData = [];  // All derivations
let derivationSelections = {};  // Current selections: {class.field: true/false}

// Helper function to get CSRF token
function getCSRFToken() {
    let cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        const cookies = document.cookie.split(';');
        for (let i = 0; i < cookies.length; i++) {
            const cookie = cookies[i].trim();
            if (cookie.substring(0, 'csrftoken'.length + 1) === ('csrftoken' + '=')) {
                cookieValue = decodeURIComponent(cookie.substring('csrftoken'.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

// Load derivations and render graph
function loadDerivationsForGraph() {
    showDerivationStatus('Loading...', 'info');

    fetch('/pybirdai/api/derivations/available/')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                derivationData = data.derivations;

                // Initialize selections from config
                derivationSelections = {};
                derivationData.forEach(d => {
                    const key = `${d.class_name}.${d.field_name}`;
                    derivationSelections[key] = d.enabled;
                });

                // Populate class filter
                const classFilter = document.getElementById('derivation-class-filter');
                if (classFilter) {
                    classFilter.innerHTML = '<option value="">All Classes</option>';
                    data.classes.forEach(cls => {
                        classFilter.innerHTML += `<option value="${cls}">${cls}</option>`;
                    });
                }

                // Update counts
                const totalCountEl = document.getElementById('derivation-total-count');
                if (totalCountEl) totalCountEl.textContent = data.total_count;

                renderDerivationGraph();
                updateDerivationCount();
                showDerivationStatus('Loaded!', 'info');
            } else {
                showDerivationStatus('Error: ' + data.error, 'error');
            }
        })
        .catch(error => {
            console.error('Error loading derivations:', error);
            showDerivationStatus('Error loading derivations', 'error');
        });
}

// Render derivation graph as simple DOM elements
function renderDerivationGraph() {
    const container = document.getElementById('derivation-graph');
    if (!container) {
        console.error('Derivation graph container not found');
        return;
    }

    const searchEl = document.getElementById('derivation-search');
    const filterEl = document.getElementById('derivation-class-filter');
    const searchTerm = searchEl ? searchEl.value.toLowerCase() : '';
    const classFilter = filterEl ? filterEl.value : '';

    // Filter derivations
    const filtered = derivationData.filter(d => {
        const matchesSearch = d.class_name.toLowerCase().includes(searchTerm) ||
                              d.field_name.toLowerCase().includes(searchTerm);
        const matchesClass = !classFilter || d.class_name === classFilter;
        return matchesSearch && matchesClass;
    });

    // Group by class
    const grouped = {};
    filtered.forEach(d => {
        if (!grouped[d.class_name]) {
            grouped[d.class_name] = [];
        }
        grouped[d.class_name].push(d);
    });

    // Build HTML
    const classNames = Object.keys(grouped).sort();

    if (classNames.length === 0) {
        container.innerHTML = '<div style="text-align: center; color: #999; padding: 40px;">No derivations found</div>';
        return;
    }

    container.innerHTML = classNames.map(className => {
        const fields = grouped[className];
        const autoFields = fields.filter(d => d.type !== 'manual');
        const allEnabled = autoFields.length > 0 && autoFields.every(d => derivationSelections[`${d.class_name}.${d.field_name}`]);

        return `
            <div class="derivation-class" data-class="${className}">
                <div class="derivation-class-header" onclick="toggleClassDerivations('${className}')">
                    <span class="class-name">${className}</span>
                    <span class="class-count">${fields.length} fields</span>
                    <span class="class-toggle ${allEnabled ? 'enabled' : ''}">
                        ${autoFields.length > 0 ? (allEnabled ? '✓ All enabled' : 'Click to enable all') : 'All manual'}
                    </span>
                </div>
                <div class="derivation-fields">
                    ${fields.map(d => {
                        const key = `${d.class_name}.${d.field_name}`;
                        const isManual = d.type === 'manual';
                        const isEnabled = isManual ? true : (derivationSelections[key] || false);
                        const statusClass = isManual ? 'manual' : (isEnabled ? 'enabled' : 'disabled');

                        return `
                            <div class="derivation-field ${statusClass}"
                                 data-key="${key}"
                                 data-manual="${isManual}"
                                 onclick="${isManual ? '' : `toggleFieldDerivation('${key}')`}"
                                 title="${key}${isManual ? ' (manual - always enabled)' : ''}">
                                <span class="field-name">${d.field_name}</span>
                                <span class="field-status">${isManual ? '📝' : (isEnabled ? '✓' : '○')}</span>
                            </div>
                        `;
                    }).join('')}
                </div>
            </div>
        `;
    }).join('');

    // Add CSS if not already added
    addDerivationStyles();
}

// Add CSS styles for derivation graph
function addDerivationStyles() {
    if (document.getElementById('derivation-graph-styles')) return;

    const styles = document.createElement('style');
    styles.id = 'derivation-graph-styles';
    styles.textContent = `
        #derivation-graph {
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            padding: 20px;
            background: #f5f5f5;
            min-height: 300px;
            overflow: auto;
        }

        .derivation-class {
            background: white;
            border: 2px solid #9c27b0;
            border-radius: 8px;
            min-width: 280px;
            max-width: 400px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }

        .derivation-class-header {
            background: #e1bee7;
            padding: 10px 15px;
            border-radius: 6px 6px 0 0;
            cursor: pointer;
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
            align-items: center;
            gap: 8px;
        }

        .derivation-class-header:hover {
            background: #ce93d8;
        }

        .derivation-class-header .class-name {
            font-weight: bold;
            color: #6a1b9a;
        }

        .derivation-class-header .class-count {
            font-size: 11px;
            color: #7b1fa2;
        }

        .derivation-class-header .class-toggle {
            font-size: 10px;
            color: #999;
            padding: 2px 8px;
            background: #f5f5f5;
            border-radius: 10px;
        }

        .derivation-class-header .class-toggle.enabled {
            background: #9c27b0;
            color: white;
        }

        .derivation-fields {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            padding: 12px;
        }

        .derivation-field {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 8px;
            padding: 6px 10px;
            border-radius: 4px;
            font-size: 11px;
            cursor: pointer;
            transition: all 0.2s;
            border: 1px solid transparent;
            min-width: 100px;
        }

        .derivation-field.disabled {
            background: #e0e0e0;
            color: #666;
            border-color: #999;
        }

        .derivation-field.disabled:hover {
            background: #d0d0d0;
        }

        .derivation-field.enabled {
            background: #9c27b0;
            color: white;
            border-color: #6a1b9a;
        }

        .derivation-field.enabled:hover {
            background: #7b1fa2;
        }

        .derivation-field.manual {
            background: #ff9800;
            color: white;
            border: 2px dashed #e65100;
            cursor: default;
        }

        .derivation-field .field-name {
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            max-width: 120px;
        }

        .derivation-field .field-status {
            font-size: 12px;
            flex-shrink: 0;
        }
    `;
    document.head.appendChild(styles);
}

// Toggle a single field derivation
function toggleFieldDerivation(key) {
    const currentEnabled = derivationSelections[key] || false;
    derivationSelections[key] = !currentEnabled;

    // Update DOM
    const fieldEl = document.querySelector(`.derivation-field[data-key="${key}"]`);
    if (fieldEl) {
        if (derivationSelections[key]) {
            fieldEl.classList.remove('disabled');
            fieldEl.classList.add('enabled');
            fieldEl.querySelector('.field-status').textContent = '✓';
        } else {
            fieldEl.classList.remove('enabled');
            fieldEl.classList.add('disabled');
            fieldEl.querySelector('.field-status').textContent = '○';
        }
    }

    // Update class header toggle status
    updateClassToggleStatus(key.split('.')[0]);
    updateDerivationCount();
}

// Toggle all derivations in a class
function toggleClassDerivations(className) {
    const classEl = document.querySelector(`.derivation-class[data-class="${className}"]`);
    if (!classEl) return;

    // Get all auto fields in this class
    const fieldEls = classEl.querySelectorAll('.derivation-field:not([data-manual="true"])');
    if (fieldEls.length === 0) return;

    // Check if all are enabled
    const allEnabled = Array.from(fieldEls).every(el => el.classList.contains('enabled'));
    const newEnabled = !allEnabled;

    // Toggle all
    fieldEls.forEach(fieldEl => {
        const key = fieldEl.dataset.key;
        derivationSelections[key] = newEnabled;

        if (newEnabled) {
            fieldEl.classList.remove('disabled');
            fieldEl.classList.add('enabled');
            fieldEl.querySelector('.field-status').textContent = '✓';
        } else {
            fieldEl.classList.remove('enabled');
            fieldEl.classList.add('disabled');
            fieldEl.querySelector('.field-status').textContent = '○';
        }
    });

    // Update class header
    updateClassToggleStatus(className);
    updateDerivationCount();
}

// Update class toggle status display
function updateClassToggleStatus(className) {
    const classEl = document.querySelector(`.derivation-class[data-class="${className}"]`);
    if (!classEl) return;

    const fieldEls = classEl.querySelectorAll('.derivation-field:not([data-manual="true"])');
    const toggleEl = classEl.querySelector('.class-toggle');
    if (!toggleEl || fieldEls.length === 0) return;

    const allEnabled = Array.from(fieldEls).every(el => el.classList.contains('enabled'));
    toggleEl.classList.toggle('enabled', allEnabled);
    toggleEl.textContent = allEnabled ? '✓ All enabled' : 'Click to enable all';
}

// Filter graph based on search/class filter
function filterDerivationGraph() {
    renderDerivationGraph();
}

// Toggle derivation (for compatibility)
function toggleDerivation(key) {
    toggleFieldDerivation(key);
}

// Update count display
function updateDerivationCount() {
    const count = Object.values(derivationSelections).filter(v => v).length;
    const el = document.getElementById('derivation-selected-count');
    if (el) el.textContent = count;
}

// Enable all derivations
function enableAllDerivations() {
    fetch('/pybirdai/api/derivations/enable-all/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCSRFToken()
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            Object.keys(derivationSelections).forEach(key => {
                derivationSelections[key] = true;
            });
            renderDerivationGraph();
            updateDerivationCount();
            showDerivationStatus(data.message, 'success');
        } else {
            showDerivationStatus('Error: ' + data.error, 'error');
        }
    })
    .catch(error => {
        console.error('Error enabling all:', error);
        showDerivationStatus('Error enabling all derivations', 'error');
    });
}

// Disable all derivations
function disableAllDerivations() {
    fetch('/pybirdai/api/derivations/disable-all/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCSRFToken()
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            Object.keys(derivationSelections).forEach(key => {
                derivationSelections[key] = false;
            });
            renderDerivationGraph();
            updateDerivationCount();
            showDerivationStatus(data.message, 'success');
        } else {
            showDerivationStatus('Error: ' + data.error, 'error');
        }
    })
    .catch(error => {
        console.error('Error disabling all:', error);
        showDerivationStatus('Error disabling all derivations', 'error');
    });
}

// Save derivation configuration
function saveDerivationConfig() {
    showDerivationStatus('Saving configuration...', 'info');

    // Build selections array
    const selections = Object.entries(derivationSelections).map(([key, enabled]) => {
        const [class_name, field_name] = key.split('.', 2);
        return { class_name, field_name, enabled };
    });

    // Save the config (merge will happen when "Setup Database" is clicked)
    fetch('/pybirdai/api/derivations/save/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCSRFToken()
        },
        body: JSON.stringify({ selections })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showDerivationStatus('Saved!', 'success');
        } else {
            throw new Error(data.error || 'Failed to save configuration');
        }
    })
    .catch(error => {
        console.error('Error saving derivation config:', error);
        showDerivationStatus('Error: ' + error.message, 'error');
    });
}

// Show status message
function showDerivationStatus(message, type) {
    const statusEl = document.getElementById('derivation-status');
    if (statusEl) {
        const color = type === 'success' ? '#28a745' : type === 'error' ? '#dc3545' : type === 'warning' ? '#ffc107' : '#666';
        statusEl.innerHTML = `<span style="color: ${color}">${message}</span>`;
    }
}

// Modal functions
function showDerivationModal() {
    document.getElementById('derivationModal').style.display = 'block';
    document.body.style.overflow = 'hidden';
    loadDerivationsForGraph();
}

function closeDerivationModal() {
    document.getElementById('derivationModal').style.display = 'none';
    document.body.style.overflow = '';
}
