// coding=UTF-8
// Copyright (c) 2025 Arfa Digital Consulting
// This program and the accompanying materials
// are made available under the terms of the Eclipse Public License 2.0
// which accompanies this distribution, and is available at
// https://www.eclipse.org/legal/epl-2.0/
//
// SPDX-License-Identifier: EPL-2.0

// ============================================
// Derivation Configuration - Cytoscape Graph
// ============================================

let derivationData = [];  // All derivations
let derivationSelections = {};  // Current selections: {class.field: true/false}
let derivationCy = null;  // Cytoscape instance

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

// Render Cytoscape graph
function renderDerivationGraph() {
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

    // Layout parameters
    const fieldWidth = 100;
    const fieldHeight = 30;
    const fieldGapX = 10;
    const fieldGapY = 8;
    const fieldsPerRow = 4;
    const classPadding = 40;
    const classGapX = 30;
    const classGapY = 30;
    const classesPerRow = 3;

    // Calculate class sizes and positions
    const classNames = Object.keys(grouped).sort();
    const classInfo = {};

    classNames.forEach(className => {
        const fieldCount = grouped[className].length;
        const rows = Math.ceil(fieldCount / fieldsPerRow);
        const cols = Math.min(fieldCount, fieldsPerRow);
        const width = cols * fieldWidth + (cols - 1) * fieldGapX + classPadding * 2;
        const height = rows * fieldHeight + (rows - 1) * fieldGapY + classPadding * 2 + 20; // +20 for label
        classInfo[className] = { width, height, fieldCount, rows, cols };
    });

    // Position classes in grid rows
    const positions = {};
    let currentX = 0;
    let currentY = 0;
    let rowMaxHeight = 0;
    let classInRow = 0;

    classNames.forEach(className => {
        const info = classInfo[className];

        // Start new row if needed
        if (classInRow >= classesPerRow) {
            currentX = 0;
            currentY += rowMaxHeight + classGapY;
            rowMaxHeight = 0;
            classInRow = 0;
        }

        // Position fields within class
        const fields = grouped[className];
        fields.forEach((d, idx) => {
            const fieldRow = Math.floor(idx / fieldsPerRow);
            const fieldCol = idx % fieldsPerRow;
            const fieldX = currentX + classPadding + fieldCol * (fieldWidth + fieldGapX) + fieldWidth / 2;
            const fieldY = currentY + classPadding + 20 + fieldRow * (fieldHeight + fieldGapY) + fieldHeight / 2;
            const key = `${d.class_name}.${d.field_name}`;
            positions[key] = { x: fieldX, y: fieldY };
        });

        currentX += info.width + classGapX;
        rowMaxHeight = Math.max(rowMaxHeight, info.height);
        classInRow++;
    });

    // Build Cytoscape elements with positions
    // NOTE: For compound nodes, do NOT set position on parent nodes -
    // Cytoscape calculates parent bounds automatically from children
    const elements = [];

    classNames.forEach(className => {
        // Class node (parent) - NO position set, Cytoscape auto-calculates from children
        elements.push({
            data: { id: `class_${className}`, label: className, type: 'class' }
        });

        // Field nodes (children) - positions set explicitly
        grouped[className].forEach(d => {
            const key = `${d.class_name}.${d.field_name}`;
            const isManual = d.type === 'manual';
            const isEnabled = isManual ? true : (derivationSelections[key] || false);

            // Determine CSS classes based on type and state
            let nodeClasses = '';
            if (isManual) {
                nodeClasses = 'manual';
            } else if (isEnabled) {
                nodeClasses = 'enabled';
            } else {
                nodeClasses = 'disabled';
            }

            elements.push({
                data: {
                    id: key,
                    label: d.field_name,
                    parent: `class_${d.class_name}`,
                    type: 'field',
                    derivationType: d.type || 'auto',  // 'auto' or 'manual'
                    enabled: isEnabled
                },
                classes: nodeClasses,
                position: positions[key]
            });
        });
    });

    // Initialize or update Cytoscape
    if (derivationCy) {
        derivationCy.destroy();
    }

    const container = document.getElementById('derivation-graph');
    if (!container) {
        console.error('Derivation graph container not found');
        return;
    }

    derivationCy = cytoscape({
        container: container,
        elements: elements,
        style: [
            // Class (parent) nodes
            {
                selector: 'node[type="class"]',
                style: {
                    'background-color': '#e1bee7',
                    'border-color': '#9c27b0',
                    'border-width': 2,
                    'label': 'data(label)',
                    'text-valign': 'top',
                    'text-halign': 'center',
                    'font-size': '10px',
                    'font-weight': 'bold',
                    'color': '#6a1b9a',
                    'padding': '10px',
                    'shape': 'roundrectangle',
                    'text-margin-y': -8
                }
            },
            // Field nodes - default/disabled style (auto-generated)
            {
                selector: 'node[type="field"]',
                style: {
                    'background-color': '#e0e0e0',
                    'border-color': '#999',
                    'border-width': 1,
                    'label': 'data(label)',
                    'text-valign': 'center',
                    'text-halign': 'center',
                    'font-size': '8px',
                    'color': '#666',
                    'width': fieldWidth,
                    'height': fieldHeight,
                    'shape': 'roundrectangle',
                    'cursor': 'pointer'
                }
            },
            // Field nodes - enabled (auto-generated, class-based for reliable updates)
            {
                selector: 'node[type="field"].enabled',
                style: {
                    'background-color': '#9c27b0',
                    'border-color': '#6a1b9a',
                    'border-width': 2,
                    'color': '#fff'
                }
            },
            // Field nodes - manual derivations (orange/gold, always enabled, not clickable)
            {
                selector: 'node[type="field"].manual',
                style: {
                    'background-color': '#ff9800',
                    'border-color': '#e65100',
                    'border-width': 2,
                    'border-style': 'dashed',
                    'color': '#fff',
                    'cursor': 'default'
                }
            }
        ],
        layout: { name: 'preset' },
        userZoomingEnabled: true,
        userPanningEnabled: true,
        boxSelectionEnabled: false
    });

    // Fit to view
    derivationCy.fit(20);

    // Click handler for field nodes - toggle enabled/disabled (only for auto-generated)
    derivationCy.on('tap', 'node[type="field"]', function(evt) {
        const node = evt.target;

        // Skip manual derivations - they are always enabled
        if (node.data('derivationType') === 'manual') {
            return;
        }

        const key = node.id();
        const currentEnabled = node.data('enabled');
        const newEnabled = !currentEnabled;

        // Toggle
        derivationSelections[key] = newEnabled;
        node.data('enabled', newEnabled);

        // Force style update by toggling classes
        if (newEnabled) {
            node.addClass('enabled');
            node.removeClass('disabled');
        } else {
            node.addClass('disabled');
            node.removeClass('enabled');
        }

        updateDerivationCount();
    });

    // Click handler for class nodes - toggle all auto-generated fields in class
    derivationCy.on('tap', 'node[type="class"]', function(evt) {
        const classNode = evt.target;
        const classId = classNode.id();

        // Get all field nodes in this class (only auto-generated, not manual)
        const childNodes = derivationCy.nodes(`[parent="${classId}"]`);
        const autoNodes = childNodes.filter(n => n.data('derivationType') !== 'manual');

        // Skip if no auto-generated nodes
        if (autoNodes.length === 0) {
            return;
        }

        // Check if all auto-generated are enabled
        const allEnabled = autoNodes.every(n => n.data('enabled'));
        const newEnabled = !allEnabled;

        // Toggle all auto-generated to opposite state (skip manual)
        autoNodes.forEach(node => {
            const key = node.id();
            derivationSelections[key] = newEnabled;
            node.data('enabled', newEnabled);

            // Force style update
            if (newEnabled) {
                node.addClass('enabled');
                node.removeClass('disabled');
            } else {
                node.addClass('disabled');
                node.removeClass('enabled');
            }
        });

        updateDerivationCount();
    });
}

// Filter graph based on search/class filter
function filterDerivationGraph() {
    if (derivationCy) {
        renderDerivationGraph();
    }
}

// Toggle derivation (for compatibility)
function toggleDerivation(key) {
    derivationSelections[key] = !derivationSelections[key];
    if (derivationCy) {
        const node = derivationCy.getElementById(key);
        if (node) {
            node.data('enabled', derivationSelections[key]);
        }
    }
    updateDerivationCount();
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
            if (derivationCy) {
                derivationCy.nodes('[type="field"]').forEach(node => {
                    node.data('enabled', true);
                });
            }
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
            if (derivationCy) {
                derivationCy.nodes('[type="field"]').forEach(node => {
                    node.data('enabled', false);
                });
            }
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
