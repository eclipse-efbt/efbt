// coding=UTF-8
// Copyright (c) 2025 Bird Software Solutions Ltd
// SPDX-License-Identifier: EPL-2.0

/**
 * Join Configuration Modal Core Functions
 * Handles: Modal show/close, framework loading, file operations, tab switching
 */

// Join Configuration State
let currentFramework = 'FINREP_REF';
let currentTab = 'in_scope_reports';
let editors = {};
const fileTypes = ['in_scope_reports', 'product_to_category', 'product_il_definitions'];

// Note: getCookie and csrftoken are defined in utils.js which is loaded first

function showJoinConfigModal(preselectedFramework = null) {
    document.getElementById('joinConfigModal').style.display = 'block';
    document.body.classList.add('modal-open');

    // Load frameworks with optional preselection
    loadFrameworks(preselectedFramework);

    // Initialize CodeMirror editors after modal is visible
    setTimeout(() => {
        if (!editors['in_scope_reports']) {
            fileTypes.forEach(fileType => {
                editors[fileType] = CodeMirror.fromTextArea(document.getElementById(`editor-${fileType}`), {
                    mode: 'text/plain',
                    theme: 'monokai',
                    lineNumbers: true,
                    lineWrapping: false,
                    indentUnit: 4,
                    extraKeys: {
                        "Ctrl-S": function() { saveCurrentTab(); },
                        "Cmd-S": function() { saveCurrentTab(); }
                    }
                });
            });
        }

        // Refresh current editor
        if (editors[currentTab]) {
            editors[currentTab].refresh();
        }

        // Load initial content
        loadCSV(currentTab);
    }, 100);
}

function closeJoinConfigModal() {
    document.getElementById('joinConfigModal').style.display = 'none';
    document.body.classList.remove('modal-open');
}

function loadFrameworks(preselectedFramework = null) {
    fetch('/pybirdai/joins-config/list-frameworks/')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                const select = document.getElementById('frameworkSelect');
                const display = document.getElementById('frameworkDisplay');
                select.innerHTML = '';

                // Determine which framework to select
                // Priority: preselected > existing in list > default
                let frameworkToSelect = preselectedFramework || data.default;

                // If preselected framework isn't in list, fall back to default
                if (preselectedFramework && !data.frameworks.includes(preselectedFramework)) {
                    console.warn(`Framework ${preselectedFramework} not found, using default`);
                    frameworkToSelect = data.default;
                }

                data.frameworks.forEach(fw => {
                    const option = document.createElement('option');
                    option.value = fw;
                    option.textContent = fw;
                    if (fw === frameworkToSelect) {
                        option.selected = true;
                        currentFramework = fw;
                    }
                    select.appendChild(option);
                });

                // Update the display element
                if (display) {
                    display.textContent = currentFramework;
                }

                // Load file info
                loadFileInfo();
            }
        })
        .catch(error => {
            console.error('Error loading frameworks:', error);
            showStatus('Error loading frameworks', 'error');
        });
}

function switchFramework() {
    const select = document.getElementById('frameworkSelect');
    currentFramework = select.value;

    // Clear visual editor state before loading new framework data
    if (typeof ilCytoscape !== 'undefined' && ilCytoscape) {
        ilCytoscape.elements().remove();
        ilJoinsData = [];
        renderJoinsList();
        updateInstructions();
    }

    // Reload current tab content
    loadCSV(currentTab);

    // Update file info
    loadFileInfo();
}

function loadFileInfo() {
    fetch(`/pybirdai/joins-config/file-info/?framework=${currentFramework}`)
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                fileTypes.forEach(fileType => {
                    const info = data.files[fileType];
                    const badge = document.getElementById(`badge-${fileType}`);
                    const fileInfo = document.getElementById(`file-info-${fileType}`);

                    if (info && info.exists) {
                        badge.textContent = info.row_count;
                        badge.style.background = '#28a745';
                        fileInfo.textContent = `${info.description} • ${info.row_count} rows • ${(info.size / 1024).toFixed(1)} KB`;
                    } else {
                        badge.textContent = '0';
                        badge.style.background = '#6c757d';
                        fileInfo.textContent = `${info.description} • File not found`;
                    }
                });
            }
        })
        .catch(error => {
            console.error('Error loading file info:', error);
        });
}

function switchTab(tabName) {
    // Don't load CSV for create_framework tab
    if (tabName === 'create_framework') {
        currentTab = tabName;

        // Hide all tabs
        document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active'));

        // Show selected tab
        document.getElementById(`tab-${tabName}`).classList.add('active');
        document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');

        // Hide save button for create tab
        document.getElementById('saveButton').style.display = 'none';
        return;
    }

    // Show save button for editor tabs
    document.getElementById('saveButton').style.display = 'inline-block';

    currentTab = tabName;

    // Hide all tabs
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active'));

    // Show selected tab
    document.getElementById(`tab-${tabName}`).classList.add('active');
    document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');

    // Load content and refresh editor
    loadCSV(tabName);

    // Initialize IL canvas if switching to IL definitions tab
    if (tabName === 'product_il_definitions') {
        setTimeout(() => {
            if (typeof initILCanvas === 'function') {
                initILCanvas();
            }
            if (typeof loadAvailableTables === 'function') {
                loadAvailableTables();
            }
            if (ilCurrentView === 'visual' && typeof syncTextToVisual === 'function') {
                syncTextToVisual();
            }
        }, 200);
    }
}

function loadCSV(fileType) {
    showStatus('Loading...', 'info');

    fetch('/pybirdai/joins-config/load/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrftoken
        },
        body: JSON.stringify({
            file_type: fileType,
            framework: currentFramework
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            if (editors[fileType]) {
                editors[fileType].setValue(data.content);
                editors[fileType].refresh();
            }
            // Sync visual editor when IL definitions are loaded
            if (fileType === 'product_il_definitions' && ilCurrentView === 'visual' && typeof syncTextToVisual === 'function') {
                setTimeout(() => syncTextToVisual(), 100);
            }
            showStatus(data.exists ? 'File loaded' : 'New file (not saved yet)', data.exists ? 'success' : 'warning');
        } else {
            showStatus('Error: ' + data.error, 'error');
        }
    })
    .catch(error => {
        console.error('Error loading CSV:', error);
        showStatus('Error loading file', 'error');
    });
}

function saveCurrentTab() {
    if (currentTab === 'create_framework') return;

    // Sync visual to text before saving IL definitions
    if (currentTab === 'product_il_definitions' && ilCurrentView === 'visual' && typeof syncVisualToText === 'function') {
        syncVisualToText();
    }

    showStatus('Saving...', 'info');
    document.getElementById('saveButton').disabled = true;

    const content = editors[currentTab].getValue();

    fetch('/pybirdai/joins-config/save/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrftoken
        },
        body: JSON.stringify({
            file_type: currentTab,
            framework: currentFramework,
            content: content
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showStatus('✓ ' + data.message, 'success');
            loadFileInfo(); // Refresh file info
        } else {
            showStatus('Error: ' + data.error, 'error');
        }
    })
    .catch(error => {
        console.error('Error saving CSV:', error);
        showStatus('Error saving file', 'error');
    })
    .finally(() => {
        document.getElementById('saveButton').disabled = false;
    });
}

function downloadCSV(fileType) {
    const content = editors[fileType].getValue();
    const blob = new Blob([content], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${fileType}_${currentFramework}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    showStatus('File downloaded', 'success');
}

function createNewFramework() {
    const frameworkName = document.getElementById('newFrameworkName').value.trim();
    const template = document.querySelector('input[name="templateStyle"]:checked').value;

    if (!frameworkName) {
        showCreateStatus('Please enter a framework name', 'error');
        return;
    }

    showCreateStatus('Creating framework...', 'info');

    fetch('/pybirdai/joins-config/create-framework/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': csrftoken
        },
        body: JSON.stringify({
            framework_name: frameworkName,
            template: template
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showCreateStatus('✓ ' + data.message, 'success');

            // Reload frameworks
            setTimeout(() => {
                loadFrameworks();

                // Switch to new framework
                setTimeout(() => {
                    document.getElementById('frameworkSelect').value = data.framework_name;
                    currentFramework = data.framework_name;
                    loadFileInfo();

                    // Switch to first tab
                    switchTab('in_scope_reports');
                }, 500);
            }, 1000);
        } else {
            showCreateStatus('Error: ' + data.error, 'error');
        }
    })
    .catch(error => {
        console.error('Error creating framework:', error);
        showCreateStatus('Error creating framework', 'error');
    });
}

function showStatus(message, type) {
    const statusEl = document.getElementById('modal-status');
    statusEl.textContent = message;
    statusEl.style.color = type === 'success' ? '#28a745' : type === 'error' ? '#dc3545' : type === 'warning' ? '#ffc107' : '#666';
}

function showCreateStatus(message, type) {
    const statusEl = document.getElementById('create-framework-status');
    statusEl.textContent = message;
    statusEl.style.display = 'block';
    statusEl.style.background = type === 'success' ? '#d4edda' : type === 'error' ? '#f8d7da' : type === 'warning' ? '#fff3cd' : '#d1ecf1';
    statusEl.style.color = type === 'success' ? '#155724' : type === 'error' ? '#721c24' : type === 'warning' ? '#856404' : '#0c5460';
    statusEl.style.border = '1px solid ' + (type === 'success' ? '#c3e6cb' : type === 'error' ? '#f5c6cb' : type === 'warning' ? '#ffeaa7' : '#bee5eb');
}
