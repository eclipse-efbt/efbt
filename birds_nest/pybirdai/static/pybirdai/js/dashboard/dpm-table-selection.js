// coding=UTF-8
// Copyright (c) 2025 Bird Software Solutions Ltd
// SPDX-License-Identifier: EPL-2.0

/**
 * DPM Table Selection Modal Functions
 * Handles: Table selection, presets, filtering for DPM workflow
 */

let dpmTablesData = [];
let dpmPresetsData = {};
let dpmCurrentTaskId = null;

function showDPMTableSelectionModal(taskId) {
    dpmCurrentTaskId = taskId;
    document.getElementById('dpmTableSelectionModal').style.display = 'block';
    document.body.classList.add('modal-open');

    // Load tables and presets
    loadDPMTables();
    loadDPMPresets();
}

function closeDPMTableSelectionModal() {
    document.getElementById('dpmTableSelectionModal').style.display = 'none';
    document.body.classList.remove('modal-open');
}

function loadDPMTables() {
    fetch('/pybirdai/workflow/dpm/get-available-tables/')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                dpmTablesData = data.tables;
                renderDPMTables();
                populateFrameworkFilter();
                updateDPMTableCount(dpmTablesData.length);
            } else {
                showDPMModalStatus('Error loading tables: ' + (data.error || 'Unknown error'), 'error');
            }
        })
        .catch(error => {
            console.error('Error loading DPM tables:', error);
            showDPMModalStatus('Error loading tables', 'error');
        });
}

function renderDPMTables() {
    const tbody = document.getElementById('dpm-tables-tbody');

    if (dpmTablesData.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" style="padding: 20px; text-align: center; color: #666;">No tables available</td></tr>';
        return;
    }

    tbody.innerHTML = dpmTablesData.map(table => `
        <tr class="dpm-table-row" data-table-id="${table.table_id}" data-framework="${table.framework || 'unknown'}" onclick="toggleDPMTableRow(this, event)">
            <td style="padding: 10px; border-bottom: 1px solid #e2e8f0;">
                <input type="checkbox" class="dpm-table-checkbox" value="${table.table_id}" onchange="updateDPMSelectionCount(); updateRowSelectedClass(this);" onclick="event.stopPropagation();" />
            </td>
            <td style="padding: 10px; border-bottom: 1px solid #e2e8f0; font-family: monospace; font-size: 12px;">${table.table_code || table.table_id}</td>
            <td style="padding: 10px; border-bottom: 1px solid #e2e8f0;">${table.table_name || ''}</td>
            <td style="padding: 10px; border-bottom: 1px solid #e2e8f0;">
                <span style="padding: 3px 8px; background: #e3f2fd; color: #1976d2; border-radius: 3px; font-size: 11px; font-weight: 600;">
                    ${table.framework || 'N/A'}
                </span>
            </td>
        </tr>
    `).join('');

    updateDPMSelectionCount();
}

function toggleDPMTableRow(row, event) {
    const checkbox = row.querySelector('.dpm-table-checkbox');
    if (checkbox) {
        checkbox.checked = !checkbox.checked;
        updateRowSelectedClass(checkbox);
        updateDPMSelectionCount();
    }
}

function updateRowSelectedClass(checkbox) {
    const row = checkbox.closest('tr');
    if (row) {
        if (checkbox.checked) {
            row.classList.add('selected');
        } else {
            row.classList.remove('selected');
        }
    }
}

function populateFrameworkFilter() {
    const frameworks = [...new Set(dpmTablesData.map(t => t.framework).filter(f => f))];
    const select = document.getElementById('dpm-framework-filter');

    select.innerHTML = '<option value="">All Frameworks</option>' +
        frameworks.map(fw => `<option value="${fw}">${fw}</option>`).join('');
}

function updateDPMTableCount(count) {
    document.getElementById('dpm-table-count').textContent = `${count} tables available for selection`;
}

function filterDPMTables() {
    const searchTerm = document.getElementById('dpm-table-search').value.toLowerCase();
    const framework = document.getElementById('dpm-framework-filter').value;

    const rows = document.querySelectorAll('.dpm-table-row');
    let visibleCount = 0;

    rows.forEach(row => {
        const tableId = row.dataset.tableId.toLowerCase();
        const tableCode = row.querySelector('td:nth-child(2)').textContent.toLowerCase();
        const tableName = row.querySelector('td:nth-child(3)').textContent.toLowerCase();
        const tableFramework = row.dataset.framework;

        const matchesSearch = !searchTerm || tableId.includes(searchTerm) ||
                            tableCode.includes(searchTerm) || tableName.includes(searchTerm);
        const matchesFramework = !framework || tableFramework === framework;

        if (matchesSearch && matchesFramework) {
            row.style.display = '';
            visibleCount++;
        } else {
            row.style.display = 'none';
        }
    });

    updateDPMTableCount(visibleCount);
}

function toggleAllDPMTables() {
    const selectAllCheckbox = document.getElementById('select-all-checkbox');
    const checkboxes = document.querySelectorAll('.dpm-table-checkbox');
    const visibleCheckboxes = Array.from(checkboxes).filter(cb =>
        cb.closest('tr').style.display !== 'none'
    );

    visibleCheckboxes.forEach(checkbox => {
        checkbox.checked = selectAllCheckbox.checked;
        updateRowSelectedClass(checkbox);
    });

    updateDPMSelectionCount();
}

function selectAllDPMTables() {
    const checkboxes = document.querySelectorAll('.dpm-table-checkbox');
    const visibleCheckboxes = Array.from(checkboxes).filter(cb =>
        cb.closest('tr').style.display !== 'none'
    );

    visibleCheckboxes.forEach(checkbox => {
        checkbox.checked = true;
        updateRowSelectedClass(checkbox);
    });
    document.getElementById('select-all-checkbox').checked = true;
    updateDPMSelectionCount();
}

function clearAllDPMTables() {
    const checkboxes = document.querySelectorAll('.dpm-table-checkbox');
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
        updateRowSelectedClass(checkbox);
    });
    document.getElementById('select-all-checkbox').checked = false;
    updateDPMSelectionCount();
}

function updateDPMSelectionCount() {
    const selected = document.querySelectorAll('.dpm-table-checkbox:checked').length;
    document.getElementById('dpm-selected-count').textContent = selected;

    // Update select-all checkbox state
    const allCheckboxes = document.querySelectorAll('.dpm-table-checkbox');
    const visibleCheckboxes = Array.from(allCheckboxes).filter(cb =>
        cb.closest('tr').style.display !== 'none'
    );
    const allVisibleChecked = visibleCheckboxes.length > 0 &&
                             visibleCheckboxes.every(cb => cb.checked);
    document.getElementById('select-all-checkbox').checked = allVisibleChecked;
}

function confirmDPMTableSelection() {
    const selectedCheckboxes = document.querySelectorAll('.dpm-table-checkbox:checked');
    const selectedTables = Array.from(selectedCheckboxes).map(cb => cb.value);

    if (selectedTables.length === 0) {
        showDPMModalStatus('Please select at least one table', 'warning');
        return;
    }

    showDPMModalStatus(`Starting Step 2 with ${selectedTables.length} selected table(s)...`, 'info');

    // Close modal and execute Step 2 with selected tables
    setTimeout(() => {
        closeDPMTableSelectionModal();
        // Execute Step 2 with the selected tables
        executeDPMStep(2, {
            selected_tables: selectedTables,
            fromModal: true
        });
    }, 500);
}

function loadDPMPresets() {
    fetch('/pybirdai/workflow/dpm/presets/')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                dpmPresetsData = data.presets || {};
                renderDPMPresetSelector();
            }
        })
        .catch(error => {
            console.error('Error loading DPM presets:', error);
        });
}

function renderDPMPresetSelector() {
    const select = document.getElementById('dpm-preset-selector');
    select.innerHTML = '<option value="">-- Select a preset --</option>' +
        Object.keys(dpmPresetsData).map(name =>
            `<option value="${name}">${name}</option>`
        ).join('');
}

function loadDPMPreset() {
    const presetName = document.getElementById('dpm-preset-selector').value;

    if (!presetName || !dpmPresetsData[presetName]) {
        return;
    }

    const tableIds = dpmPresetsData[presetName];

    // Clear all checkboxes first
    clearAllDPMTables();

    // Check the tables in the preset
    tableIds.forEach(tableId => {
        const checkbox = document.querySelector(`.dpm-table-checkbox[value="${tableId}"]`);
        if (checkbox) {
            checkbox.checked = true;
            updateRowSelectedClass(checkbox);
        }
    });

    updateDPMSelectionCount();
    showDPMModalStatus(`Loaded preset: ${presetName}`, 'success');
}

function saveDPMPreset() {
    const presetName = prompt('Enter a name for this preset:');

    if (!presetName || presetName.trim() === '') {
        return;
    }

    const selectedCheckboxes = document.querySelectorAll('.dpm-table-checkbox:checked');
    const selectedTables = Array.from(selectedCheckboxes).map(cb => cb.value);

    if (selectedTables.length === 0) {
        showDPMModalStatus('Please select at least one table to save as a preset', 'warning');
        return;
    }

    fetch('/pybirdai/workflow/dpm/presets/', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCookie('csrftoken')
        },
        body: JSON.stringify({
            task_id: dpmCurrentTaskId,
            preset_name: presetName.trim(),
            table_ids: selectedTables
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            dpmPresetsData[presetName.trim()] = selectedTables;
            renderDPMPresetSelector();
            document.getElementById('dpm-preset-selector').value = presetName.trim();
            showDPMModalStatus(`Preset "${presetName}" saved successfully`, 'success');
        } else {
            showDPMModalStatus('Error saving preset: ' + (data.error || 'Unknown error'), 'error');
        }
    })
    .catch(error => {
        console.error('Error saving preset:', error);
        showDPMModalStatus('Error saving preset', 'error');
    });
}

function deleteDPMPreset() {
    const presetName = document.getElementById('dpm-preset-selector').value;

    if (!presetName) {
        showDPMModalStatus('Please select a preset to delete', 'warning');
        return;
    }

    if (!confirm(`Are you sure you want to delete the preset "${presetName}"?`)) {
        return;
    }

    fetch('/pybirdai/workflow/dpm/presets/', {
        method: 'DELETE',
        headers: {
            'Content-Type': 'application/json',
            'X-CSRFToken': getCookie('csrftoken')
        },
        body: JSON.stringify({
            task_id: dpmCurrentTaskId,
            preset_name: presetName
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            delete dpmPresetsData[presetName];
            renderDPMPresetSelector();
            showDPMModalStatus(`Preset "${presetName}" deleted successfully`, 'success');
        } else {
            showDPMModalStatus('Error deleting preset: ' + (data.error || 'Unknown error'), 'error');
        }
    })
    .catch(error => {
        console.error('Error deleting preset:', error);
        showDPMModalStatus('Error deleting preset', 'error');
    });
}

function showDPMModalStatus(message, type) {
    const statusEl = document.getElementById('dpm-modal-status');
    statusEl.textContent = message;
    statusEl.style.color = type === 'success' ? '#28a745' : type === 'error' ? '#dc3545' : type === 'warning' ? '#ffc107' : '#666';
}
