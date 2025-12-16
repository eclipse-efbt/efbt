// coding=UTF-8
// Copyright (c) 2025 Bird Software Solutions Ltd
// SPDX-License-Identifier: EPL-2.0

/**
 * IL Visual Editor Functions
 * Handles: Canvas rendering, table chips, join creation wizard,
 * join list management, text/visual sync
 */

// IL Visual Editor State
let ilCanvasInitialized = false;
let ilNodes = {};  // {id: {id, name}}
let ilJoinsData = [];  // Array of join definitions: {name, mainTable, filterTable, filter, relatedTables, comments}
let ilSelectedNodes = new Set();  // Set of selected node IDs (for creating new joins)
let ilSelectedJoinIndex = -1;  // Currently highlighted join (-1 = none)
let ilCurrentView = 'visual';  // 'visual' or 'text'
let ilAvailableTables = [];  // All available tables from API
let ilTablesFilterText = '';  // Current filter text for tables
let ilJoinsFilterText = '';  // Current filter text for joins

// Wizard state
let wizardStep = 1;
let wizardSelectedMainTable = null;
let wizardSelectedTables = [];

function initILCanvas() {
    if (ilCanvasInitialized) return;

    const container = document.getElementById('il-selected-tables');
    if (!container) return;

    // Setup click handler for deselect on container click
    container.addEventListener('click', function(e) {
        if (e.target === container || e.target.id === 'il-tables-chips') {
            deselectAllNodes();
        }
    });

    ilCanvasInitialized = true;
    loadJoinsToCanvas();
}

function drawAllArrows() {
    // No-op: removed canvas arrows in favor of clean card layout
}

function updateSelectionBar() {
    const createJoinBtn = document.getElementById('create-join-btn');
    const count = ilSelectedNodes.size;

    if (createJoinBtn) {
        createJoinBtn.disabled = count < 1;
    }
}

function updateInstructions() {
    const emptyState = document.getElementById('il-empty-state');
    const tablesCanvas = document.getElementById('il-tables-canvas');
    const countBadge = document.getElementById('il-selected-count');
    const nodeCount = Object.keys(ilNodes).length;

    if (countBadge) {
        countBadge.textContent = nodeCount;
    }

    if (nodeCount > 0) {
        if (emptyState) emptyState.style.display = 'none';
        if (tablesCanvas) tablesCanvas.style.display = 'flex';
        renderCanvas();
    } else {
        if (emptyState) emptyState.style.display = 'block';
        if (tablesCanvas) tablesCanvas.style.display = 'none';
    }
}

// Render the canvas - single flat grid with all tables (each appears ONCE)
function renderCanvas() {
    const joinsSectionsEl = document.getElementById('il-joins-sections');
    const unusedSectionEl = document.getElementById('il-unused-section');
    const unusedChipsEl = document.getElementById('il-unused-chips');

    if (!joinsSectionsEl || !unusedSectionEl || !unusedChipsEl) return;

    // Build map: which joins use each table, and with what role
    const tableJoins = {};  // {tableId: [{joinIndex, role, joinName}]}
    ilJoinsData.forEach((join, idx) => {
        if (join.mainTable) {
            if (!tableJoins[join.mainTable]) tableJoins[join.mainTable] = [];
            tableJoins[join.mainTable].push({ joinIndex: idx, role: 'main', joinName: join.name });
        }
        if (join.filterTable) {
            if (!tableJoins[join.filterTable]) tableJoins[join.filterTable] = [];
            tableJoins[join.filterTable].push({ joinIndex: idx, role: 'filter', joinName: join.name });
        }
        (join.relatedTables || []).forEach(t => {
            if (!tableJoins[t]) tableJoins[t] = [];
            tableJoins[t].push({ joinIndex: idx, role: 'related', joinName: join.name });
        });
    });

    // Get role for a table in the SELECTED join (if any)
    function getRoleInSelectedJoin(tableId) {
        if (ilSelectedJoinIndex < 0) return null;
        const joins = tableJoins[tableId] || [];
        const match = joins.find(j => j.joinIndex === ilSelectedJoinIndex);
        return match ? match.role : null;
    }

    // Separate tables into: used (in at least one join) and unused
    const usedTables = Object.keys(ilNodes).filter(id => tableJoins[id] && tableJoins[id].length > 0);
    const unusedTables = Object.keys(ilNodes).filter(id => !tableJoins[id] || tableJoins[id].length === 0);

    // Render single grid of all used tables
    if (usedTables.length > 0) {
        joinsSectionsEl.innerHTML = `
            <div class="canvas-section">
                <div class="canvas-section-header">
                    <span class="canvas-section-title">
                        <i class="fas fa-cubes" style="color: #4a90d9;"></i> Tables in Joins
                    </span>
                    <span style="font-size: 11px; color: #888;">
                        ${usedTables.length} tables
                        ${ilSelectedJoinIndex >= 0 ? `<span style="color: #ffc107; margin-left: 8px;"><i class="fas fa-eye"></i> Viewing: ${ilJoinsData[ilSelectedJoinIndex]?.name || ''}</span>` : ''}
                    </span>
                </div>
                <div class="canvas-section-chips">
                    ${usedTables.map(tableId => {
                        const roleInSelected = getRoleInSelectedJoin(tableId);
                        const joinCount = (tableJoins[tableId] || []).length;
                        return createChipHTML(tableId, roleInSelected, joinCount, ilSelectedJoinIndex >= 0);
                    }).join('')}
                </div>
            </div>
        `;
    } else {
        joinsSectionsEl.innerHTML = '';
    }

    // Render unused tables section
    if (unusedTables.length > 0) {
        unusedSectionEl.style.display = 'block';
        unusedChipsEl.innerHTML = unusedTables.map(id => createChipHTML(id, null, 0, false)).join('');
    } else {
        unusedSectionEl.style.display = 'none';
    }
}

// Create chip HTML - role is the role in SELECTED join (null if not in selected join or no join selected)
function createChipHTML(tableId, roleInSelectedJoin, joinCount, isJoinSelected) {
    const icons = {
        main: '<i class="fas fa-star chip-icon" title="Main table"></i>',
        filter: '<i class="fas fa-filter chip-icon" title="Filter table"></i>',
        related: '<i class="fas fa-cube chip-icon" title="Related table"></i>'
    };

    const label = tableId.length > 18 ? tableId.substring(0, 16) + '...' : tableId;
    const isSelected = ilSelectedNodes.has(tableId);

    // Determine visual state
    let roleClass = 'role-unused';
    let icon = '';

    if (isJoinSelected) {
        if (roleInSelectedJoin) {
            // Table is in the selected join - show its role
            roleClass = `role-${roleInSelectedJoin}`;
            icon = icons[roleInSelectedJoin] || '';
        } else if (joinCount > 0) {
            // Table is in OTHER joins but not the selected one - dimmed
            roleClass = 'role-dimmed';
            icon = '<i class="fas fa-cube chip-icon" style="opacity: 0.5;"></i>';
        } else {
            // Unused table
            roleClass = 'role-unused';
        }
    } else {
        // No join selected
        if (joinCount > 0) {
            roleClass = 'role-in-joins';
            icon = '<i class="fas fa-cube chip-icon"></i>';
        } else {
            roleClass = 'role-unused';
        }
    }

    // Badge showing number of joins this table belongs to
    const badge = joinCount > 0 ? `<span class="chip-badge" title="Used in ${joinCount} join${joinCount > 1 ? 's' : ''}">${joinCount}</span>` : '';

    return `
        <div class="il-table-chip ${roleClass}${isSelected ? ' selected' : ''}"
             id="il-chip-${tableId}"
             data-table-id="${tableId}"
             data-join-count="${joinCount}"
             title="${tableId} (${joinCount} join${joinCount !== 1 ? 's' : ''})"
             onclick="toggleNodeSelection('${tableId}')">
            ${icon}
            <span class="chip-label">${label}</span>
            ${badge}
            <button class="chip-delete" onclick="event.stopPropagation(); removeTableFromCanvas('${tableId}')" title="Remove">&times;</button>
        </div>
    `;
}

// Toggle join selection (single selection mode)
function toggleJoinSelection(joinIndex) {
    if (ilSelectedJoinIndex === joinIndex) {
        // Deselect
        ilSelectedJoinIndex = -1;
    } else {
        // Select this join
        ilSelectedJoinIndex = joinIndex;
    }
    renderCanvas();
    renderJoinsList();  // Update right sidebar to show active state
}

// ============================================================
// Tables Sidebar (Left) - Load, Filter, Add/Remove
// ============================================================

function loadAvailableTables() {
    const listContainer = document.getElementById('il-tables-list');
    const countBadge = document.getElementById('il-tables-count');

    listContainer.innerHTML = '<p style="color: #999; font-size: 12px; text-align: center; margin-top: 20px;"><i class="fas fa-spinner fa-spin"></i> Loading...</p>';

    // Pass framework to filter tables by IL model
    fetch(`/pybirdai/joins-config/il-tables/?framework=${encodeURIComponent(currentFramework)}`)
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                ilAvailableTables = data.tables;
                countBadge.textContent = ilAvailableTables.length;
                renderTablesList();
            } else {
                listContainer.innerHTML = '<p style="color: #dc3545; font-size: 12px; text-align: center;">Error loading tables</p>';
            }
        })
        .catch(error => {
            console.error('Error loading tables:', error);
            listContainer.innerHTML = '<p style="color: #dc3545; font-size: 12px; text-align: center;">Error loading tables</p>';
        });
}

function filterTablesList(query) {
    ilTablesFilterText = query.toLowerCase().trim();
    renderTablesList();
}

function renderTablesList() {
    const listContainer = document.getElementById('il-tables-list');

    // Get tables on canvas
    const tablesOnCanvas = new Set(Object.keys(ilNodes));

    // Filter tables
    let filtered = ilAvailableTables;
    if (ilTablesFilterText) {
        filtered = ilAvailableTables.filter(t =>
            t.id.toLowerCase().includes(ilTablesFilterText) ||
            (t.name && t.name.toLowerCase().includes(ilTablesFilterText))
        );
    }

    if (filtered.length === 0) {
        listContainer.innerHTML = '<p style="color: #999; font-size: 12px; text-align: center; margin-top: 20px;">No tables found</p>';
        return;
    }

    listContainer.innerHTML = filtered.map(table => {
        const onCanvas = tablesOnCanvas.has(table.id);
        return `
            <div class="il-table-item ${onCanvas ? 'on-canvas' : ''}" data-table-id="${table.id}">
                <span class="table-name" title="${table.id}">${table.id}</span>
                <button class="add-btn" onclick="event.stopPropagation(); addTableToCanvas('${table.id}', '${(table.name || table.id).replace(/'/g, "\\'")}')">
                    <i class="fas fa-plus"></i>
                </button>
                <button class="remove-btn" onclick="event.stopPropagation(); removeTableFromCanvas('${table.id}')">
                    <i class="fas fa-times"></i>
                </button>
            </div>
        `;
    }).join('');
}

function addTableToCanvas(tableId, tableName) {
    if (!ilCanvasInitialized) {
        initILCanvas();
    }

    // Check if already on canvas
    if (ilNodes[tableId]) {
        showStatus(`"${tableId}" is already selected`, 'warning');
        return;
    }

    // Store node data
    ilNodes[tableId] = { id: tableId, name: tableName };

    // Update UI
    renderTablesList();
    updateInstructions();

    showStatus(`Added "${tableId}"`, 'success');
}

function toggleNodeSelection(tableId) {
    if (ilSelectedNodes.has(tableId)) {
        ilSelectedNodes.delete(tableId);
    } else {
        ilSelectedNodes.add(tableId);
    }
    updateSelectionBar();
    renderCanvas();
}

function removeTableFromCanvas(tableId) {
    // Remove from data
    delete ilNodes[tableId];
    ilSelectedNodes.delete(tableId);

    // Update joins (remove table from joins or remove joins entirely)
    updateJoinsFromCanvas();
    renderTablesList();
    updateInstructions();
    updateSelectionBar();

    showStatus(`Removed "${tableId}"`, 'success');
}

// ============================================================
// Joins Sidebar (Right) - Filter
// ============================================================

function filterJoinsList(query) {
    ilJoinsFilterText = query.toLowerCase().trim();
    renderJoinsList();
}

function clearCanvas() {
    clearSelectedTables();
}

function clearSelectedTables() {
    const nodeCount = Object.keys(ilNodes).length;
    if (nodeCount === 0) {
        showStatus('No tables selected', 'info');
        return;
    }

    if (confirm('Clear all selected tables? This will also remove all join definitions.')) {
        // Clear data
        ilNodes = {};
        ilSelectedNodes.clear();
        ilJoinsData = [];
        ilSelectedJoinIndex = -1;

        // Update UI
        renderJoinsList();
        renderTablesList();
        updateInstructions();
        updateSelectionBar();

        showStatus('All tables cleared', 'success');
    }
}

function deselectAllNodes() {
    // Remove selected class from all chips
    document.querySelectorAll('.il-table-chip.selected').forEach(chip => {
        chip.classList.remove('selected');
        const statusEl = chip.querySelector('.chip-status');
        if (statusEl) statusEl.textContent = '○';
    });
    ilSelectedNodes.clear();
    updateSelectionBar();
}

// Toggle between visual and text view
function toggleILView(view) {
    ilCurrentView = view;
    const visualEditor = document.getElementById('il-visual-editor');
    const textEditor = document.getElementById('il-text-editor');
    const btnVisual = document.getElementById('btn-visual-view');
    const btnText = document.getElementById('btn-text-view');

    if (view === 'visual') {
        visualEditor.style.display = 'flex';
        textEditor.style.display = 'none';
        btnVisual.classList.remove('btn-outline-secondary');
        btnVisual.classList.add('btn-primary');
        btnText.classList.remove('btn-primary');
        btnText.classList.add('btn-outline-secondary');

        // Sync from text to visual
        syncTextToVisual();
    } else {
        visualEditor.style.display = 'none';
        textEditor.style.display = 'flex';
        btnText.classList.remove('btn-outline-secondary');
        btnText.classList.add('btn-primary');
        btnVisual.classList.remove('btn-primary');
        btnVisual.classList.add('btn-outline-secondary');

        // Sync from visual to text
        syncVisualToText();

        // Refresh CodeMirror
        if (editors['product_il_definitions']) {
            setTimeout(() => editors['product_il_definitions'].refresh(), 100);
        }
    }
}

// ============================================================
// 3-Step Join Creation Wizard
// ============================================================

function showCreateJoinDialog() {
    if (ilSelectedNodes.size < 1) {
        showStatus('Select at least one table to create a join', 'warning');
        return;
    }

    // Store selected tables
    wizardSelectedTables = Array.from(ilSelectedNodes);
    wizardSelectedMainTable = null;
    wizardStep = 1;

    // Reset wizard UI
    document.getElementById('wizard-title').textContent = 'Create New Join';
    document.getElementById('join-name-input').value = '';
    document.getElementById('join-filter-input').value = '';

    // Populate step 1: Main table options
    const mainTableOptions = document.getElementById('main-table-options');
    mainTableOptions.innerHTML = wizardSelectedTables.map(tableId => `
        <div class="main-table-option" onclick="selectMainTable('${tableId}')" data-table="${tableId}">
            ${tableId}
        </div>
    `).join('');

    // Load existing filters for step 2 suggestions
    loadFilterSuggestions();

    // Show step 1, hide others
    showWizardStep(1);

    // Show dialog
    document.getElementById('create-join-dialog').style.display = 'flex';
}

function selectMainTable(tableId) {
    wizardSelectedMainTable = tableId;

    // Update UI to show selection
    document.querySelectorAll('.main-table-option').forEach(opt => {
        opt.classList.toggle('selected', opt.dataset.table === tableId);
    });
}

function loadFilterSuggestions() {
    fetch(`/pybirdai/joins-config/filters/?framework=${currentFramework}`)
        .then(response => response.json())
        .then(data => {
            if (data.success && data.filters.length > 0) {
                const container = document.getElementById('existing-filters');
                container.innerHTML = `
                    <p style="font-size: 12px; color: #666; margin-bottom: 8px;">Existing filters (click to use):</p>
                    ${data.filters.map(f => `<span class="filter-suggestion" onclick="document.getElementById('join-filter-input').value='${f}'">${f}</span>`).join('')}
                `;
            }
        })
        .catch(error => console.error('Error loading filters:', error));
}

function showWizardStep(step) {
    wizardStep = step;

    // Hide all steps
    document.querySelectorAll('.wizard-step').forEach(s => s.style.display = 'none');

    // Show current step
    document.getElementById(`wizard-step-${step}`).style.display = 'block';

    // Update step indicator
    document.querySelectorAll('.step-dot').forEach(dot => {
        const dotStep = parseInt(dot.dataset.step);
        dot.style.background = dotStep <= step ? '#28a745' : '#dee2e6';
    });

    // Update buttons
    document.getElementById('wizard-back-btn').style.display = step > 1 ? 'inline-block' : 'none';
    document.getElementById('wizard-next-btn').style.display = step < 3 ? 'inline-block' : 'none';
    document.getElementById('wizard-finish-btn').style.display = step === 3 ? 'inline-block' : 'none';

    // If step 3, populate summary
    if (step === 3) {
        document.getElementById('selected-main-table').textContent = wizardSelectedMainTable;

        const relatedTables = wizardSelectedTables.filter(t => t !== wizardSelectedMainTable);
        document.getElementById('join-related-tables').innerHTML = relatedTables.length > 0
            ? relatedTables.map(t => `<span class="table-chip" style="display: inline-block; background: #e3f2fd; color: #1976d2; padding: 4px 10px; border-radius: 15px; font-size: 12px; font-family: monospace;">${t}</span>`).join('')
            : '<span style="color: #999; font-size: 12px;">No related tables (single table join)</span>';
    }
}

function wizardNext() {
    if (wizardStep === 1) {
        if (!wizardSelectedMainTable) {
            showStatus('Please select a main table', 'warning');
            return;
        }
        showWizardStep(2);
    } else if (wizardStep === 2) {
        showWizardStep(3);
    }
}

function wizardBack() {
    if (wizardStep > 1) {
        showWizardStep(wizardStep - 1);
    }
}

function closeCreateJoinDialog() {
    document.getElementById('create-join-dialog').style.display = 'none';
    wizardStep = 1;
    wizardSelectedMainTable = null;
    wizardSelectedTables = [];
}

function confirmCreateJoin() {
    const name = document.getElementById('join-name-input').value.trim();
    const filter = document.getElementById('join-filter-input').value.trim();

    if (!name) {
        showStatus('Please enter a join name', 'warning');
        document.getElementById('join-name-input').focus();
        return;
    }

    if (!wizardSelectedMainTable) {
        showStatus('No main table selected', 'error');
        return;
    }

    const relatedTables = wizardSelectedTables.filter(t => t !== wizardSelectedMainTable);

    // Create join object
    const joinDef = {
        name: name,
        mainTable: wizardSelectedMainTable,
        filter: filter,
        relatedTables: relatedTables,
        comments: ''
    };

    // Add to joins data
    ilJoinsData.push(joinDef);

    // Update UI
    renderJoinsList();
    highlightJoinOnCanvas(ilJoinsData.length - 1);
    closeCreateJoinDialog();
    deselectAllNodes();

    // Sync to text editor
    syncVisualToText();

    showStatus(`Join "${name}" created with ${relatedTables.length + 1} tables`, 'success');
}

// ============================================================
// Join List with Inline Comment Editing
// ============================================================

function renderJoinsList() {
    const container = document.getElementById('il-joins-list');
    const countBadge = document.getElementById('il-joins-count');

    countBadge.textContent = ilJoinsData.length;

    if (ilJoinsData.length === 0) {
        container.innerHTML = '<p style="color: #999; font-size: 13px; text-align: center; margin-top: 20px;">No joins defined yet</p>';
        return;
    }

    container.innerHTML = ilJoinsData.map((join, index) => {
        const isActive = index === ilSelectedJoinIndex;
        const tableCount = 1 + (join.filterTable ? 1 : 0) + (join.relatedTables || []).length;

        return `
        <div class="il-join-card ${isActive ? 'active' : ''}" data-join-index="${index}">
            <div class="join-name" onclick="highlightJoinOnCanvas(${index})">
                ${isActive ? '<i class="fas fa-check-circle" style="color: #ffc107; margin-right: 5px;"></i>' : ''}
                ${join.name}
            </div>
            <div class="join-info" onclick="highlightJoinOnCanvas(${index})">
                Filter: ${join.filter || 'None'} | ${tableCount} tables
            </div>
            <div class="join-tables" onclick="highlightJoinOnCanvas(${index})">
                <span class="table-chip main" title="Main Table"><i class="fas fa-star" style="font-size: 8px; margin-right: 3px;"></i>${join.mainTable}</span>
                ${join.filterTable ? `<span class="table-chip filter" title="Filter Table"><i class="fas fa-filter" style="font-size: 8px; margin-right: 3px;"></i>${join.filterTable}</span>` : ''}
                ${(join.relatedTables || []).map(t => `<span class="table-chip">${t}</span>`).join('')}
            </div>
            ${join.comments ? `<div class="existing-comment" onclick="toggleCommentEditor(${index})">${join.comments}</div>` : ''}
            <div class="join-actions">
                <button onclick="event.stopPropagation(); toggleCommentEditor(${index})" class="btn btn-sm btn-outline-secondary" title="Add/Edit Comment">
                    <i class="fas fa-comment"></i>
                </button>
                <button onclick="event.stopPropagation(); deleteJoin(${index})" class="btn btn-sm btn-outline-danger" title="Delete">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
            <div id="comment-editor-${index}" class="comment-editor" style="display: none;">
                <textarea placeholder="Add a comment..." onclick="event.stopPropagation()">${join.comments || ''}</textarea>
                <div class="comment-actions">
                    <button onclick="event.stopPropagation(); cancelCommentEdit(${index})" class="btn btn-sm btn-outline-secondary">Cancel</button>
                    <button onclick="event.stopPropagation(); saveComment(${index})" class="btn btn-sm btn-primary">Save</button>
                </div>
            </div>
        </div>
    `;
    }).join('');
}

function toggleCommentEditor(joinIndex) {
    const editor = document.getElementById(`comment-editor-${joinIndex}`);
    const isVisible = editor.style.display !== 'none';

    // Hide all other editors
    document.querySelectorAll('.comment-editor').forEach(e => e.style.display = 'none');

    // Toggle this one
    editor.style.display = isVisible ? 'none' : 'block';

    if (!isVisible) {
        editor.querySelector('textarea').focus();
    }
}

function cancelCommentEdit(joinIndex) {
    const editor = document.getElementById(`comment-editor-${joinIndex}`);
    editor.style.display = 'none';

    // Reset textarea to original value
    const join = ilJoinsData[joinIndex];
    editor.querySelector('textarea').value = join.comments || '';
}

function saveComment(joinIndex) {
    const editor = document.getElementById(`comment-editor-${joinIndex}`);
    const textarea = editor.querySelector('textarea');
    const newComment = textarea.value.trim();

    // Update join data
    ilJoinsData[joinIndex].comments = newComment;

    // Re-render to show updated comment
    renderJoinsList();

    // Sync to text editor
    syncVisualToText();

    showStatus('Comment saved', 'success');
}

function highlightJoinOnCanvas(joinIndex) {
    if (!ilCanvasInitialized) {
        initILCanvas();
    }

    const join = ilJoinsData[joinIndex];
    if (!join) return;

    // Collect all tables in this join and add missing ones
    const allTables = [
        join.mainTable,
        ...(join.filterTable ? [join.filterTable] : []),
        ...(join.relatedTables || [])
    ].filter(t => t);

    const missingTables = allTables.filter(t => !ilNodes[t]);
    if (missingTables.length > 0) {
        missingTables.forEach((tableId) => {
            ilNodes[tableId] = { id: tableId, name: tableId };
        });
        renderTablesList();
    }

    // Use the new single selection mode
    toggleJoinSelection(joinIndex);

    showStatus(`Viewing join: ${join.name}`, 'info');
}

function closeJoinPreview() {
    ilSelectedJoinIndex = -1;
    renderCanvas();
    renderJoinsList();
}

function deleteJoin(joinIndex) {
    const join = ilJoinsData[joinIndex];
    if (!join) return;

    if (confirm(`Delete join "${join.name}"?`)) {
        ilJoinsData.splice(joinIndex, 1);
        renderJoinsList();

        // Clear highlighting
        document.querySelectorAll('.il-table-chip').forEach(chip => {
            chip.classList.remove('in-preview', 'main-table');
        });
        closeJoinPreview();

        syncVisualToText();
        showStatus(`Join "${join.name}" deleted`, 'success');
    }
}

// ============================================================
// Text/Visual Sync Functions
// ============================================================

// Sync visual to text (generate CSV from joins)
function syncVisualToText() {
    if (!editors['product_il_definitions']) return;

    const csvLines = ['Name,Main Table,Filter,Related Tables,Comments'];

    ilJoinsData.forEach(join => {
        const relatedStr = join.relatedTables.join(':');
        // Escape commas in fields
        const name = join.name.includes(',') ? `"${join.name}"` : join.name;
        const comments = (join.comments || '').includes(',') ? `"${join.comments}"` : (join.comments || '');
        csvLines.push(`${name},${join.mainTable},${join.filter || ''},${relatedStr},${comments}`);
    });

    editors['product_il_definitions'].setValue(csvLines.join('\n') + '\n');
}

// Sync text to visual (parse CSV and populate canvas)
function syncTextToVisual() {
    if (!editors['product_il_definitions']) return;
    if (!ilCanvasInitialized) {
        initILCanvas();
    }

    const content = editors['product_il_definitions'].getValue();
    const lines = content.trim().split('\n');

    // Clear data
    ilNodes = {};
    ilSelectedNodes.clear();
    ilJoinsData = [];
    ilSelectedJoinIndex = -1;

    if (lines.length <= 1) {
        renderJoinsList();
        updateInstructions();
        return;
    }

    // Parse CSV: Name,Main Table,Filter,Related Tables,Comments
    const allTables = new Set();

    for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;

        // Simple CSV parsing
        const parts = line.split(',');
        if (parts.length < 2) continue;

        const name = parts[0] ? parts[0].trim() : '';
        const mainTable = parts[1] ? parts[1].trim() : '';
        const filter = parts[2] ? parts[2].trim() : '';
        const relatedTablesStr = parts[3] || '';
        const comments = parts.slice(4).join(',');

        // Skip rows with empty name or main table
        if (!name || !mainTable) continue;

        const relatedTables = relatedTablesStr ? relatedTablesStr.split(':').filter(t => t && t.trim()) : [];

        // Collect all tables
        allTables.add(mainTable);
        relatedTables.forEach(t => {
            if (t && t.trim()) allTables.add(t.trim());
        });

        // Store join data
        ilJoinsData.push({
            name,
            mainTable,
            filterTable: null,
            filter,
            relatedTables: relatedTables.map(t => t.trim()),
            comments
        });
    }

    // Add all tables to ilNodes
    const tableArray = Array.from(allTables).filter(t => t && t.trim());
    tableArray.forEach(tableId => {
        ilNodes[tableId] = { id: tableId, name: tableId };
    });

    // Render joins list and canvas
    renderJoinsList();
    renderTablesList();
    updateInstructions();
}

// Update joins when a table is removed from canvas
function updateJoinsFromCanvas() {
    const remainingNodeIds = new Set(Object.keys(ilNodes));

    // Filter out joins that reference removed tables
    ilJoinsData = ilJoinsData.filter(join => {
        // Check if main table still exists
        if (!remainingNodeIds.has(join.mainTable)) {
            return false;
        }
        // Clear filter table if it no longer exists
        if (join.filterTable && !remainingNodeIds.has(join.filterTable)) {
            join.filterTable = null;
        }
        // Remove related tables that no longer exist
        join.relatedTables = (join.relatedTables || []).filter(t => remainingNodeIds.has(t));
        return true;
    });

    // Reset selection if selected join was removed
    if (ilSelectedJoinIndex >= ilJoinsData.length) {
        ilSelectedJoinIndex = -1;
    }

    renderJoinsList();
    syncVisualToText();
}

// Load existing joins when switching to IL definitions tab
function loadJoinsToCanvas() {
    if (editors['product_il_definitions']) {
        syncTextToVisual();
    }
}
