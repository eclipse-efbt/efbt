// Enhanced Lineage Viewer JavaScript
// Provides multiple visualization modes: Graph, Sankey, Hierarchy, Timeline
(function() {
    'use strict';

    // State management
    let cy = null;
    let lineageData = null;
    let currentLayout = 'dagre';
    let currentViewMode = 'graph';
    let sankeyChart = null;
    let timelineChart = null;

    // Color palette for consistent styling
    const colorPalette = {
        // Table types
        databaseTable: '#3498db',      // Blue
        derivedTable: '#2ecc71',       // Green
        outputTable: '#e74c3c',        // Red

        // Node types
        databaseField: '#f39c12',      // Orange
        function: '#9b59b6',           // Purple
        databaseRow: '#7f8c8d',        // Gray
        derivedRow: '#d35400',         // Dark Orange
        columnValue: '#8e44ad',        // Dark Purple
        evaluatedFunction: '#1abc9c',  // Teal

        // Flow types
        dataFlow: '#3498db',
        filterFlow: '#e74c3c',
        joinFlow: '#2ecc71',
        aggregateFlow: '#f39c12',
        transformFlow: '#9b59b6',

        // Status colors
        used: '#27ae60',
        unused: '#bdc3c7',
        highlighted: '#f1c40f',
    };

    // Node styling based on type
    const nodeStyles = {
        'database_table': {
            'background-color': colorPalette.databaseTable,
            'shape': 'round-rectangle',
            'width': 'label',
            'height': 50,
            'padding': '15px'
        },
        'derived_table': {
            'background-color': colorPalette.derivedTable,
            'shape': 'round-rectangle',
            'width': 'label',
            'height': 50,
            'padding': '15px'
        },
        'output_table': {
            'background-color': colorPalette.outputTable,
            'shape': 'round-rectangle',
            'width': 'label',
            'height': 50,
            'padding': '15px',
            'border-width': 4,
            'border-color': '#c0392b'
        },
        'database_field': {
            'background-color': colorPalette.databaseField,
            'shape': 'ellipse',
            'width': 100,
            'height': 40
        },
        'function': {
            'background-color': colorPalette.function,
            'shape': 'diamond',
            'width': 100,
            'height': 50
        },
        'database_row': {
            'background-color': colorPalette.databaseRow,
            'shape': 'round-rectangle',
            'width': 80,
            'height': 30
        },
        'derived_row': {
            'background-color': colorPalette.derivedRow,
            'shape': 'round-rectangle',
            'width': 80,
            'height': 30
        },
        'transformation_step': {
            'background-color': '#2980b9',
            'shape': 'hexagon',
            'width': 120,
            'height': 60
        },
        'calculation_chain': {
            'background-color': '#c0392b',
            'shape': 'octagon',
            'width': 140,
            'height': 70
        }
    };

    // Edge styling based on type
    const edgeStyles = {
        'data_flow': {
            'line-color': colorPalette.dataFlow,
            'target-arrow-color': colorPalette.dataFlow,
            'width': 3,
            'curve-style': 'bezier'
        },
        'filter_flow': {
            'line-color': colorPalette.filterFlow,
            'target-arrow-color': colorPalette.filterFlow,
            'width': 2,
            'line-style': 'dashed'
        },
        'join_flow': {
            'line-color': colorPalette.joinFlow,
            'target-arrow-color': colorPalette.joinFlow,
            'width': 3
        },
        'aggregate_flow': {
            'line-color': colorPalette.aggregateFlow,
            'target-arrow-color': colorPalette.aggregateFlow,
            'width': 4
        },
        'transform_flow': {
            'line-color': colorPalette.transformFlow,
            'target-arrow-color': colorPalette.transformFlow,
            'width': 3
        },
        'has_field': {
            'line-color': '#95a5a6',
            'target-arrow-color': '#95a5a6',
            'width': 1,
            'line-style': 'dotted'
        },
        'has_function': {
            'line-color': '#95a5a6',
            'target-arrow-color': '#95a5a6',
            'width': 1,
            'line-style': 'dotted'
        },
        'derived_from': {
            'line-color': colorPalette.derivedTable,
            'target-arrow-color': colorPalette.derivedTable,
            'width': 4
        },
        'step_flow': {
            'line-color': '#2980b9',
            'target-arrow-color': '#2980b9',
            'width': 5,
            'curve-style': 'taxi'
        }
    };

    // ========================================================================
    // CYTOSCAPE GRAPH VIEW
    // ========================================================================

    function initializeCytoscape() {
        const container = document.getElementById('cy');
        if (!container) {
            console.error('Cytoscape container not found');
            return;
        }

        // Ensure container has dimensions
        if (container.offsetWidth === 0 || container.offsetHeight === 0) {
            container.style.width = '100%';
            container.style.height = '600px';
        }

        cy = cytoscape({
            container: container,

            style: [
                {
                    selector: 'node',
                    style: {
                        'label': 'data(label)',
                        'text-valign': 'center',
                        'text-halign': 'center',
                        'color': '#fff',
                        'text-outline-width': 2,
                        'text-outline-color': '#2c3e50',
                        'font-size': '11px',
                        'font-weight': 'bold',
                        'border-width': 2,
                        'border-color': '#2c3e50',
                        'text-wrap': 'ellipsis',
                        'text-max-width': '100px'
                    }
                },
                {
                    selector: 'node.highlighted',
                    style: {
                        'border-width': 4,
                        'border-color': colorPalette.highlighted,
                        'box-shadow': '0 0 20px ' + colorPalette.highlighted
                    }
                },
                {
                    selector: 'node.used',
                    style: {
                        'opacity': 1
                    }
                },
                {
                    selector: 'node.unused',
                    style: {
                        'opacity': 0.4
                    }
                },
                {
                    selector: 'edge',
                    style: {
                        'curve-style': 'bezier',
                        'target-arrow-shape': 'triangle',
                        'arrow-scale': 1.2,
                        'label': 'data(label)',
                        'font-size': '9px',
                        'text-rotation': 'autorotate',
                        'text-margin-y': -8,
                        'text-background-color': '#fff',
                        'text-background-opacity': 0.8,
                        'text-background-padding': '2px'
                    }
                },
                {
                    selector: 'edge.highlighted',
                    style: {
                        'line-color': colorPalette.highlighted,
                        'target-arrow-color': colorPalette.highlighted,
                        'width': 5
                    }
                },
                {
                    selector: ':selected',
                    style: {
                        'border-width': 4,
                        'border-color': '#e74c3c'
                    }
                }
            ],

            layout: { name: 'preset' },
            minZoom: 0.1,
            maxZoom: 5,
            wheelSensitivity: 0.3
        });

        // Apply type-specific styles
        Object.keys(nodeStyles).forEach(type => {
            cy.style().selector(`node[type="${type}"]`).style(nodeStyles[type]).update();
        });

        Object.keys(edgeStyles).forEach(type => {
            cy.style().selector(`edge[type="${type}"]`).style(edgeStyles[type]).update();
        });

        // Event handlers
        cy.on('tap', 'node', handleNodeClick);
        cy.on('tap', handleBackgroundClick);
        cy.on('mouseover', 'node', handleNodeHover);
        cy.on('mouseout', 'node', handleNodeHoverOut);

        console.log('Cytoscape initialized');
    }

    function handleNodeClick(event) {
        const node = event.target;

        // Remove previous highlights
        cy.elements().removeClass('highlighted');

        // Highlight clicked node and its connections
        node.addClass('highlighted');
        node.connectedEdges().addClass('highlighted');
        node.neighborhood('node').addClass('highlighted');

        showNodeDetails(node);
    }

    function handleBackgroundClick(event) {
        if (event.target === cy) {
            cy.elements().removeClass('highlighted');
            clearNodeDetails();
        }
    }

    function handleNodeHover(event) {
        const node = event.target;
        node.addClass('hover');
        document.body.style.cursor = 'pointer';
    }

    function handleNodeHoverOut(event) {
        const node = event.target;
        node.removeClass('hover');
        document.body.style.cursor = 'default';
    }

    // ========================================================================
    // DATA LOADING AND GRAPH UPDATING
    // ========================================================================

    function loadLineageData() {
        const trailDataEl = document.getElementById('trail-data');
        if (!trailDataEl) {
            console.error('Trail data element not found');
            return;
        }

        const trailId = trailDataEl.dataset.trailId;
        const lineageType = trailDataEl.dataset.lineageType || 'enhanced';
        const detailLevel = document.getElementById('detail-level')?.value || 'table';
        const maxRows = document.getElementById('max-rows')?.value || 10;
        const hideEmpty = document.getElementById('hide-empty-tables')?.checked ?? true;

        showLoading(true);

        // Determine API endpoint based on lineage type
        let apiEndpoint;
        if (lineageType === 'enhanced') {
            apiEndpoint = `/pybirdai/api/trail/${trailId}/enhanced-lineage/`;
        } else if (lineageType === 'filtered') {
            apiEndpoint = `/pybirdai/api/trail/${trailId}/filtered-lineage/`;
        } else {
            apiEndpoint = `/pybirdai/api/trail/${trailId}/lineage`;
        }

        const params = new URLSearchParams({
            detail: detailLevel,
            max_rows: maxRows,
            hide_empty: hideEmpty
        });

        fetch(`${apiEndpoint}?${params}`)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                return response.json();
            })
            .then(data => {
                if (data.error) {
                    throw new Error(data.error);
                }

                lineageData = data;
                updateVisualization();
                updateSummaryPanel();
                showLoading(false);
            })
            .catch(error => {
                console.error('Error loading lineage data:', error);
                showError('Failed to load lineage data: ' + error.message);
                showLoading(false);
            });
    }

    function updateVisualization() {
        switch (currentViewMode) {
            case 'graph':
                updateGraphView();
                break;
            case 'sankey':
                updateSankeyView();
                break;
            case 'hierarchy':
                updateHierarchyView();
                break;
            case 'timeline':
                updateTimelineView();
                break;
            case 'cell':
                updateCellView();
                break;
            default:
                updateGraphView();
        }
    }

    function updateGraphView() {
        if (!cy || !lineageData) return;

        // Clear existing elements
        cy.elements().remove();

        const elements = buildGraphElements();

        // Add elements in batches for performance
        cy.add(elements.nodes);
        cy.add(elements.edges);

        console.log(`Graph updated: ${elements.nodes.length} nodes, ${elements.edges.length} edges`);

        applyLayout();
    }

    function buildGraphElements() {
        const nodes = [];
        const edges = [];
        const detailLevel = document.getElementById('detail-level')?.value || 'table';

        // Add database tables
        if (lineageData.database_tables) {
            lineageData.database_tables.forEach(table => {
                nodes.push({
                    group: 'nodes',
                    data: {
                        id: `db_table_${table.id}`,
                        label: truncateLabel(table.name, 25),
                        fullLabel: table.name,
                        type: 'database_table',
                        details: {
                            type: 'Database Table',
                            name: table.name,
                            fieldCount: table.fields?.length || 0
                        }
                    }
                });

                // Add fields if detail level allows
                if (detailLevel !== 'table' && table.fields) {
                    table.fields.forEach(field => {
                        nodes.push({
                            group: 'nodes',
                            data: {
                                id: `db_field_${field.id}`,
                                label: truncateLabel(field.name, 15),
                                fullLabel: field.name,
                                type: 'database_field',
                                wasUsed: field.was_used,
                                details: {
                                    type: 'Database Field',
                                    name: field.name,
                                    table: table.name,
                                    wasUsed: field.was_used
                                }
                            },
                            classes: field.was_used ? 'used' : 'unused'
                        });

                        edges.push({
                            group: 'edges',
                            data: {
                                id: `edge_table_field_${table.id}_${field.id}`,
                                source: `db_table_${table.id}`,
                                target: `db_field_${field.id}`,
                                type: 'has_field'
                            }
                        });
                    });
                }
            });
        }

        // Add derived tables
        if (lineageData.derived_tables) {
            lineageData.derived_tables.forEach(table => {
                // Determine if this is an output table (final report table)
                const isOutput = table.name.match(/^F_\d{2}_\d{2}_REF_/) && !table.name.includes('_UnionItem');
                const nodeType = isOutput ? 'output_table' : 'derived_table';

                nodes.push({
                    group: 'nodes',
                    data: {
                        id: `derived_table_${table.id}`,
                        label: truncateLabel(table.name, 25),
                        fullLabel: table.name,
                        type: nodeType,
                        details: {
                            type: isOutput ? 'Output Table' : 'Derived Table',
                            name: table.name,
                            functionCount: table.functions?.length || 0
                        }
                    }
                });

                // Add functions if detail level allows
                if (detailLevel !== 'table' && table.functions) {
                    table.functions.forEach(func => {
                        nodes.push({
                            group: 'nodes',
                            data: {
                                id: `function_${func.id}`,
                                label: truncateLabel(func.name.split('.').pop() || func.name, 15),
                                fullLabel: func.name,
                                type: 'function',
                                wasUsed: func.was_used,
                                details: {
                                    type: 'Function',
                                    name: func.name,
                                    table: table.name,
                                    wasUsed: func.was_used,
                                    functionText: func.function_text
                                }
                            },
                            classes: func.was_used ? 'used' : 'unused'
                        });

                        edges.push({
                            group: 'edges',
                            data: {
                                id: `edge_table_func_${table.id}_${func.id}`,
                                source: `derived_table_${table.id}`,
                                target: `function_${func.id}`,
                                type: 'has_function'
                            }
                        });
                    });
                }
            });
        }

        // Add data flow edges from lineage relationships
        if (lineageData.lineage_relationships) {
            // Add table creation source tables edges
            if (lineageData.lineage_relationships.table_creation_source_tables) {
                lineageData.lineage_relationships.table_creation_source_tables.forEach(ref => {
                    const sourceType = ref.source_object_type === 'databasetable' ? 'db_table' : 'derived_table';
                    const sourceId = `${sourceType}_${ref.source_object_id}`;

                    // Find the target derived table that uses this creation function
                    if (lineageData.derived_tables) {
                        lineageData.derived_tables.forEach(table => {
                            if (table.table_creation_function_id === ref.table_creation_function_id) {
                                edges.push({
                                    group: 'edges',
                                    data: {
                                        id: `edge_flow_${ref.id}`,
                                        source: sourceId,
                                        target: `derived_table_${table.id}`,
                                        type: 'data_flow',
                                        label: ''
                                    }
                                });
                            }
                        });
                    }
                });
            }

            // Add function column references
            if (lineageData.lineage_relationships.function_column_references) {
                lineageData.lineage_relationships.function_column_references.forEach(ref => {
                    const refType = ref.referenced_object_type === 'databasefield' ? 'db_field' : 'function';
                    const refId = `${refType}_${ref.referenced_object_id}`;

                    edges.push({
                        group: 'edges',
                        data: {
                            id: `edge_func_ref_${ref.id}`,
                            source: refId,
                            target: `function_${ref.function_id}`,
                            type: 'transform_flow',
                            label: ''
                        }
                    });
                });
            }
        }

        // Add transformation steps if available
        if (lineageData.transformation_steps) {
            lineageData.transformation_steps.forEach(step => {
                nodes.push({
                    group: 'nodes',
                    data: {
                        id: `step_${step.id}`,
                        label: `${step.step_number}. ${truncateLabel(step.step_name, 20)}`,
                        fullLabel: step.step_name,
                        type: 'transformation_step',
                        details: {
                            type: 'Transformation Step',
                            stepNumber: step.step_number,
                            stepType: step.step_type,
                            name: step.step_name,
                            description: step.description,
                            inputRows: step.input_row_count,
                            outputRows: step.output_row_count,
                            executionTime: step.execution_time_ms
                        }
                    }
                });
            });

            // Connect consecutive steps
            for (let i = 0; i < lineageData.transformation_steps.length - 1; i++) {
                const current = lineageData.transformation_steps[i];
                const next = lineageData.transformation_steps[i + 1];
                edges.push({
                    group: 'edges',
                    data: {
                        id: `edge_step_${current.id}_${next.id}`,
                        source: `step_${current.id}`,
                        target: `step_${next.id}`,
                        type: 'step_flow'
                    }
                });
            }
        }

        // Add data flow edges if available
        if (lineageData.data_flow_edges) {
            lineageData.data_flow_edges.forEach(edge => {
                edges.push({
                    group: 'edges',
                    data: {
                        id: `flow_${edge.id}`,
                        source: `${edge.source_type}_${edge.source_id}`,
                        target: `${edge.target_type}_${edge.target_id}`,
                        type: `${edge.flow_type.toLowerCase()}_flow`,
                        label: edge.row_count > 0 ? `${edge.row_count} rows` : ''
                    }
                });
            });
        }

        return { nodes, edges };
    }

    function applyLayout() {
        if (!cy || cy.nodes().length === 0) return;

        const layoutOptions = {
            name: currentLayout,
            animate: true,
            animationDuration: 500,
            fit: true,
            padding: 50
        };

        switch (currentLayout) {
            case 'dagre':
                layoutOptions.rankDir = 'LR';
                layoutOptions.nodeSep = 50;
                layoutOptions.rankSep = 100;
                layoutOptions.edgeSep = 20;
                break;
            case 'breadthfirst':
                layoutOptions.directed = true;
                layoutOptions.spacingFactor = 1.5;
                break;
            case 'cose':
                layoutOptions.idealEdgeLength = 100;
                layoutOptions.nodeRepulsion = 4500;
                layoutOptions.numIter = 100;
                break;
            case 'circle':
                layoutOptions.avoidOverlap = true;
                break;
            case 'concentric':
                layoutOptions.concentric = function(node) {
                    const type = node.data('type');
                    if (type === 'database_table') return 3;
                    if (type === 'derived_table') return 2;
                    if (type === 'output_table') return 1;
                    return 0;
                };
                break;
        }

        cy.layout(layoutOptions).run();
    }

    // ========================================================================
    // SANKEY DIAGRAM VIEW
    // ========================================================================

    function updateSankeyView() {
        const container = document.getElementById('sankey-container');
        if (!container || !lineageData) return;

        container.style.display = 'block';
        document.getElementById('cy').style.display = 'none';

        // Build Sankey data from lineage
        const sankeyData = buildSankeyData();

        if (sankeyData.nodes.length === 0) {
            container.innerHTML = '<div class="no-data">No data flow information available for Sankey diagram</div>';
            return;
        }

        // Use D3 Sankey or similar library
        renderSankeyDiagram(container, sankeyData);
    }

    function buildSankeyData() {
        const nodes = [];
        const links = [];
        const nodeIndex = {};

        // Add tables as nodes
        if (lineageData.database_tables) {
            lineageData.database_tables.forEach(table => {
                const id = `db_${table.id}`;
                nodeIndex[id] = nodes.length;
                nodes.push({
                    id: id,
                    name: table.name,
                    type: 'source'
                });
            });
        }

        if (lineageData.derived_tables) {
            lineageData.derived_tables.forEach(table => {
                const id = `derived_${table.id}`;
                nodeIndex[id] = nodes.length;
                const isOutput = table.name.match(/^F_\d{2}_\d{2}_REF_/) && !table.name.includes('_UnionItem');
                nodes.push({
                    id: id,
                    name: table.name,
                    type: isOutput ? 'output' : 'transform'
                });
            });
        }

        // Add data flow edges as links
        if (lineageData.data_flow_edges) {
            lineageData.data_flow_edges.forEach(edge => {
                const sourceKey = `${edge.source_type}_${edge.source_id}`;
                const targetKey = `${edge.target_type}_${edge.target_id}`;

                if (nodeIndex[sourceKey] !== undefined && nodeIndex[targetKey] !== undefined) {
                    links.push({
                        source: nodeIndex[sourceKey],
                        target: nodeIndex[targetKey],
                        value: edge.row_count || 1
                    });
                }
            });
        }

        // If no explicit flow edges, infer from table creation sources
        if (links.length === 0 && lineageData.lineage_relationships?.table_creation_source_tables) {
            lineageData.lineage_relationships.table_creation_source_tables.forEach(ref => {
                const sourceType = ref.source_object_type === 'databasetable' ? 'db' : 'derived';
                const sourceKey = `${sourceType}_${ref.source_object_id}`;

                // Find target table
                if (lineageData.derived_tables) {
                    lineageData.derived_tables.forEach(table => {
                        if (table.table_creation_function_id === ref.table_creation_function_id) {
                            const targetKey = `derived_${table.id}`;
                            if (nodeIndex[sourceKey] !== undefined && nodeIndex[targetKey] !== undefined) {
                                links.push({
                                    source: nodeIndex[sourceKey],
                                    target: nodeIndex[targetKey],
                                    value: 1
                                });
                            }
                        }
                    });
                }
            });
        }

        return { nodes, links };
    }

    function renderSankeyDiagram(container, data) {
        // Clear container
        container.innerHTML = '';

        if (typeof d3 === 'undefined') {
            container.innerHTML = `
                <div class="sankey-placeholder">
                    <h4>Sankey Diagram</h4>
                    <p>Data flow visualization shows ${data.nodes.length} nodes and ${data.links.length} connections.</p>
                    <div class="sankey-summary">
                        <div class="sankey-node-list">
                            <h5>Tables:</h5>
                            <ul>
                                ${data.nodes.map(n => `<li class="${n.type}">${n.name}</li>`).join('')}
                            </ul>
                        </div>
                    </div>
                    <p class="text-muted">Add D3.js and d3-sankey for interactive visualization</p>
                </div>
            `;
            return;
        }

        // D3 Sankey rendering would go here
        // This is a placeholder for the actual implementation
    }

    // ========================================================================
    // HIERARCHY VIEW
    // ========================================================================

    function updateHierarchyView() {
        const container = document.getElementById('hierarchy-container');
        if (!container || !lineageData) return;

        container.style.display = 'block';
        document.getElementById('cy').style.display = 'none';

        // Build hierarchical tree structure
        const treeData = buildHierarchyData();
        renderHierarchyTree(container, treeData);
    }

    function buildHierarchyData() {
        const root = {
            name: 'Lineage Root',
            children: []
        };

        // Group by table type
        const dbTables = {
            name: 'Source Tables',
            type: 'category',
            children: []
        };

        const derivedTables = {
            name: 'Derived Tables',
            type: 'category',
            children: []
        };

        const outputTables = {
            name: 'Output Tables',
            type: 'category',
            children: []
        };

        if (lineageData.database_tables) {
            lineageData.database_tables.forEach(table => {
                const tableNode = {
                    name: table.name,
                    type: 'database_table',
                    children: table.fields?.map(f => ({
                        name: f.name,
                        type: 'field',
                        wasUsed: f.was_used
                    })) || []
                };
                dbTables.children.push(tableNode);
            });
        }

        if (lineageData.derived_tables) {
            lineageData.derived_tables.forEach(table => {
                const isOutput = table.name.match(/^F_\d{2}_\d{2}_REF_/) && !table.name.includes('_UnionItem');
                const tableNode = {
                    name: table.name,
                    type: isOutput ? 'output_table' : 'derived_table',
                    children: table.functions?.map(f => ({
                        name: f.name,
                        type: 'function',
                        wasUsed: f.was_used
                    })) || []
                };

                if (isOutput) {
                    outputTables.children.push(tableNode);
                } else {
                    derivedTables.children.push(tableNode);
                }
            });
        }

        if (dbTables.children.length > 0) root.children.push(dbTables);
        if (derivedTables.children.length > 0) root.children.push(derivedTables);
        if (outputTables.children.length > 0) root.children.push(outputTables);

        return root;
    }

    function renderHierarchyTree(container, data) {
        container.innerHTML = '';

        const renderNode = (node, depth = 0) => {
            const div = document.createElement('div');
            div.className = `tree-node depth-${depth} type-${node.type || 'default'}`;
            div.style.paddingLeft = `${depth * 20}px`;

            const icon = getNodeIcon(node.type);
            const usedClass = node.wasUsed === false ? 'unused' : '';

            div.innerHTML = `
                <span class="tree-toggle">${node.children?.length > 0 ? '▼' : '•'}</span>
                <span class="tree-icon">${icon}</span>
                <span class="tree-label ${usedClass}">${node.name}</span>
            `;

            if (node.children && node.children.length > 0) {
                const childContainer = document.createElement('div');
                childContainer.className = 'tree-children';
                node.children.forEach(child => {
                    childContainer.appendChild(renderNode(child, depth + 1));
                });
                div.appendChild(childContainer);

                // Toggle functionality
                div.querySelector('.tree-toggle').addEventListener('click', (e) => {
                    e.stopPropagation();
                    childContainer.classList.toggle('collapsed');
                    div.querySelector('.tree-toggle').textContent =
                        childContainer.classList.contains('collapsed') ? '▶' : '▼';
                });
            }

            return div;
        };

        container.appendChild(renderNode(data));
    }

    function getNodeIcon(type) {
        const icons = {
            'category': '',
            'database_table': '',
            'derived_table': '',
            'output_table': '',
            'field': '',
            'function': 'f',
            'default': ''
        };
        return icons[type] || icons['default'];
    }

    // ========================================================================
    // TIMELINE VIEW
    // ========================================================================

    function updateTimelineView() {
        const container = document.getElementById('timeline-container');
        if (!container || !lineageData) return;

        container.style.display = 'block';
        document.getElementById('cy').style.display = 'none';

        if (!lineageData.transformation_steps || lineageData.transformation_steps.length === 0) {
            container.innerHTML = '<div class="no-data">No transformation steps recorded for timeline view</div>';
            return;
        }

        renderTimeline(container, lineageData.transformation_steps);
    }

    function renderTimeline(container, steps) {
        container.innerHTML = `
            <div class="timeline-wrapper">
                <h4>Transformation Timeline</h4>
                <div class="timeline">
                    ${steps.map((step, index) => `
                        <div class="timeline-item ${step.step_type.toLowerCase()}">
                            <div class="timeline-marker">${step.step_number}</div>
                            <div class="timeline-content">
                                <h5>${step.step_name}</h5>
                                <p class="step-type">${step.step_type}</p>
                                <div class="step-stats">
                                    <span>In: ${step.input_row_count || '-'}</span>
                                    <span>Out: ${step.output_row_count || '-'}</span>
                                    ${step.execution_time_ms ? `<span>${step.execution_time_ms}ms</span>` : ''}
                                </div>
                                ${step.description ? `<p class="step-description">${step.description}</p>` : ''}
                            </div>
                            ${index < steps.length - 1 ? '<div class="timeline-connector"></div>' : ''}
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }

    // ========================================================================
    // CELL-CENTRIC VIEW
    // ========================================================================

    function updateCellView() {
        const container = document.getElementById('cell-container');
        if (!container || !lineageData) return;

        container.style.display = 'block';
        document.getElementById('cy').style.display = 'none';

        if (!lineageData.cell_lineages || lineageData.cell_lineages.length === 0) {
            // Try to infer cells from calculation chains or evaluated derived tables
            const inferredCells = inferCellsFromData();
            if (inferredCells.length === 0) {
                container.innerHTML = '<div class="no-data">No cell lineage data available</div>';
                return;
            }
            renderCellGrid(container, inferredCells);
        } else {
            renderCellGrid(container, lineageData.cell_lineages);
        }
    }

    function inferCellsFromData() {
        const cells = [];

        // Infer from calculation chains
        if (lineageData.calculation_chains) {
            lineageData.calculation_chains.forEach(chain => {
                cells.push({
                    cell_code: chain.chain_name,
                    computed_value: chain.final_value,
                    report_template: chain.output_table || 'Unknown',
                    source_row_count: chain.total_contributing_rows
                });
            });
        }

        // Infer from evaluated derived tables (output tables only)
        if (lineageData.evaluated_derived_tables) {
            lineageData.evaluated_derived_tables.forEach(table => {
                const isOutput = table.table_name.match(/^F_\d{2}_\d{2}_REF_/) && !table.table_name.includes('_UnionItem');
                if (isOutput && table.rows) {
                    table.rows.forEach(row => {
                        if (row.evaluated_functions) {
                            row.evaluated_functions.forEach(func => {
                                if (func.value !== null) {
                                    cells.push({
                                        cell_code: `${row.row_identifier || 'row'}.${func.function_name}`,
                                        computed_value: func.value,
                                        report_template: table.table_name,
                                        row_key: row.row_identifier
                                    });
                                }
                            });
                        }
                    });
                }
            });
        }

        return cells;
    }

    function renderCellGrid(container, cells) {
        container.innerHTML = `
            <div class="cell-grid-wrapper">
                <h4>Output Cells</h4>
                <div class="cell-grid">
                    ${cells.map(cell => `
                        <div class="cell-card" data-cell-code="${cell.cell_code}">
                            <div class="cell-header">
                                <span class="cell-code">${truncateLabel(cell.cell_code, 30)}</span>
                            </div>
                            <div class="cell-value">${formatValue(cell.computed_value)}</div>
                            <div class="cell-meta">
                                <span>${cell.report_template || ''}</span>
                                ${cell.source_row_count ? `<span>${cell.source_row_count} source rows</span>` : ''}
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;

        // Add click handlers for cell cards
        container.querySelectorAll('.cell-card').forEach(card => {
            card.addEventListener('click', () => {
                const cellCode = card.dataset.cellCode;
                showCellLineageDetails(cellCode);
            });
        });
    }

    function showCellLineageDetails(cellCode) {
        // Find the cell and show its detailed lineage
        const detailsPanel = document.getElementById('node-details');
        if (!detailsPanel) return;

        detailsPanel.innerHTML = `
            <div class="cell-details">
                <h5>Cell: ${cellCode}</h5>
                <p>Click on graph nodes to see detailed lineage for this cell.</p>
                <button class="btn btn-sm btn-primary" onclick="window.showCellInGraph('${cellCode}')">
                    Show in Graph View
                </button>
            </div>
        `;
    }

    // ========================================================================
    // SUMMARY PANEL
    // ========================================================================

    function updateSummaryPanel() {
        if (!lineageData) return;

        // Update counts
        const dbTableCount = lineageData.database_tables?.length || 0;
        const derivedTableCount = lineageData.derived_tables?.length || 0;
        const totalCounts = lineageData.metadata?.total_counts || {};

        setElementText('db-table-count', totalCounts.database_tables || dbTableCount);
        setElementText('derived-table-count', totalCounts.derived_tables || derivedTableCount);
        setElementText('total-nodes', cy ? cy.nodes().length : 0);
        setElementText('total-edges', cy ? cy.edges().length : 0);

        // Update additional stats if available
        if (lineageData.transformation_steps) {
            setElementText('transformation-steps', lineageData.transformation_steps.length);
        }
        if (lineageData.calculation_chains) {
            setElementText('calculation-chains', lineageData.calculation_chains.length);
        }
    }

    // ========================================================================
    // NODE DETAILS PANEL
    // ========================================================================

    function showNodeDetails(node) {
        const detailsPanel = document.getElementById('node-details');
        if (!detailsPanel) return;

        const nodeData = node.data();
        const details = nodeData.details || {};

        let html = `
            <div class="node-details-content">
                <div class="detail-header">
                    <h5>${nodeData.fullLabel || nodeData.label}</h5>
                    <span class="detail-type">${details.type || formatNodeType(nodeData.type)}</span>
                </div>
        `;

        // Type-specific details
        switch (nodeData.type) {
            case 'database_table':
            case 'derived_table':
            case 'output_table':
                html += `
                    <div class="detail-section">
                        <p><strong>Name:</strong> ${details.name}</p>
                        <p><strong>Fields:</strong> ${details.fieldCount || details.functionCount || 0}</p>
                    </div>
                `;
                break;

            case 'database_field':
                html += `
                    <div class="detail-section">
                        <p><strong>Table:</strong> ${details.table}</p>
                        <p><strong>Used:</strong> ${details.wasUsed ? 'Yes' : 'No'}</p>
                    </div>
                `;
                break;

            case 'function':
                html += `
                    <div class="detail-section">
                        <p><strong>Table:</strong> ${details.table}</p>
                        <p><strong>Used:</strong> ${details.wasUsed ? 'Yes' : 'No'}</p>
                        ${details.functionText ? `
                            <div class="function-code">
                                <strong>Code:</strong>
                                <pre>${escapeHtml(details.functionText)}</pre>
                            </div>
                        ` : ''}
                    </div>
                `;
                break;

            case 'transformation_step':
                html += `
                    <div class="detail-section">
                        <p><strong>Step #:</strong> ${details.stepNumber}</p>
                        <p><strong>Type:</strong> ${details.stepType}</p>
                        <p><strong>Input Rows:</strong> ${details.inputRows || 0}</p>
                        <p><strong>Output Rows:</strong> ${details.outputRows || 0}</p>
                        ${details.executionTime ? `<p><strong>Time:</strong> ${details.executionTime}ms</p>` : ''}
                        ${details.description ? `<p><strong>Description:</strong> ${details.description}</p>` : ''}
                    </div>
                `;
                break;
        }

        // Connections
        const inEdges = node.incomers('edge');
        const outEdges = node.outgoers('edge');

        if (inEdges.length > 0) {
            html += `
                <div class="detail-section">
                    <h6>Sources (${inEdges.length})</h6>
                    <ul class="connection-list">
                        ${inEdges.map(e => `<li>${e.source().data('label')}</li>`).join('')}
                    </ul>
                </div>
            `;
        }

        if (outEdges.length > 0) {
            html += `
                <div class="detail-section">
                    <h6>Targets (${outEdges.length})</h6>
                    <ul class="connection-list">
                        ${outEdges.map(e => `<li>${e.target().data('label')}</li>`).join('')}
                    </ul>
                </div>
            `;
        }

        html += '</div>';
        detailsPanel.innerHTML = html;
    }

    function clearNodeDetails() {
        const detailsPanel = document.getElementById('node-details');
        if (!detailsPanel) return;

        detailsPanel.innerHTML = `
            <div class="no-selection">
                <div class="no-selection-icon">?</div>
                <p>Click on a node to view details</p>
            </div>
        `;
    }

    // ========================================================================
    // UTILITY FUNCTIONS
    // ========================================================================

    function showLoading(show) {
        const indicator = document.getElementById('loading-indicator');
        if (indicator) {
            indicator.style.display = show ? 'block' : 'none';
        }
    }

    function showError(message) {
        alert(message);
    }

    function setElementText(id, text) {
        const el = document.getElementById(id);
        if (el) el.textContent = text;
    }

    function truncateLabel(label, maxLength) {
        if (!label) return '';
        if (label.length <= maxLength) return label;
        return label.substring(0, maxLength - 3) + '...';
    }

    function formatValue(value) {
        if (value === null || value === undefined) return '-';
        if (typeof value === 'number') {
            return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
        }
        return String(value);
    }

    function formatNodeType(type) {
        if (!type) return 'Unknown';
        return type.split('_').map(word =>
            word.charAt(0).toUpperCase() + word.slice(1)
        ).join(' ');
    }

    function escapeHtml(text) {
        if (!text) return '';
        const map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return text.replace(/[&<>"']/g, m => map[m]);
    }

    // ========================================================================
    // VIEW MODE SWITCHING
    // ========================================================================

    function switchViewMode(mode) {
        currentViewMode = mode;

        // Hide all view containers
        ['cy', 'sankey-container', 'hierarchy-container', 'timeline-container', 'cell-container'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.style.display = 'none';
        });

        // Show the selected view
        updateVisualization();

        // Update view mode selector
        document.querySelectorAll('.view-mode-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.mode === mode);
        });
    }

    // ========================================================================
    // FILTERING
    // ========================================================================

    function filterByTableType(type) {
        if (!cy) return;

        if (type === 'all') {
            cy.nodes().show();
        } else {
            cy.nodes().hide();
            cy.nodes(`[type="${type}"]`).show();

            // Also show connected edges
            cy.nodes().forEach(node => {
                if (node.visible()) {
                    node.connectedEdges().show();
                    node.connectedEdges().connectedNodes().show();
                }
            });
        }
    }

    function filterByUsage(showUsed, showUnused) {
        if (!cy) return;

        cy.nodes().forEach(node => {
            const wasUsed = node.data('wasUsed');
            if (wasUsed === true && showUsed) {
                node.show();
            } else if (wasUsed === false && showUnused) {
                node.show();
            } else if (wasUsed === undefined) {
                node.show(); // Tables and other nodes without usage tracking
            } else {
                node.hide();
            }
        });
    }

    // ========================================================================
    // EXPORT FUNCTIONS
    // ========================================================================

    function exportGraph(format) {
        if (!cy) return;

        switch (format) {
            case 'png':
                const png = cy.png({ full: true, scale: 2 });
                downloadFile(png, 'lineage-graph.png', 'image/png');
                break;
            case 'json':
                const json = JSON.stringify(cy.json(), null, 2);
                downloadFile('data:text/json;charset=utf-8,' + encodeURIComponent(json),
                    'lineage-graph.json', 'application/json');
                break;
        }
    }

    function downloadFile(dataUrl, filename, mimeType) {
        const a = document.createElement('a');
        a.href = dataUrl;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    }

    // ========================================================================
    // INITIALIZATION
    // ========================================================================

    function init() {
        // Initialize Cytoscape
        initializeCytoscape();

        // Load initial data
        loadLineageData();

        // Set up event handlers
        setupEventHandlers();
    }

    function setupEventHandlers() {
        // View mode buttons
        document.querySelectorAll('.view-mode-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                switchViewMode(btn.dataset.mode);
            });
        });

        // Zoom controls
        document.getElementById('zoom-in')?.addEventListener('click', () => {
            if (cy) cy.zoom(cy.zoom() * 1.2);
        });

        document.getElementById('zoom-out')?.addEventListener('click', () => {
            if (cy) cy.zoom(cy.zoom() * 0.8);
        });

        document.getElementById('fit-graph')?.addEventListener('click', () => {
            if (cy) cy.fit();
        });

        // Layout select
        document.getElementById('layout-select')?.addEventListener('change', (e) => {
            currentLayout = e.target.value;
            applyLayout();
        });

        // Detail level
        document.getElementById('detail-level')?.addEventListener('change', () => {
            loadLineageData();
        });

        // Max rows
        document.getElementById('max-rows')?.addEventListener('change', () => {
            loadLineageData();
        });

        // Hide empty tables
        document.getElementById('hide-empty-tables')?.addEventListener('change', () => {
            loadLineageData();
        });

        // Show labels toggle
        document.getElementById('show-labels')?.addEventListener('change', (e) => {
            if (cy) {
                cy.style()
                    .selector('node')
                    .style('label', e.target.checked ? 'data(label)' : '')
                    .update();
            }
        });

        // Table type filter
        document.getElementById('table-type-filter')?.addEventListener('change', (e) => {
            filterByTableType(e.target.value);
        });

        // Usage filter checkboxes
        document.getElementById('show-used')?.addEventListener('change', updateUsageFilter);
        document.getElementById('show-unused')?.addEventListener('change', updateUsageFilter);

        // Export buttons
        document.getElementById('export-png')?.addEventListener('click', () => exportGraph('png'));
        document.getElementById('export-json')?.addEventListener('click', () => exportGraph('json'));
    }

    function updateUsageFilter() {
        const showUsed = document.getElementById('show-used')?.checked ?? true;
        const showUnused = document.getElementById('show-unused')?.checked ?? true;
        filterByUsage(showUsed, showUnused);
    }

    // Expose functions to global scope for template usage
    window.enhancedLineageViewer = {
        init,
        loadLineageData,
        switchViewMode,
        filterByTableType,
        filterByUsage,
        exportGraph,
        applyLayout: () => applyLayout()
    };

    window.showCellInGraph = function(cellCode) {
        switchViewMode('graph');
        // Highlight nodes related to this cell
        // Implementation depends on cell code format
    };

    // Auto-initialize on DOM ready
    document.addEventListener('DOMContentLoaded', init);
})();
