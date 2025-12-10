// ANCRDT Tables Graph Visualization
// Copyright (c) 2025 Arfa Digital Consulting
// SPDX-License-Identifier: EPL-2.0

(function() {
    'use strict';

    let cy = null;
    let graphData = null;
    let currentLayout = 'dagre';

    // Node styling based on type
    const nodeColors = {
        'ancrdt_cube': '#4A90E2',
        'il_table': '#2E8B57',
        'assignment_table': '#FFA500',
        'filter_table': '#CD853F'
    };

    // Edge styling based on type
    const edgeStyles = {
        'joins_to': {
            'line-color': '#4A90E2',
            'target-arrow-color': '#4A90E2',
            'width': 3,
            'line-style': 'solid'
        },
        'relates_to': {
            'line-color': '#888888',
            'target-arrow-color': '#888888',
            'width': 2,
            'line-style': 'dashed'
        },
        'filtered_by': {
            'line-color': '#CD853F',
            'target-arrow-color': '#CD853F',
            'width': 2,
            'line-style': 'dotted'
        }
    };

    // Initialize Cytoscape
    function initializeCytoscape() {
        const container = document.getElementById('cy');
        if (!container) {
            console.error('Cytoscape container #cy not found!');
            return;
        }

        // Register dagre layout if available
        if (typeof cytoscape !== 'undefined' && typeof cytoscapeDagre !== 'undefined') {
            cytoscape.use(cytoscapeDagre);
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
                        'text-outline-color': '#333',
                        'font-size': '11px',
                        'font-weight': 'bold',
                        'border-width': 2,
                        'border-color': '#333',
                        'shape': 'round-rectangle',
                        'width': 'label',
                        'height': 40,
                        'padding': '15px',
                        'text-wrap': 'wrap',
                        'text-max-width': '120px'
                    }
                },
                {
                    selector: 'node[type="ancrdt_cube"]',
                    style: {
                        'background-color': nodeColors['ancrdt_cube'],
                        'shape': 'round-rectangle',
                        'width': 140,
                        'height': 50
                    }
                },
                {
                    selector: 'node[type="il_table"]',
                    style: {
                        'background-color': nodeColors['il_table'],
                        'shape': 'round-rectangle'
                    }
                },
                {
                    selector: 'node[type="assignment_table"]',
                    style: {
                        'background-color': nodeColors['assignment_table'],
                        'shape': 'round-rectangle'
                    }
                },
                {
                    selector: 'node[type="filter_table"]',
                    style: {
                        'background-color': nodeColors['filter_table'],
                        'shape': 'diamond',
                        'width': 100,
                        'height': 50
                    }
                },
                {
                    selector: 'edge',
                    style: {
                        'curve-style': 'bezier',
                        'target-arrow-shape': 'triangle',
                        'arrow-scale': 1.2,
                        'font-size': '9px',
                        'text-rotation': 'autorotate',
                        'text-margin-y': -10,
                        'color': '#666'
                    }
                },
                {
                    selector: 'edge[type="joins_to"]',
                    style: edgeStyles['joins_to']
                },
                {
                    selector: 'edge[type="relates_to"]',
                    style: edgeStyles['relates_to']
                },
                {
                    selector: 'edge[type="filtered_by"]',
                    style: edgeStyles['filtered_by']
                },
                {
                    selector: ':selected',
                    style: {
                        'border-width': 4,
                        'border-color': '#ff0000',
                        'overlay-padding': 5,
                        'overlay-color': '#ff0000',
                        'overlay-opacity': 0.2
                    }
                },
                {
                    selector: '.highlighted',
                    style: {
                        'border-width': 4,
                        'border-color': '#00ff00',
                        'overlay-padding': 5,
                        'overlay-color': '#00ff00',
                        'overlay-opacity': 0.2
                    }
                }
            ],

            layout: {
                name: 'preset'
            },

            minZoom: 0.1,
            maxZoom: 4,
            wheelSensitivity: 0.3
        });

        // Node click handler
        cy.on('tap', 'node', function(event) {
            const node = event.target;
            showNodeDetails(node);
            highlightConnections(node);
        });

        // Edge click handler
        cy.on('tap', 'edge', function(event) {
            const edge = event.target;
            showEdgeDetails(edge);
        });

        // Background click handler
        cy.on('tap', function(event) {
            if (event.target === cy) {
                clearNodeDetails();
                cy.elements().removeClass('highlighted');
            }
        });

        console.log('Cytoscape initialized successfully');
    }

    // Load graph data from API
    function loadGraphData() {
        document.getElementById('loading-indicator').style.display = 'block';

        fetch('/pybirdai/api/ancrdt/tables/graph/')
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                return response.json();
            })
            .then(data => {
                if (data.error) {
                    throw new Error(data.message || data.error);
                }

                graphData = data;
                updateGraph();
                updateSummary();
                document.getElementById('loading-indicator').style.display = 'none';
            })
            .catch(error => {
                console.error('Error loading graph data:', error);
                document.getElementById('loading-indicator').innerHTML = `
                    <div class="text-danger">
                        <i class="fas fa-exclamation-triangle fa-3x mb-3"></i>
                        <p>Failed to load graph data</p>
                        <small>${error.message}</small>
                        <br><br>
                        <button class="btn btn-sm btn-primary" onclick="location.reload()">
                            <i class="fas fa-redo"></i> Retry
                        </button>
                    </div>
                `;
            });
    }

    // Update graph with loaded data
    function updateGraph() {
        if (!cy || !graphData) {
            return;
        }

        // Clear existing elements
        cy.elements().remove();

        // Add nodes
        graphData.nodes.forEach(node => {
            cy.add({
                group: 'nodes',
                data: {
                    id: node.id,
                    label: node.label,
                    type: node.type,
                    color: node.color,
                    details: node.details
                }
            });
        });

        // Add edges
        graphData.edges.forEach(edge => {
            cy.add({
                group: 'edges',
                data: {
                    id: edge.id,
                    source: edge.source,
                    target: edge.target,
                    type: edge.type,
                    label: edge.label || '',
                    details: edge.details
                }
            });
        });

        console.log(`Graph updated with ${graphData.nodes.length} nodes and ${graphData.edges.length} edges`);

        // Apply layout
        applyLayout();
    }

    // Apply selected layout
    function applyLayout() {
        if (!cy) return;

        let layoutOptions = {
            name: currentLayout,
            animate: true,
            animationDuration: 500,
            fit: true,
            padding: 50
        };

        if (currentLayout === 'dagre') {
            layoutOptions = {
                name: 'dagre',
                rankDir: 'TB',
                nodeSep: 80,
                rankSep: 100,
                edgeSep: 50,
                animate: true,
                animationDuration: 500,
                fit: true,
                padding: 50
            };
        } else if (currentLayout === 'breadthfirst') {
            layoutOptions.directed = true;
            layoutOptions.spacingFactor = 1.5;
            layoutOptions.roots = cy.nodes('[type="ancrdt_cube"]').map(n => n.id());
        } else if (currentLayout === 'cose') {
            layoutOptions.idealEdgeLength = 150;
            layoutOptions.nodeRepulsion = 10000;
            layoutOptions.nodeOverlap = 20;
            layoutOptions.numIter = 100;
        } else if (currentLayout === 'circle') {
            layoutOptions.radius = 300;
        }

        const layout = cy.layout(layoutOptions);
        layout.run();
    }

    // Update summary panel
    function updateSummary() {
        if (!graphData || !graphData.summary) return;

        const summary = graphData.summary;
        document.getElementById('ancrdt-cube-count').textContent = summary.ancrdt_cubes || 0;
        document.getElementById('il-table-count').textContent = summary.il_tables || 0;
        document.getElementById('assignment-table-count').textContent =
            (summary.assignment_tables || 0) + (summary.filter_tables || 0);
        document.getElementById('total-edges-count').textContent = summary.total_edges || 0;
        document.getElementById('join-def-count').textContent = summary.join_definitions || 0;
        document.getElementById('rolc-mapping-count').textContent = summary.rolc_mappings || 0;
    }

    // Show node details in side panel
    function showNodeDetails(node) {
        const nodeData = node.data();
        const detailsPanel = document.getElementById('node-details');

        let html = `
            <div class="detail-section">
                <h6>
                    <span class="badge" style="background-color: ${nodeData.color || nodeColors[nodeData.type]};">
                        ${formatNodeType(nodeData.type)}
                    </span>
                </h6>
                <p><strong>Name:</strong> ${nodeData.label}</p>
                <p><strong>ID:</strong> <code>${nodeData.id}</code></p>
        `;

        // Add type-specific details
        if (nodeData.details) {
            const details = nodeData.details;

            if (nodeData.type === 'ancrdt_cube' && details.joins) {
                html += `
                    <div class="mt-3">
                        <strong>Configured Joins:</strong>
                        <ul class="list-unstyled mt-2">
                `;
                details.joins.forEach(join => {
                    html += `<li class="join-item"><i class="fas fa-link"></i> ${join}</li>`;
                });
                html += `</ul></div>`;
            }

            if (details.table_type) {
                html += `<p><strong>Table Type:</strong> ${details.table_type}</p>`;
            }

            if (details.parent_join) {
                html += `<p><strong>Part of Join:</strong> ${details.parent_join}</p>`;
            }
        }

        html += '</div>';

        // Add connections info
        const incomingEdges = node.incomers('edge');
        const outgoingEdges = node.outgoers('edge');

        if (outgoingEdges.length > 0) {
            html += `
                <div class="detail-section">
                    <h6><i class="fas fa-arrow-right"></i> Connects To (${outgoingEdges.length})</h6>
            `;
            outgoingEdges.forEach(edge => {
                const target = edge.target();
                html += `
                    <div class="join-item">
                        <small class="text-muted">${edge.data('type')}</small><br>
                        ${target.data('label')}
                    </div>
                `;
            });
            html += '</div>';
        }

        if (incomingEdges.length > 0) {
            html += `
                <div class="detail-section">
                    <h6><i class="fas fa-arrow-left"></i> Connected From (${incomingEdges.length})</h6>
            `;
            incomingEdges.forEach(edge => {
                const source = edge.source();
                html += `
                    <div class="join-item">
                        <small class="text-muted">${edge.data('type')}</small><br>
                        ${source.data('label')}
                    </div>
                `;
            });
            html += '</div>';
        }

        detailsPanel.innerHTML = html;
    }

    // Show edge details
    function showEdgeDetails(edge) {
        const edgeData = edge.data();
        const detailsPanel = document.getElementById('node-details');

        let html = `
            <div class="detail-section">
                <h6><i class="fas fa-link"></i> Relationship</h6>
                <p><strong>Type:</strong> ${formatEdgeType(edgeData.type)}</p>
                <p><strong>From:</strong> ${edge.source().data('label')}</p>
                <p><strong>To:</strong> ${edge.target().data('label')}</p>
        `;

        if (edgeData.label) {
            html += `<p><strong>Label:</strong> ${edgeData.label}</p>`;
        }

        if (edgeData.details) {
            if (edgeData.details.join_name) {
                html += `<p><strong>Join Name:</strong> ${edgeData.details.join_name}</p>`;
            }
            if (edgeData.details.filter) {
                html += `<p><strong>Filter:</strong> ${edgeData.details.filter}</p>`;
            }
        }

        html += '</div>';
        detailsPanel.innerHTML = html;
    }

    // Highlight connected nodes and edges
    function highlightConnections(node) {
        cy.elements().removeClass('highlighted');
        node.neighborhood().addClass('highlighted');
    }

    // Clear node details
    function clearNodeDetails() {
        document.getElementById('node-details').innerHTML = `
            <div class="no-selection">
                <i class="fas fa-mouse-pointer fa-3x mb-3" style="color: #dee2e6;"></i>
                <p>Click on a node to view its details and relationships</p>
            </div>
        `;
    }

    // Utility functions
    function formatNodeType(type) {
        const typeLabels = {
            'ancrdt_cube': 'ANCRDT Cube',
            'il_table': 'IL Table',
            'assignment_table': 'Assignment Table',
            'filter_table': 'Filter Table'
        };
        return typeLabels[type] || type.split('_').map(word =>
            word.charAt(0).toUpperCase() + word.slice(1)
        ).join(' ');
    }

    function formatEdgeType(type) {
        const typeLabels = {
            'joins_to': 'Joins To',
            'relates_to': 'Relates To',
            'filtered_by': 'Filtered By'
        };
        return typeLabels[type] || type.split('_').map(word =>
            word.charAt(0).toUpperCase() + word.slice(1)
        ).join(' ');
    }

    // Export interactive graph as standalone HTML
    function exportInteractiveGraph() {
        if (!cy || !graphData) {
            alert('No graph data available to export.');
            return;
        }

        // Get current node positions
        const nodesWithPositions = graphData.nodes.map(node => {
            const cyNode = cy.getElementById(node.id);
            const pos = cyNode.position();
            return {
                ...node,
                position: { x: pos.x, y: pos.y }
            };
        });

        const exportData = {
            nodes: nodesWithPositions,
            edges: graphData.edges,
            summary: graphData.summary
        };

        const htmlContent = generateStandaloneHTML(exportData);

        // Create and trigger download
        const blob = new Blob([htmlContent], { type: 'text/html' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `ancrdt-tables-graph-${new Date().toISOString().slice(0, 10)}.html`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }

    // Generate standalone HTML for export
    function generateStandaloneHTML(data) {
        const timestamp = new Date().toLocaleString();

        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ANCRDT Tables Graph - Exported ${timestamp}</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.23.0/cytoscape.min.js"><\/script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/dagre/0.8.5/dagre.min.js"><\/script>
    <script src="https://cdn.jsdelivr.net/npm/cytoscape-dagre@2.5.0/cytoscape-dagre.min.js"><\/script>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
        #container { height: 100vh; display: flex; flex-direction: column; }
        #control-bar {
            background: #f8f9fa; border-bottom: 1px solid #dee2e6;
            padding: 12px 20px; display: flex; align-items: center;
            gap: 15px; flex-wrap: wrap;
        }
        #control-bar h4 { margin-right: 20px; color: #333; }
        .btn {
            padding: 6px 12px; border: 1px solid #ccc; border-radius: 4px;
            background: white; cursor: pointer; font-size: 13px;
        }
        .btn:hover { background: #e9ecef; }
        .btn-group { display: flex; }
        .btn-group .btn { border-radius: 0; margin-left: -1px; }
        .btn-group .btn:first-child { border-radius: 4px 0 0 4px; margin-left: 0; }
        .btn-group .btn:last-child { border-radius: 0 4px 4px 0; }
        select { padding: 6px 10px; border: 1px solid #ccc; border-radius: 4px; font-size: 13px; }
        .legend { display: flex; gap: 15px; margin-left: auto; }
        .legend-item { display: flex; align-items: center; gap: 5px; font-size: 12px; }
        .legend-color { width: 14px; height: 14px; border-radius: 3px; border: 1px solid #333; }
        #main-content { flex: 1; display: flex; overflow: hidden; }
        #cy { flex: 2; background: #1a1a2e; }
        #side-panel {
            width: 320px; background: white; border-left: 1px solid #dee2e6;
            overflow-y: auto; padding: 20px;
        }
        #side-panel h5 { margin-bottom: 15px; color: #333; }
        .no-selection { text-align: center; color: #999; padding: 40px 20px; }
        .detail-section { margin-bottom: 20px; }
        .detail-section h6 { color: #555; border-bottom: 1px solid #eee; padding-bottom: 8px; margin-bottom: 10px; }
        .detail-section p { margin: 5px 0; font-size: 13px; }
        .badge {
            display: inline-block; padding: 3px 8px; border-radius: 4px;
            color: white; font-size: 11px; font-weight: bold;
        }
        .join-item {
            background: #f0f0f0; padding: 8px 10px; border-radius: 4px;
            margin-bottom: 5px; font-size: 12px;
        }
        code { background: #e9ecef; padding: 2px 5px; border-radius: 3px; font-size: 12px; }
        .export-note {
            font-size: 11px; color: #888; margin-left: 10px;
        }
        .stat-row { display: flex; gap: 10px; margin-bottom: 10px; }
        .stat-card {
            flex: 1; padding: 10px; border-radius: 6px; color: white; text-align: center;
        }
        .stat-card.blue { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); }
        .stat-card.green { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); }
        .stat-card.orange { background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); }
        .stat-value { font-size: 1.3rem; font-weight: bold; }
        .stat-label { font-size: 0.7rem; opacity: 0.9; }
    </style>
</head>
<body>
    <div id="container">
        <div id="control-bar">
            <h4>ANCRDT Tables Graph</h4>
            <div class="btn-group">
                <button class="btn" id="zoom-in" title="Zoom In">+</button>
                <button class="btn" id="zoom-out" title="Zoom Out">-</button>
                <button class="btn" id="fit-graph" title="Fit to View">Fit</button>
            </div>
            <select id="layout-select">
                <option value="preset">Current Layout</option>
                <option value="dagre">Hierarchical (Dagre)</option>
                <option value="breadthfirst">Breadth-First</option>
                <option value="cose">Force-Directed</option>
                <option value="circle">Circular</option>
            </select>
            <div class="legend">
                <div class="legend-item"><div class="legend-color" style="background:#4A90E2;"></div>ANCRDT Cube</div>
                <div class="legend-item"><div class="legend-color" style="background:#2E8B57;"></div>IL Table</div>
                <div class="legend-item"><div class="legend-color" style="background:#FFA500;"></div>Assignment</div>
                <div class="legend-item"><div class="legend-color" style="background:#CD853F;"></div>Filter</div>
            </div>
            <span class="export-note">Exported: ${timestamp}</span>
        </div>
        <div id="main-content">
            <div id="cy"></div>
            <div id="side-panel">
                <h5>Graph Summary</h5>
                <div class="stat-row">
                    <div class="stat-card blue">
                        <div class="stat-value">${data.summary?.ancrdt_cubes || 0}</div>
                        <div class="stat-label">ANCRDT Cubes</div>
                    </div>
                    <div class="stat-card green">
                        <div class="stat-value">${data.summary?.il_tables || 0}</div>
                        <div class="stat-label">IL Tables</div>
                    </div>
                </div>
                <div class="stat-row">
                    <div class="stat-card orange">
                        <div class="stat-value">${(data.summary?.assignment_tables || 0) + (data.summary?.filter_tables || 0)}</div>
                        <div class="stat-label">Assignment/Filter</div>
                    </div>
                    <div class="stat-card blue">
                        <div class="stat-value">${data.summary?.total_edges || 0}</div>
                        <div class="stat-label">Relationships</div>
                    </div>
                </div>
                <hr style="margin: 20px 0; border: none; border-top: 1px solid #eee;">
                <h5>Node Details</h5>
                <div id="node-details">
                    <div class="no-selection">Click on a node to view details</div>
                </div>
            </div>
        </div>
    </div>
    <script>
        const graphData = ${JSON.stringify(data, null, 2)};
        const nodeColors = {
            'ancrdt_cube': '#4A90E2', 'il_table': '#2E8B57',
            'assignment_table': '#FFA500', 'filter_table': '#CD853F'
        };
        const edgeStyles = {
            'joins_to': { 'line-color': '#4A90E2', 'target-arrow-color': '#4A90E2', 'width': 3, 'line-style': 'solid' },
            'relates_to': { 'line-color': '#888888', 'target-arrow-color': '#888888', 'width': 2, 'line-style': 'dashed' },
            'filtered_by': { 'line-color': '#CD853F', 'target-arrow-color': '#CD853F', 'width': 2, 'line-style': 'dotted' }
        };

        if (typeof cytoscape !== 'undefined' && typeof cytoscapeDagre !== 'undefined') {
            cytoscape.use(cytoscapeDagre);
        }

        const cy = cytoscape({
            container: document.getElementById('cy'),
            style: [
                { selector: 'node', style: {
                    'label': 'data(label)', 'text-valign': 'center', 'text-halign': 'center',
                    'color': '#fff', 'text-outline-width': 2, 'text-outline-color': '#333',
                    'font-size': '11px', 'font-weight': 'bold', 'border-width': 2, 'border-color': '#333',
                    'shape': 'round-rectangle', 'width': 'label', 'height': 40, 'padding': '15px',
                    'text-wrap': 'wrap', 'text-max-width': '120px'
                }},
                { selector: 'node[type="ancrdt_cube"]', style: { 'background-color': '#4A90E2', 'width': 140, 'height': 50 }},
                { selector: 'node[type="il_table"]', style: { 'background-color': '#2E8B57' }},
                { selector: 'node[type="assignment_table"]', style: { 'background-color': '#FFA500' }},
                { selector: 'node[type="filter_table"]', style: { 'background-color': '#CD853F', 'shape': 'diamond', 'width': 100, 'height': 50 }},
                { selector: 'edge', style: {
                    'curve-style': 'bezier', 'target-arrow-shape': 'triangle', 'arrow-scale': 1.2
                }},
                { selector: 'edge[type="joins_to"]', style: edgeStyles['joins_to'] },
                { selector: 'edge[type="relates_to"]', style: edgeStyles['relates_to'] },
                { selector: 'edge[type="filtered_by"]', style: edgeStyles['filtered_by'] },
                { selector: ':selected', style: { 'border-width': 4, 'border-color': '#ff0000' }},
                { selector: '.highlighted', style: { 'border-width': 4, 'border-color': '#00ff00' }}
            ],
            layout: { name: 'preset' },
            minZoom: 0.1, maxZoom: 4, wheelSensitivity: 0.3
        });

        // Add nodes with positions
        graphData.nodes.forEach(node => {
            cy.add({ group: 'nodes', data: { id: node.id, label: node.label, type: node.type, details: node.details }, position: node.position });
        });
        graphData.edges.forEach(edge => {
            cy.add({ group: 'edges', data: { id: edge.id, source: edge.source, target: edge.target, type: edge.type, label: edge.label || '', details: edge.details }});
        });

        cy.fit(50);

        // Event handlers
        cy.on('tap', 'node', function(e) {
            const node = e.target, d = node.data();
            cy.elements().removeClass('highlighted');
            node.neighborhood().addClass('highlighted');
            let html = '<div class="detail-section"><h6><span class="badge" style="background:' + (nodeColors[d.type] || '#666') + ';">' + formatType(d.type) + '</span></h6>';
            html += '<p><strong>Name:</strong> ' + d.label + '</p><p><strong>ID:</strong> <code>' + d.id + '</code></p>';
            if (d.details && d.details.joins) {
                html += '<div style="margin-top:10px;"><strong>Joins:</strong><ul style="margin:5px 0 0 20px;">';
                d.details.joins.forEach(j => html += '<li style="font-size:12px;">' + j + '</li>');
                html += '</ul></div>';
            }
            html += '</div>';
            const out = node.outgoers('edge'), inc = node.incomers('edge');
            if (out.length) {
                html += '<div class="detail-section"><h6>Connects To (' + out.length + ')</h6>';
                out.forEach(e => html += '<div class="join-item">' + e.target().data('label') + '</div>');
                html += '</div>';
            }
            if (inc.length) {
                html += '<div class="detail-section"><h6>Connected From (' + inc.length + ')</h6>';
                inc.forEach(e => html += '<div class="join-item">' + e.source().data('label') + '</div>');
                html += '</div>';
            }
            document.getElementById('node-details').innerHTML = html;
        });

        cy.on('tap', function(e) {
            if (e.target === cy) {
                cy.elements().removeClass('highlighted');
                document.getElementById('node-details').innerHTML = '<div class="no-selection">Click on a node to view details</div>';
            }
        });

        function formatType(t) {
            return { 'ancrdt_cube': 'ANCRDT Cube', 'il_table': 'IL Table', 'assignment_table': 'Assignment', 'filter_table': 'Filter' }[t] || t;
        }

        function applyLayout(name) {
            if (name === 'preset') { cy.fit(50); return; }
            let opts = { name: name, animate: true, animationDuration: 500, fit: true, padding: 50 };
            if (name === 'dagre') { opts.rankDir = 'TB'; opts.nodeSep = 80; opts.rankSep = 100; }
            else if (name === 'breadthfirst') { opts.directed = true; opts.spacingFactor = 1.5; }
            else if (name === 'cose') { opts.idealEdgeLength = 150; opts.nodeRepulsion = 10000; }
            cy.layout(opts).run();
        }

        document.getElementById('zoom-in').onclick = () => { cy.zoom(cy.zoom() * 1.3); cy.center(); };
        document.getElementById('zoom-out').onclick = () => { cy.zoom(cy.zoom() * 0.7); cy.center(); };
        document.getElementById('fit-graph').onclick = () => cy.fit(50);
        document.getElementById('layout-select').onchange = (e) => applyLayout(e.target.value);
    <\/script>
</body>
</html>`;
    }

    // Event handlers
    document.addEventListener('DOMContentLoaded', function() {
        initializeCytoscape();
        loadGraphData();

        // Zoom controls
        document.getElementById('zoom-in').addEventListener('click', () => {
            cy.zoom(cy.zoom() * 1.3);
            cy.center();
        });

        document.getElementById('zoom-out').addEventListener('click', () => {
            cy.zoom(cy.zoom() * 0.7);
            cy.center();
        });

        document.getElementById('fit-graph').addEventListener('click', () => {
            cy.fit(50);
        });

        // Layout selector
        document.getElementById('layout-select').addEventListener('change', (e) => {
            currentLayout = e.target.value;
            applyLayout();
        });

        // Show/hide labels
        document.getElementById('show-labels').addEventListener('change', (e) => {
            if (e.target.checked) {
                cy.style().selector('node').style('label', 'data(label)').update();
            } else {
                cy.style().selector('node').style('label', '').update();
            }
        });

        // Show/hide edge labels
        document.getElementById('show-edge-labels').addEventListener('change', (e) => {
            if (e.target.checked) {
                cy.style().selector('edge').style('label', 'data(label)').update();
            } else {
                cy.style().selector('edge').style('label', '').update();
            }
        });

        // Refresh button
        document.getElementById('refresh-data').addEventListener('click', () => {
            loadGraphData();
        });

        // Export button
        document.getElementById('export-graph').addEventListener('click', () => {
            exportInteractiveGraph();
        });
    });
})();
