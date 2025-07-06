// Lineage Viewer JavaScript
(function() {
    'use strict';
    
    let cy = null;
    let lineageData = null;
    let currentLayout = 'breadthfirst';
    
    // Node styling based on type
    const nodeStyles = {
        'database_table': {
            'background-color': '#4A90E2',
            'shape': 'round-rectangle',
            'width': '120px',
            'height': '60px'
        },
        'derived_table': {
            'background-color': '#50C878',
            'shape': 'round-rectangle',
            'width': '120px',
            'height': '60px'
        },
        'database_field': {
            'background-color': '#FFB84D',
            'shape': 'ellipse',
            'width': '80px',
            'height': '40px'
        },
        'function': {
            'background-color': '#FF6B6B',
            'shape': 'diamond',
            'width': '80px',
            'height': '40px'
        },
        'database_row': {
            'background-color': '#A0A0A0',
            'shape': 'round-rectangle',
            'width': '60px',
            'height': '30px'
        },
        'derived_row': {
            'background-color': '#A0A0A0',
            'shape': 'round-rectangle',
            'width': '60px',
            'height': '30px'
        },
        'database_column_value': {
            'background-color': '#9370DB',
            'shape': 'ellipse',
            'width': '50px',
            'height': '25px'
        },
        'evaluated_function': {
            'background-color': '#20B2AA',
            'shape': 'hexagon',
            'width': '60px',
            'height': '30px'
        },
        'table_creation_function': {
            'background-color': '#8B4513',
            'shape': 'octagon',
            'width': '100px',
            'height': '50px'
        }
    };
    
    // Edge styling based on type
    const edgeStyles = {
        'has_field': {
            'line-color': '#cccccc',
            'target-arrow-color': '#cccccc',
            'width': 2
        },
        'has_function': {
            'line-color': '#cccccc',
            'target-arrow-color': '#cccccc',
            'width': 2
        },
        'depends_on': {
            'line-color': '#FF6B6B',
            'target-arrow-color': '#FF6B6B',
            'width': 3,
            'line-style': 'dashed'
        },
        'derived_from': {
            'line-color': '#50C878',
            'target-arrow-color': '#50C878',
            'width': 4
        },
        'contains_row': {
            'line-color': '#e0e0e0',
            'target-arrow-color': '#e0e0e0',
            'width': 1
        },
        'row_derived_from': {
            'line-color': '#808080',
            'target-arrow-color': '#808080',
            'width': 2,
            'line-style': 'dotted'
        },
        'has_value': {
            'line-color': '#9370DB',
            'target-arrow-color': '#9370DB',
            'width': 1
        },
        'has_evaluated_function': {
            'line-color': '#20B2AA',
            'target-arrow-color': '#20B2AA',
            'width': 2
        },
        'instance_of_field': {
            'line-color': '#FFB84D',
            'target-arrow-color': '#FFB84D',
            'width': 1,
            'line-style': 'dashed'
        },
        'instance_of_function': {
            'line-color': '#FF6B6B',
            'target-arrow-color': '#FF6B6B',
            'width': 1,
            'line-style': 'dashed'
        },
        'derived_from_value': {
            'line-color': '#8B4513',
            'target-arrow-color': '#8B4513',
            'width': 3,
            'line-style': 'solid'
        },
        'derived_from_row': {
            'line-color': '#556B2F',
            'target-arrow-color': '#556B2F',
            'width': 2,
            'line-style': 'solid'
        },
        'creates_table': {
            'line-color': '#8B4513',
            'target-arrow-color': '#8B4513',
            'width': 4,
            'line-style': 'solid'
        },
        'references_column': {
            'line-color': '#D2691E',
            'target-arrow-color': '#D2691E',
            'width': 2,
            'line-style': 'dashed'
        }
    };
    
    // Initialize Cytoscape
    function initializeCytoscape() {
        const container = document.getElementById('cy');
        console.log('Initializing Cytoscape with container:', container);
        
        if (!container) {
            console.error('Cytoscape container #cy not found!');
            return;
        }
        
        console.log('Container dimensions:', container.offsetWidth, 'x', container.offsetHeight);
        
        // Ensure container has dimensions
        if (container.offsetWidth === 0 || container.offsetHeight === 0) {
            console.warn('Container has zero dimensions, setting fallback size');
            container.style.width = '800px';
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
                        'text-outline-color': '#333',
                        'font-size': '12px',
                        'border-width': 2,
                        'border-color': '#333'
                    }
                },
                {
                    selector: 'edge',
                    style: {
                        'curve-style': 'bezier',
                        'target-arrow-shape': 'triangle',
                        'arrow-scale': 1.5,
                        'label': 'data(label)',
                        'font-size': '10px',
                        'text-rotation': 'autorotate',
                        'text-margin-y': -10
                    }
                },
                {
                    selector: ':selected',
                    style: {
                        'border-width': 4,
                        'border-color': '#ff0000'
                    }
                }
            ],
            
            layout: {
                name: 'preset'
            },
            
            minZoom: 0.1,
            maxZoom: 5
        });
        
        console.log('Cytoscape initialized:', cy);
        
        // Test that cytoscape is working
        cy.ready(function() {
            console.log('Cytoscape is ready');
            
            // Add a test node to verify the graph is working
            cy.add({
                group: 'nodes',
                data: { id: 'test', label: 'Test Node', type: 'database_table' }
            });
            
            console.log('Test node added, total nodes:', cy.nodes().length);
            
            // Remove test node after 2 seconds
            setTimeout(() => {
                cy.remove('#test');
                console.log('Test node removed');
            }, 2000);
        });
        
        // Apply type-specific styles
        Object.keys(nodeStyles).forEach(type => {
            cy.style().selector(`node[type="${type}"]`).style(nodeStyles[type]).update();
        });
        
        Object.keys(edgeStyles).forEach(type => {
            cy.style().selector(`edge[type="${type}"]`).style(edgeStyles[type]).update();
        });
        
        // Node click handler
        cy.on('tap', 'node', function(event) {
            const node = event.target;
            showNodeDetails(node);
        });
        
        // Background click handler
        cy.on('tap', function(event) {
            if (event.target === cy) {
                clearNodeDetails();
            }
        });
    }
    
    // Load lineage data
    function loadLineageData() {
        const trailId = document.getElementById('trail-data').dataset.trailId;
        const detailLevel = document.getElementById('detail-level').value;
        const maxRows = document.getElementById('max-rows').value;
        const maxValues = document.getElementById('max-values').value;
        const hideEmpty = document.getElementById('hide-empty-tables').checked;
        
        console.log('Loading lineage data...', { trailId, detailLevel, maxRows, maxValues, hideEmpty });
        document.getElementById('loading-indicator').style.display = 'block';
        
        const params = new URLSearchParams({
            detail: detailLevel,
            max_rows: maxRows,
            max_values: maxValues,
            hide_empty: hideEmpty
        });
        
        fetch(`/pybirdai/api/trail/${trailId}/lineage?${params}`)
            .then(response => {
                console.log('Response received:', response.status);
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                return response.json();
            })
            .then(data => {
                console.log('Data loaded:', data);
                
                // Check if we got an error response
                if (data.error) {
                    throw new Error(data.error);
                }
                
                lineageData = data;
                updateGraph();
                updateSummary();
                document.getElementById('loading-indicator').style.display = 'none';
            })
            .catch(error => {
                console.error('Error loading lineage data:', error);
                alert('Failed to load lineage data: ' + error.message);
                document.getElementById('loading-indicator').style.display = 'none';
            });
    }
    
    // Update graph with new data
    function updateGraph() {
        if (!cy || !lineageData) {
            console.log('Cannot update graph - missing cy or lineageData');
            return;
        }
        
        console.log('Updating graph with', lineageData.nodes.length, 'nodes and', lineageData.edges.length, 'edges');
        
        // Clear existing elements
        cy.elements().remove();
        
        // Add nodes
        lineageData.nodes.forEach((node, index) => {
            try {
                cy.add({
                    group: 'nodes',
                    data: {
                        id: node.id,
                        label: node.label,
                        type: node.type,
                        details: node.details
                    }
                });
            } catch (error) {
                console.error('Error adding node', index, node, error);
            }
        });
        
        // Add edges
        lineageData.edges.forEach((edge, index) => {
            try {
                cy.add({
                    group: 'edges',
                    data: {
                        id: `edge_${edge.source}_${edge.target}`,
                        source: edge.source,
                        target: edge.target,
                        type: edge.type,
                        label: edge.label || ''
                    }
                });
            } catch (error) {
                console.error('Error adding edge', index, edge, error);
            }
        });
        
        console.log('Graph updated, applying layout...');
        
        // Apply layout
        applyLayout();
    }
    
    // Apply selected layout
    function applyLayout() {
        if (!cy) {
            console.log('Cannot apply layout - cy not initialized');
            return;
        }
        
        const nodeCount = cy.nodes().length;
        const edgeCount = cy.edges().length;
        
        console.log(`Applying ${currentLayout} layout to ${nodeCount} nodes and ${edgeCount} edges`);
        
        let layoutOptions = {
            name: currentLayout,
            animate: true,
            animationDuration: 500,
            fit: true,
            padding: 50
        };
        
        // Layout-specific options
        if (currentLayout === 'breadthfirst') {
            layoutOptions.directed = true;
            layoutOptions.spacingFactor = 1.5;
            layoutOptions.levelSeparation = 100;
        } else if (currentLayout === 'cose') {
            // Use built-in cose layout instead of cose-bilkent
            layoutOptions.idealEdgeLength = 100;
            layoutOptions.nodeRepulsion = 4500;
            layoutOptions.nodeOverlap = 20;
            layoutOptions.numIter = 50;
        } else if (currentLayout === 'circle') {
            layoutOptions.radius = 200;
        }
        
        const layout = cy.layout(layoutOptions);
        layout.on('layoutready', () => {
            console.log('Layout completed successfully');
        });
        layout.run();
    }
    
    // Update summary panel
    function updateSummary() {
        if (!lineageData || !lineageData.summary) {
            console.log('Cannot update summary - missing data');
            return;
        }
        
        console.log('Updating summary with:', lineageData.summary);
        
        // Safely access nested properties
        const tableCount = lineageData.summary.table_count || {};
        
        document.getElementById('db-table-count').textContent = 
            tableCount.database || 0;
        document.getElementById('derived-table-count').textContent = 
            tableCount.derived || 0;
        document.getElementById('total-nodes').textContent = 
            lineageData.summary.total_nodes || 0;
        document.getElementById('total-edges').textContent = 
            lineageData.summary.total_edges || 0;
    }
    
    // Show node details
    function showNodeDetails(node) {
        const nodeData = node.data();
        const detailsPanel = document.getElementById('node-details');
        
        // Start building HTML
        let html = `
            <div class="detail-section">
                <h5>${nodeData.label}</h5>
                <p><strong>Type:</strong> ${formatNodeType(nodeData.type)}</p>
        `;
        
        // Add type-specific details
        if (nodeData.details) {
            const details = nodeData.details;
            
            switch (nodeData.type) {
                case 'database_table':
                case 'derived_table':
                    html += `
                        <p><strong>Columns:</strong> ${details.column_count || details.function_count || 0}</p>
                        <p><strong>Rows:</strong> ${details.row_count || 0}</p>
                    `;
                    if (details.creation_function) {
                        html += `<p><strong>Creation Function:</strong> ${details.creation_function}</p>`;
                    }
                    break;
                    
                case 'function':
                    if (details.function_text) {
                        html += `
                            <div class="mt-3">
                                <strong>Function Code:</strong>
                                <pre class="bg-light p-2 rounded" style="max-height: 200px; overflow-y: auto;">${escapeHtml(details.function_text)}</pre>
                            </div>
                        `;
                    }
                    break;
                    
                case 'database_field':
                    html += `<p><strong>Table:</strong> ${details.table}</p>`;
                    break;
            }
        }
        
        html += '</div>';
        
        // Add relationships
        const incomingEdges = node.incomers('edge');
        const outgoingEdges = node.outgoers('edge');
        
        if (outgoingEdges.length > 0) {
            html += `
                <div class="detail-section">
                    <h6>Dependencies (${outgoingEdges.length})</h6>
            `;
            outgoingEdges.forEach(edge => {
                const target = edge.target();
                html += `<div class="dependency-item">${edge.data('type')}: ${target.data('label')}</div>`;
            });
            html += '</div>';
        }
        
        if (incomingEdges.length > 0) {
            html += `
                <div class="detail-section">
                    <h6>Used By (${incomingEdges.length})</h6>
            `;
            incomingEdges.forEach(edge => {
                const source = edge.source();
                html += `<div class="dependency-item">${source.data('label')}</div>`;
            });
            html += '</div>';
        }
        
        // Load additional details if needed
        if (nodeData.details && (nodeData.details.table_id || nodeData.details.function_id)) {
            loadAdditionalDetails(nodeData);
        }
        
        detailsPanel.innerHTML = html;
    }
    
    // Load additional node details from server
    function loadAdditionalDetails(nodeData) {
        const trailId = document.getElementById('trail-data').dataset.trailId;
        const nodeId = nodeData.details.table_id || nodeData.details.function_id || nodeData.details.field_id;
        
        if (!nodeId) return;
        
        fetch(`/pybirdai/api/trail/${trailId}/node/${nodeData.type}/${nodeId}`)
            .then(response => response.json())
            .then(details => {
                // Update details panel with additional information
                const detailsPanel = document.getElementById('node-details');
                let additionalHtml = '';
                
                if (details.sample_data && details.sample_data.length > 0) {
                    additionalHtml += `
                        <div class="detail-section">
                            <h6>Sample Data</h6>
                            <div class="table-responsive">
                                <table class="table table-sm sample-data-table">
                                    <thead>
                                        <tr>
                    `;
                    
                    // Get column headers
                    const columns = Object.keys(details.sample_data[0]).filter(k => k !== 'row_id');
                    columns.forEach(col => {
                        additionalHtml += `<th>${col}</th>`;
                    });
                    
                    additionalHtml += `
                                        </tr>
                                    </thead>
                                    <tbody>
                    `;
                    
                    // Add rows
                    details.sample_data.forEach(row => {
                        additionalHtml += '<tr>';
                        columns.forEach(col => {
                            additionalHtml += `<td>${row[col] || '-'}</td>`;
                        });
                        additionalHtml += '</tr>';
                    });
                    
                    additionalHtml += `
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    `;
                }
                
                detailsPanel.innerHTML += additionalHtml;
            })
            .catch(error => {
                console.error('Error loading additional details:', error);
            });
    }
    
    // Clear node details
    function clearNodeDetails() {
        document.getElementById('node-details').innerHTML = `
            <div class="no-selection">
                <i class="fas fa-mouse-pointer fa-2x mb-3"></i>
                <p>Click on a node to view details</p>
            </div>
        `;
    }
    
    // Utility functions
    function formatNodeType(type) {
        return type.split('_').map(word => 
            word.charAt(0).toUpperCase() + word.slice(1)
        ).join(' ');
    }
    
    function escapeHtml(text) {
        const map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return text.replace(/[&<>"']/g, m => map[m]);
    }
    
    // Event handlers
    document.addEventListener('DOMContentLoaded', function() {
        initializeCytoscape();
        loadLineageData();
        
        // Control event handlers
        document.getElementById('zoom-in').addEventListener('click', () => {
            cy.zoom(cy.zoom() * 1.2);
        });
        
        document.getElementById('zoom-out').addEventListener('click', () => {
            cy.zoom(cy.zoom() * 0.8);
        });
        
        document.getElementById('fit-graph').addEventListener('click', () => {
            cy.fit();
        });
        
        document.getElementById('detail-level').addEventListener('change', () => {
            loadLineageData();
        });
        
        document.getElementById('max-rows').addEventListener('change', () => {
            loadLineageData();
        });
        
        document.getElementById('max-values').addEventListener('change', () => {
            loadLineageData();
        });
        
        document.getElementById('hide-empty-tables').addEventListener('change', () => {
            loadLineageData();
        });
        
        document.getElementById('show-labels').addEventListener('change', (e) => {
            if (e.target.checked) {
                cy.style().selector('node').style('label', 'data(label)').update();
            } else {
                cy.style().selector('node').style('label', '').update();
            }
        });
        
        document.getElementById('layout-select').addEventListener('change', (e) => {
            currentLayout = e.target.value;
            applyLayout();
        });
    });
})();