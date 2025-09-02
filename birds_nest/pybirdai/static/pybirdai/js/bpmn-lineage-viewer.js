/**
 * BPMN Metadata Lineage Viewer
 * 
 * Creates BPMN 2.0 compliant visualizations for metadata lineage using:
 * - UserTask: Rounded rectangles with user icon (input data consumed)
 * - ServiceTask: Rounded rectangles with gear icon (output data produced)
 * - SequenceFlow: Arrows connecting tasks (transformation processes)
 * 
 * Based on BPMN 2.0 visual standards.
 */

class BPMNLineageViewer {
    constructor(containerId, options = {}) {
        this.container = document.getElementById(containerId);
        this.options = {
            width: options.width || 1200,
            height: options.height || 800,
            nodeWidth: options.nodeWidth || 120,
            nodeHeight: options.nodeHeight || 80,
            levelGap: options.levelGap || 200,
            nodeGap: options.nodeGap || 150,
            ...options
        };
        
        this.svg = null;
        this.nodes = [];
        this.edges = [];
        this.layout = {};
        
        this.initSVG();
    }
    
    initSVG() {
        // Clear existing content
        this.container.innerHTML = '';
        
        // Create SVG
        this.svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        this.svg.setAttribute('width', this.options.width);
        this.svg.setAttribute('height', this.options.height);
        this.svg.setAttribute('viewBox', `0 0 ${this.options.width} ${this.options.height}`);
        this.svg.style.border = '1px solid #ddd';
        this.svg.style.backgroundColor = '#fafafa';
        
        // Add BPMN styles
        const defs = document.createElementNS('http://www.w3.org/2000/svg', 'defs');
        
        // Arrow marker for sequence flows
        const arrowMarker = document.createElementNS('http://www.w3.org/2000/svg', 'marker');
        arrowMarker.setAttribute('id', 'arrowhead');
        arrowMarker.setAttribute('markerWidth', '10');
        arrowMarker.setAttribute('markerHeight', '7');
        arrowMarker.setAttribute('refX', '9');
        arrowMarker.setAttribute('refY', '3.5');
        arrowMarker.setAttribute('orient', 'auto');
        
        const arrowPath = document.createElementNS('http://www.w3.org/2000/svg', 'polygon');
        arrowPath.setAttribute('points', '0 0, 10 3.5, 0 7');
        arrowPath.setAttribute('fill', '#333');
        
        arrowMarker.appendChild(arrowPath);
        defs.appendChild(arrowMarker);
        this.svg.appendChild(defs);
        
        this.container.appendChild(this.svg);
    }
    
    async loadData(datapoint_id) {
        try {
            const response = await fetch(`/pybirdai/datapoint/${datapoint_id}/bpmn_metadata_lineage/process/`);
            const data = await response.json();
            
            if (data.success && data.lineage) {
                this.renderLineage(data.lineage);
                return data.lineage;
            } else {
                throw new Error(data.error || 'Failed to load lineage data');
            }
        } catch (error) {
            console.error('Error loading BPMN lineage data:', error);
            this.showError(error.message);
            throw error;
        }
    }
    
    renderLineage(lineage) {
        // Convert lineage data to nodes and edges
        this.processLineageData(lineage);
        
        // Calculate layout
        this.calculateLayout();
        
        // Clear SVG
        while (this.svg.children.length > 1) { // Keep defs
            this.svg.removeChild(this.svg.lastChild);
        }
        
        // Render edges first (so they appear behind nodes)
        this.renderEdges();
        
        // Render nodes
        this.renderNodes();
        
        // Add title
        this.addTitle('BPMN Metadata Lineage Workflow');
    }
    
    processLineageData(lineage) {
        this.nodes = [];
        this.edges = [];
        
        // Process UserTasks (input data)
        lineage.user_tasks.forEach(task => {
            this.nodes.push({
                id: `user_task_${task.id}`,
                type: 'UserTask',
                label: this.truncateText(task.name, 15),
                fullLabel: task.name,
                description: task.description,
                entity_reference: task.entity_reference,
                level: 0 // Input level
            });
        });
        
        // Process ServiceTasks (output data)
        lineage.service_tasks.forEach(task => {
            let level = 1; // Default to output level
            
            // Determine level based on task type
            if (task.enriched_attribute_reference) {
                if (task.enriched_attribute_reference.includes('.')) {
                    const parts = task.enriched_attribute_reference.split('.');
                    if (parts.length === 2) {
                        level = 2; // Join table level
                    } else if (parts.length === 3) {
                        level = 2; // Join table column level
                    }
                }
            }
            
            this.nodes.push({
                id: `service_task_${task.id}`,
                type: 'ServiceTask',
                label: this.truncateText(task.name, 15),
                fullLabel: task.name,
                description: task.description,
                enriched_attribute_reference: task.enriched_attribute_reference,
                level: level
            });
        });
        
        // Process SequenceFlows
        lineage.sequence_flows.forEach(flow => {
            if (flow.source_ref && flow.target_ref) {
                const sourceId = flow.source_type === 'UserTask' 
                    ? `user_task_${flow.source_ref}` 
                    : `service_task_${flow.source_ref}`;
                const targetId = flow.target_type === 'UserTask' 
                    ? `user_task_${flow.target_ref}` 
                    : `service_task_${flow.target_ref}`;
                    
                this.edges.push({
                    id: flow.id,
                    source: sourceId,
                    target: targetId,
                    label: this.truncateText(flow.name || '', 12),
                    description: flow.description
                });
            }
        });
    }
    
    calculateLayout() {
        // Group nodes by level
        const levels = {};
        this.nodes.forEach(node => {
            if (!levels[node.level]) {
                levels[node.level] = [];
            }
            levels[node.level].push(node);
        });
        
        // Calculate positions with improved spacing for many nodes
        const startX = 50;
        const startY = 80;
        
        // Find the level with the most nodes to adjust SVG height
        let maxNodesInLevel = 0;
        Object.values(levels).forEach(levelNodes => {
            if (levelNodes.length > maxNodesInLevel) {
                maxNodesInLevel = levelNodes.length;
            }
        });
        
        // Adjust SVG height if needed
        const requiredHeight = startY + (maxNodesInLevel * this.options.nodeGap) + 100;
        if (requiredHeight > this.options.height) {
            this.svg.setAttribute('height', requiredHeight);
            this.svg.setAttribute('viewBox', `0 0 ${this.options.width} ${requiredHeight}`);
        }
        
        Object.keys(levels).forEach(level => {
            const levelNodes = levels[level];
            const levelX = startX + (parseInt(level) * this.options.levelGap);
            
            // Center nodes vertically in their level
            const totalLevelHeight = (levelNodes.length - 1) * this.options.nodeGap;
            const levelStartY = startY + (maxNodesInLevel * this.options.nodeGap - totalLevelHeight) / 2;
            
            levelNodes.forEach((node, index) => {
                const nodeY = levelStartY + (index * this.options.nodeGap);
                
                this.layout[node.id] = {
                    x: levelX,
                    y: nodeY,
                    width: this.options.nodeWidth,
                    height: this.options.nodeHeight
                };
            });
        });
    }
    
    renderNodes() {
        this.nodes.forEach(node => {
            const layout = this.layout[node.id];
            if (!layout) return;
            
            // Create node group
            const nodeGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            nodeGroup.setAttribute('class', 'bpmn-node');
            nodeGroup.setAttribute('transform', `translate(${layout.x}, ${layout.y})`);
            
            // Create main rectangle (BPMN task shape)
            const rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
            rect.setAttribute('width', layout.width);
            rect.setAttribute('height', layout.height);
            rect.setAttribute('rx', '10'); // Rounded corners for BPMN tasks
            rect.setAttribute('ry', '10');
            
            if (node.type === 'UserTask') {
                rect.setAttribute('fill', '#e3f2fd'); // Light blue for user tasks
                rect.setAttribute('stroke', '#1976d2'); // Blue border
            } else {
                rect.setAttribute('fill', '#e8f5e8'); // Light green for service tasks
                rect.setAttribute('stroke', '#388e3c'); // Green border
            }
            
            rect.setAttribute('stroke-width', '2');
            nodeGroup.appendChild(rect);
            
            // Add BPMN icon
            const iconGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            iconGroup.setAttribute('transform', 'translate(8, 8)');
            
            if (node.type === 'UserTask') {
                // User icon for UserTask
                this.createUserIcon(iconGroup);
            } else {
                // Gear icon for ServiceTask
                this.createGearIcon(iconGroup);
            }
            
            nodeGroup.appendChild(iconGroup);
            
            // Add label text
            const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            text.setAttribute('x', layout.width / 2);
            text.setAttribute('y', layout.height / 2 + 5);
            text.setAttribute('text-anchor', 'middle');
            text.setAttribute('dominant-baseline', 'central');
            text.setAttribute('font-family', 'Arial, sans-serif');
            text.setAttribute('font-size', '11');
            text.setAttribute('fill', '#333');
            text.setAttribute('font-weight', 'bold');
            
            // Split long labels into multiple lines
            const words = node.label.split(' ');
            if (words.length > 2) {
                const tspan1 = document.createElementNS('http://www.w3.org/2000/svg', 'tspan');
                tspan1.setAttribute('x', layout.width / 2);
                tspan1.setAttribute('dy', '-6');
                tspan1.textContent = words.slice(0, 2).join(' ');
                text.appendChild(tspan1);
                
                const tspan2 = document.createElementNS('http://www.w3.org/2000/svg', 'tspan');
                tspan2.setAttribute('x', layout.width / 2);
                tspan2.setAttribute('dy', '12');
                tspan2.textContent = words.slice(2).join(' ');
                text.appendChild(tspan2);
            } else {
                text.textContent = node.label;
            }
            
            nodeGroup.appendChild(text);
            
            // Add tooltip behavior
            this.addTooltip(nodeGroup, node);
            
            this.svg.appendChild(nodeGroup);
        });
    }
    
    renderEdges() {
        this.edges.forEach(edge => {
            const sourceLayout = this.layout[edge.source];
            const targetLayout = this.layout[edge.target];
            
            if (!sourceLayout || !targetLayout) return;
            
            // Calculate connection points
            const sourceX = sourceLayout.x + sourceLayout.width;
            const sourceY = sourceLayout.y + sourceLayout.height / 2;
            const targetX = targetLayout.x;
            const targetY = targetLayout.y + targetLayout.height / 2;
            
            // Create sequence flow line
            const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            line.setAttribute('x1', sourceX);
            line.setAttribute('y1', sourceY);
            line.setAttribute('x2', targetX);
            line.setAttribute('y2', targetY);
            line.setAttribute('stroke', '#333');
            line.setAttribute('stroke-width', '2');
            line.setAttribute('marker-end', 'url(#arrowhead)');
            line.setAttribute('class', 'sequence-flow');
            
            this.svg.appendChild(line);
            
            // Add flow label if exists
            if (edge.label) {
                const midX = (sourceX + targetX) / 2;
                const midY = (sourceY + targetY) / 2;
                
                const labelText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                labelText.setAttribute('x', midX);
                labelText.setAttribute('y', midY - 5);
                labelText.setAttribute('text-anchor', 'middle');
                labelText.setAttribute('font-family', 'Arial, sans-serif');
                labelText.setAttribute('font-size', '9');
                labelText.setAttribute('fill', '#666');
                labelText.setAttribute('background', 'white');
                labelText.textContent = edge.label;
                
                this.svg.appendChild(labelText);
            }
        });
    }
    
    createUserIcon(group) {
        // Simple user icon (head + body)
        const head = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        head.setAttribute('cx', '8');
        head.setAttribute('cy', '6');
        head.setAttribute('r', '3');
        head.setAttribute('fill', 'none');
        head.setAttribute('stroke', '#1976d2');
        head.setAttribute('stroke-width', '1.5');
        
        const body = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        body.setAttribute('d', 'M 2 16 Q 2 12 8 12 Q 14 12 14 16');
        body.setAttribute('fill', 'none');
        body.setAttribute('stroke', '#1976d2');
        body.setAttribute('stroke-width', '1.5');
        
        group.appendChild(head);
        group.appendChild(body);
    }
    
    createGearIcon(group) {
        // Simple gear icon
        const gear = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        gear.setAttribute('d', 'M 8 2 L 10 2 L 10 4 L 12 5 L 14 3 L 15.5 4.5 L 13.5 6.5 L 14 8 L 16 8 L 16 10 L 14 10 L 13 12 L 15 14 L 13.5 15.5 L 11.5 13.5 L 10 14 L 10 16 L 8 16 L 8 14 L 6 13 L 4 15 L 2.5 13.5 L 4.5 11.5 L 4 10 L 2 10 L 2 8 L 4 8 L 5 6 L 3 4 L 4.5 2.5 L 6.5 4.5 L 8 4 Z M 8 6 A 2 2 0 0 1 10 8 A 2 2 0 0 1 8 10 A 2 2 0 0 1 6 8 A 2 2 0 0 1 8 6');
        gear.setAttribute('fill', 'none');
        gear.setAttribute('stroke', '#388e3c');
        gear.setAttribute('stroke-width', '1');
        gear.setAttribute('transform', 'scale(0.7)');
        
        group.appendChild(gear);
    }
    
    addTooltip(element, node) {
        element.addEventListener('mouseenter', (e) => {
            this.showTooltip(e, node);
        });
        
        element.addEventListener('mouseleave', () => {
            this.hideTooltip();
        });
    }
    
    showTooltip(event, node) {
        // Remove existing tooltip
        this.hideTooltip();
        
        const tooltip = document.createElement('div');
        tooltip.id = 'bpmn-tooltip';
        tooltip.style.cssText = `
            position: absolute;
            background: #333;
            color: white;
            padding: 8px 12px;
            border-radius: 4px;
            font-size: 12px;
            z-index: 1000;
            max-width: 300px;
            word-wrap: break-word;
            box-shadow: 0 2px 8px rgba(0,0,0,0.2);
        `;
        
        let content = `<strong>${node.fullLabel}</strong>`;
        if (node.description) {
            content += `<br><em>${node.description}</em>`;
        }
        if (node.entity_reference) {
            content += `<br><small>Entity: ${node.entity_reference}</small>`;
        }
        if (node.enriched_attribute_reference) {
            content += `<br><small>Reference: ${node.enriched_attribute_reference}</small>`;
        }
        
        tooltip.innerHTML = content;
        
        document.body.appendChild(tooltip);
        
        // Position tooltip
        const rect = this.container.getBoundingClientRect();
        tooltip.style.left = (event.pageX + 10) + 'px';
        tooltip.style.top = (event.pageY - tooltip.offsetHeight - 10) + 'px';
    }
    
    hideTooltip() {
        const tooltip = document.getElementById('bpmn-tooltip');
        if (tooltip) {
            tooltip.remove();
        }
    }
    
    addTitle(title) {
        const titleText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        titleText.setAttribute('x', this.options.width / 2);
        titleText.setAttribute('y', 30);
        titleText.setAttribute('text-anchor', 'middle');
        titleText.setAttribute('font-family', 'Arial, sans-serif');
        titleText.setAttribute('font-size', '18');
        titleText.setAttribute('font-weight', 'bold');
        titleText.setAttribute('fill', '#333');
        titleText.textContent = title;
        
        this.svg.appendChild(titleText);
    }
    
    truncateText(text, maxLength) {
        if (!text) return '';
        if (text.length <= maxLength) return text;
        return text.substring(0, maxLength - 3) + '...';
    }
    
    showError(message) {
        this.container.innerHTML = `
            <div style="text-align: center; padding: 50px; color: #666;">
                <h3>Error Loading BPMN Lineage</h3>
                <p>${message}</p>
            </div>
        `;
    }
    
    // Export functionality
    exportAsSVG() {
        const serializer = new XMLSerializer();
        const svgString = serializer.serializeToString(this.svg);
        const blob = new Blob([svgString], { type: 'image/svg+xml' });
        const url = URL.createObjectURL(blob);
        
        const link = document.createElement('a');
        link.href = url;
        link.download = 'bpmn-metadata-lineage.svg';
        link.click();
        
        URL.revokeObjectURL(url);
    }
}

// Export for use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = BPMNLineageViewer;
} else if (typeof window !== 'undefined') {
    window.BPMNLineageViewer = BPMNLineageViewer;
}