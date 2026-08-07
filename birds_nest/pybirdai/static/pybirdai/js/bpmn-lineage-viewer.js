/**
 * BPMN Metadata Lineage Viewer
 *
 * Renders metadata lineage as a left-to-right BPMN process diagram:
 * - UserTask: input data consumed (person marker, blue)
 * - ServiceTask: data produced by a transformation (gear marker, green/purple)
 * - SequenceFlow: arrows connecting tasks
 *
 * Tasks that belong to the same table are drawn inside one container (a BPMN
 * pool/lane) so the diagram shows ~20 tables instead of ~120 loose boxes.
 * Containers start collapsed and can be expanded to reveal their columns.
 *
 * Layout is a layered (Sugiyama style) pass: rank assignment from the flow
 * graph, barycenter ordering to reduce edge crossings, then vertical
 * straightening. The model half of this file has no DOM dependency so the
 * layout can be exercised outside the browser.
 */

(function (global) {
    'use strict';

    // ---------------------------------------------------------------------
    // Vocabulary
    // ---------------------------------------------------------------------

    // Task ids are generated with stable prefixes by
    // process_steps/metadata_lineage/bpmn_metadata_lineage_processor.py
    const KIND_BY_PREFIX = [
        ['input_table_', 'inputTable'],
        ['input_col_', 'inputColumn'],
        ['join_table_', 'joinTable'],
        ['join_col_', 'joinColumn'],
        ['output_table_', 'outputTable'],
        ['output_col_', 'outputColumn'],
        ['datapoint_', 'datapoint'],
        ['metric_', 'metric']
    ];

    const KIND_INFO = {
        inputTable: { role: 'container', rank: 0, stage: 'Input tables', bpmn: 'UserTask', palette: 'input' },
        inputColumn: { role: 'item', bpmn: 'UserTask', palette: 'input' },
        joinTable: { role: 'container', rank: 1, stage: 'Join tables', bpmn: 'ServiceTask', palette: 'join' },
        joinColumn: { role: 'item', bpmn: 'ServiceTask', palette: 'join' },
        outputTable: { role: 'container', rank: 2, stage: 'Output table', bpmn: 'ServiceTask', palette: 'output' },
        outputColumn: { role: 'item', bpmn: 'ServiceTask', palette: 'output' },
        datapoint: { role: 'container', rank: 3, stage: 'Datapoint', bpmn: 'EndEvent', palette: 'datapoint' },
        metric: { role: 'item', bpmn: 'ServiceTask', palette: 'datapoint' },
        task: { role: 'container', bpmn: 'Task', palette: 'default' }
    };

    const PALETTE = {
        input: { fill: '#e8f1fc', band: '#d3e4f8', stroke: '#1565c0', text: '#0d3c73' },
        join: { fill: '#e9f5ea', band: '#d5ecd7', stroke: '#2e7d32', text: '#17441c' },
        output: { fill: '#f3ebfa', band: '#e6d7f3', stroke: '#6a1b9a', text: '#3d0f57' },
        datapoint: { fill: '#fdeeea', band: '#f9dbd4', stroke: '#c62828', text: '#7a1414' },
        default: { fill: '#eceff1', band: '#dde3e6', stroke: '#546e7a', text: '#263238' }
    };

    function kindOf(id) {
        for (let i = 0; i < KIND_BY_PREFIX.length; i++) {
            if (id.indexOf(KIND_BY_PREFIX[i][0]) === 0) return KIND_BY_PREFIX[i][1];
        }
        return 'task';
    }

    function infoFor(kind) {
        return KIND_INFO[kind] || KIND_INFO.task;
    }

    // "Input Column: PRPS" -> "PRPS"; the kind is already carried by colour+icon.
    function shortLabel(name, id) {
        if (!name) return id || '';
        const idx = name.indexOf(': ');
        return idx > -1 ? name.slice(idx + 2) : name;
    }

    // ---------------------------------------------------------------------
    // Text measurement (canvas in the browser, estimate elsewhere)
    // ---------------------------------------------------------------------

    const measure = (function () {
        let ctx = null;
        if (typeof document !== 'undefined' && document.createElement) {
            try {
                ctx = document.createElement('canvas').getContext('2d');
            } catch (e) {
                ctx = null;
            }
        }
        return function (text, font) {
            if (!text) return 0;
            if (ctx) {
                ctx.font = font;
                return ctx.measureText(text).width;
            }
            const size = parseFloat(font) || 12;
            return text.length * size * 0.56;
        };
    })();

    function ellipsize(text, font, maxWidth) {
        if (!text) return '';
        if (measure(text, font) <= maxWidth) return text;
        let lo = 0;
        let hi = text.length;
        while (lo < hi) {
            const mid = Math.ceil((lo + hi) / 2);
            if (measure(text.slice(0, mid) + '…', font) <= maxWidth) lo = mid;
            else hi = mid - 1;
        }
        return text.slice(0, lo) + '…';
    }

    // ---------------------------------------------------------------------
    // Model: lineage JSON -> graph of containers, items and flows
    // ---------------------------------------------------------------------

    class BPMNLineageModel {
        constructor(options) {
            this.options = Object.assign({
                containerWidth: 252,
                headerHeight: 38,
                rowHeight: 26,
                rowGap: 4,
                padding: 10,
                collapsedHeight: 72,
                rankGap: 180,
                containerGap: 34,
                barycenterSweeps: 8
            }, options || {});

            this.nodes = new Map();      // task id -> node
            this.containers = new Map(); // container id -> container
            this.edges = [];             // task-level flows
            this.ranks = [];             // rank index -> { containers: [], stage, x, width }
            this.diagnostics = { unresolvedFlows: 0, unknownRefs: [], crossings: 0 };
        }

        build(lineage) {
            this.nodes.clear();
            this.containers.clear();
            this.edges = [];
            this.diagnostics = { unresolvedFlows: 0, unknownRefs: [], crossings: 0 };

            const addTask = (task, bpmnType) => {
                const kind = kindOf(task.id);
                const info = infoFor(kind);
                const reference = task.entity_reference || task.enriched_attribute_reference || '';
                this.nodes.set(task.id, {
                    id: task.id,
                    name: task.name || task.id,
                    label: shortLabel(task.name, task.id),
                    kind: kind,
                    role: info.role,
                    bpmn: bpmnType || info.bpmn,
                    palette: PALETTE[info.palette] || PALETTE.default,
                    description: task.description || '',
                    reference: reference,
                    containerId: null,
                    outgoing: [],
                    incoming: []
                });
            };

            (lineage.user_tasks || []).forEach(t => addTask(t, 'UserTask'));
            (lineage.service_tasks || []).forEach(t => addTask(t, 'ServiceTask'));

            this._assignContainers();
            this._buildEdges(lineage.sequence_flows || []);
            this._assignRanks();

            return this;
        }

        /**
         * A column task belongs to the table task whose reference is its own
         * reference minus the last segment, e.g.
         *   INSTRMNT_RL.PRPS                      -> INSTRMNT_RL
         *   F_05_01.Finance leases.TYP_CLLTRL     -> F_05_01.Finance leases
         *   F_05_01_152589_REF.MTRC               -> F_05_01_152589_REF
         * Tasks whose kind is a table are always containers in their own right,
         * so a join table never gets swallowed by the output table it feeds.
         */
        _assignContainers() {
            const byReference = new Map();
            this.nodes.forEach(node => {
                if (node.reference && !byReference.has(node.reference)) {
                    byReference.set(node.reference, node.id);
                }
            });

            this.nodes.forEach(node => {
                if (node.role === 'container') return;
                const parts = node.reference ? node.reference.split('.') : [];
                let parentId = null;
                if (parts.length > 1) {
                    parentId = byReference.get(parts.slice(0, -1).join('.')) || null;
                }
                if (!parentId) {
                    // Fall back to the longest container id that prefixes this id.
                    this.nodes.forEach(other => {
                        if (other.role !== 'container' || other.id === node.id) return;
                        if (node.id.indexOf(other.id) === 0 && (!parentId || other.id.length > parentId.length)) {
                            parentId = other.id;
                        }
                    });
                }
                node.containerId = parentId && this.nodes.has(parentId) ? parentId : null;
            });

            // Containers: every container node, plus a synthetic one for any
            // orphan item so nothing is dropped from the diagram.
            this.nodes.forEach(node => {
                if (node.role === 'container') {
                    this.containers.set(node.id, this._newContainer(node.id, node));
                }
            });
            this.nodes.forEach(node => {
                if (node.role === 'container') return;
                if (!node.containerId) {
                    node.role = 'container';
                    this.containers.set(node.id, this._newContainer(node.id, node));
                    return;
                }
                this.containers.get(node.containerId).children.push(node);
            });

            this.containers.forEach(container => {
                container.children.sort((a, b) => a.label.localeCompare(b.label));
                container.children.forEach((child, i) => { child.childIndex = i; });
            });
        }

        _newContainer(id, node) {
            return {
                id: id,
                node: node,
                kind: node.kind,
                palette: node.palette,
                label: shortLabel(node.name, node.id),
                children: [],
                expanded: false,
                rank: 0,
                order: 0,
                x: 0, y: 0, width: 0, height: 0
            };
        }

        /**
         * Flow endpoints are plain task ids. Older payloads (and the graph API)
         * prefix them with user_task_/service_task_, and the stored source_type
         * is the base model name, so accept both spellings.
         */
        _resolveRef(ref) {
            if (!ref) return null;
            if (this.nodes.has(ref)) return ref;
            for (const prefix of ['user_task_', 'service_task_']) {
                if (ref.indexOf(prefix) === 0 && this.nodes.has(ref.slice(prefix.length))) {
                    return ref.slice(prefix.length);
                }
                if (this.nodes.has(prefix + ref)) return prefix + ref;
            }
            return null;
        }

        _buildEdges(flows) {
            const unknown = new Set();
            flows.forEach(flow => {
                const source = this._resolveRef(flow.source_ref);
                const target = this._resolveRef(flow.target_ref);
                if (!source || !target) {
                    this.diagnostics.unresolvedFlows++;
                    if (!source && flow.source_ref) unknown.add(flow.source_ref);
                    if (!target && flow.target_ref) unknown.add(flow.target_ref);
                    return;
                }
                const edge = {
                    id: flow.id,
                    source: source,
                    target: target,
                    name: flow.name || '',
                    description: flow.description || ''
                };
                this.edges.push(edge);
                this.nodes.get(source).outgoing.push(edge);
                this.nodes.get(target).incoming.push(edge);
            });
            this.diagnostics.unknownRefs = Array.from(unknown);
        }

        containerOf(nodeId) {
            const node = this.nodes.get(nodeId);
            if (!node) return null;
            return node.role === 'container' ? node.id : node.containerId;
        }

        /** Container-level flow graph, used for ranking and ordering. */
        containerEdges() {
            const merged = new Map();
            this.edges.forEach(edge => {
                const s = this.containerOf(edge.source);
                const t = this.containerOf(edge.target);
                if (!s || !t || s === t) return;
                const key = s + ' ' + t;
                const found = merged.get(key);
                if (found) found.weight++;
                else merged.set(key, { source: s, target: t, weight: 1 });
            });
            return Array.from(merged.values());
        }

        /**
         * Rank = distance along the pipeline. Seeded from the known task kinds
         * (input -> join -> output -> datapoint) and then relaxed along the flow
         * graph so unknown kinds, and any longer chain than the seed implies,
         * still land in the right column.
         */
        _assignRanks() {
            const edges = this.containerEdges();
            const rank = new Map();
            const indegree = new Map();
            const adjacency = new Map();

            this.containers.forEach(c => {
                const seed = infoFor(c.kind).rank;
                rank.set(c.id, typeof seed === 'number' ? seed : 0);
                indegree.set(c.id, 0);
                adjacency.set(c.id, []);
            });
            edges.forEach(e => {
                adjacency.get(e.source).push(e.target);
                indegree.set(e.target, indegree.get(e.target) + 1);
            });

            const queue = [];
            indegree.forEach((deg, id) => { if (deg === 0) queue.push(id); });
            let visited = 0;
            while (queue.length) {
                const id = queue.shift();
                visited++;
                adjacency.get(id).forEach(next => {
                    rank.set(next, Math.max(rank.get(next), rank.get(id) + 1));
                    indegree.set(next, indegree.get(next) - 1);
                    if (indegree.get(next) === 0) queue.push(next);
                });
            }
            if (visited < this.containers.size) {
                // Cyclic input: leave the remaining containers on their seed rank.
                console.warn('BPMN lineage: flow graph contains a cycle, using seed ranks for the rest');
            }

            const maxRank = Math.max(0, ...Array.from(rank.values()));
            this.ranks = [];
            for (let r = 0; r <= maxRank; r++) this.ranks.push({ index: r, containers: [], stage: '', x: 0, width: 0 });
            this.containers.forEach(c => {
                c.rank = rank.get(c.id);
                this.ranks[c.rank].containers.push(c);
            });

            this.ranks.forEach(r => {
                r.containers.sort((a, b) => a.label.localeCompare(b.label));
                const counts = new Map();
                r.containers.forEach(c => {
                    const stage = infoFor(c.kind).stage;
                    if (stage) counts.set(stage, (counts.get(stage) || 0) + 1);
                });
                let best = '';
                let bestCount = 0;
                counts.forEach((count, stage) => { if (count > bestCount) { best = stage; bestCount = count; } });
                r.stage = best || ('Stage ' + (r.index + 1));
            });
        }

        // -- expansion state ------------------------------------------------

        setExpanded(containerId, expanded) {
            const container = this.containers.get(containerId);
            if (!container || !container.children.length) return false;
            container.expanded = expanded;
            return true;
        }

        setAllExpanded(expanded) {
            this.containers.forEach(c => { if (c.children.length) c.expanded = expanded; });
        }

        /** Where a task is drawn right now: its own row, or its collapsed container. */
        visibleAnchor(nodeId) {
            const node = this.nodes.get(nodeId);
            if (!node) return null;
            if (node.role === 'container') return node.id;
            const container = this.containers.get(node.containerId);
            return container && container.expanded ? node.id : container.id;
        }

        /** Flows between whatever is currently visible, merged and weighted. */
        visibleEdges() {
            const merged = new Map();
            this.edges.forEach(edge => {
                const s = this.visibleAnchor(edge.source);
                const t = this.visibleAnchor(edge.target);
                if (!s || !t || s === t) return;
                const key = s + ' ' + t;
                const found = merged.get(key);
                if (found) {
                    found.weight++;
                    found.flows.push(edge);
                } else {
                    merged.set(key, { source: s, target: t, weight: 1, flows: [edge] });
                }
            });
            return Array.from(merged.values());
        }

        // -- layout ---------------------------------------------------------

        layout() {
            const opt = this.options;

            if (!this.containers.size) {
                this.bounds = { x: 0, y: 0, width: 0, height: 0 };
                this.laidOutEdges = [];
                return { containers: [], edges: [], bounds: this.bounds };
            }

            this.containers.forEach(c => {
                c.width = opt.containerWidth;
                if (c.expanded && c.children.length) {
                    const rows = c.children.length;
                    c.height = opt.headerHeight + opt.padding * 2 +
                        rows * opt.rowHeight + (rows - 1) * opt.rowGap;
                } else {
                    c.height = opt.collapsedHeight;
                }
            });

            this._orderContainers();
            this._orderChildren();
            this._placeVertically();

            let x = 0;
            this.ranks.forEach(rank => {
                rank.width = Math.max(opt.containerWidth, ...rank.containers.map(c => c.width));
                rank.x = x;
                rank.containers.forEach(c => { c.x = x; });
                x += rank.width + opt.rankGap;
            });

            this._placeChildren();
            const edges = this._routeEdges();
            this.diagnostics.crossings = this._countCrossings(edges);

            const boxes = [];
            this.containers.forEach(c => boxes.push(c));
            const minY = Math.min(...boxes.map(c => c.y));
            const maxY = Math.max(...boxes.map(c => c.y + c.height));
            const maxX = Math.max(...boxes.map(c => c.x + c.width));

            this.bounds = { x: 0, y: minY, width: maxX, height: maxY - minY };
            this.laidOutEdges = edges;
            return { containers: Array.from(this.containers.values()), edges: edges, bounds: this.bounds };
        }

        _orderContainers() {
            const edges = this.containerEdges();
            const successors = new Map();
            const predecessors = new Map();
            this.containers.forEach(c => { successors.set(c.id, []); predecessors.set(c.id, []); });
            edges.forEach(e => {
                successors.get(e.source).push(e);
                predecessors.get(e.target).push(e);
            });

            const positionOf = new Map();
            this.ranks.forEach(rank => rank.containers.forEach((c, i) => positionOf.set(c.id, i)));

            const sweep = (rank, neighbours, endpoint) => {
                const scored = rank.containers.map((c, i) => {
                    const links = neighbours.get(c.id);
                    let sum = 0;
                    let weight = 0;
                    links.forEach(link => {
                        const pos = positionOf.get(link[endpoint]);
                        if (pos === undefined) return;
                        sum += pos * link.weight;
                        weight += link.weight;
                    });
                    return { container: c, key: weight ? sum / weight : i, tie: i };
                });
                scored.sort((a, b) => (a.key - b.key) || (a.tie - b.tie));
                rank.containers = scored.map(s => s.container);
                rank.containers.forEach((c, i) => positionOf.set(c.id, i));
            };

            for (let pass = 0; pass < this.options.barycenterSweeps; pass++) {
                if (pass % 2 === 0) {
                    for (let r = 1; r < this.ranks.length; r++) sweep(this.ranks[r], predecessors, 'source');
                } else {
                    for (let r = this.ranks.length - 2; r >= 0; r--) sweep(this.ranks[r], successors, 'target');
                }
            }
            this.ranks.forEach(rank => rank.containers.forEach((c, i) => { c.order = i; }));
        }

        /**
         * Columns with the same name recur across join tables, so alphabetical
         * rows usually give near-parallel flows. Barycenter ordering wins on
         * less regular data - run both and keep whichever crosses less.
         */
        _orderChildren() {
            const expanded = [];
            this.containers.forEach(c => { if (c.expanded && c.children.length > 1) expanded.push(c); });
            if (!expanded.length) return;

            const alphabetical = expanded.map(c => c.children.slice().sort((a, b) => a.label.localeCompare(b.label)));
            this._applyChildOrder(expanded, alphabetical);
            const alphabeticalCrossings = this._estimateCrossings();

            const byBarycenter = expanded.map(c => {
                const scored = c.children.map((child, i) => {
                    let sum = 0;
                    let count = 0;
                    child.incoming.concat(child.outgoing).forEach(edge => {
                        const otherId = edge.source === child.id ? edge.target : edge.source;
                        const anchor = this.visibleAnchor(otherId);
                        if (!anchor) return;
                        const other = this.nodes.get(anchor);
                        const container = this.containers.get(this.containerOf(anchor));
                        if (!container) return;
                        sum += container.order + (other && other.childIndex !== undefined ? other.childIndex / 1000 : 0);
                        count++;
                    });
                    return { child: child, key: count ? sum / count : i, tie: i };
                });
                scored.sort((a, b) => (a.key - b.key) || (a.tie - b.tie));
                return scored.map(s => s.child);
            });
            this._applyChildOrder(expanded, byBarycenter);
            const barycenterCrossings = this._estimateCrossings();

            if (alphabeticalCrossings <= barycenterCrossings) {
                this._applyChildOrder(expanded, alphabetical);
            }
        }

        _applyChildOrder(containers, orders) {
            containers.forEach((c, i) => {
                c.children = orders[i];
                c.children.forEach((child, index) => { child.childIndex = index; });
            });
        }

        /** Crossing count from ordering alone, before coordinates exist. */
        _estimateCrossings() {
            const slot = id => {
                const anchor = this.visibleAnchor(id);
                const container = this.containers.get(this.containerOf(anchor));
                if (!container) return 0;
                const node = this.nodes.get(anchor);
                const child = node && node.childIndex !== undefined && container.expanded ? node.childIndex : 0;
                return container.order * 1000 + child;
            };
            const byGutter = new Map();
            this.visibleEdges().forEach(edge => {
                const sourceRank = this.containers.get(this.containerOf(edge.source)).rank;
                if (!byGutter.has(sourceRank)) byGutter.set(sourceRank, []);
                byGutter.get(sourceRank).push([slot(edge.source), slot(edge.target), edge.weight]);
            });
            let crossings = 0;
            byGutter.forEach(list => {
                for (let i = 0; i < list.length; i++) {
                    for (let j = i + 1; j < list.length; j++) {
                        const a = list[i];
                        const b = list[j];
                        if ((a[0] - b[0]) * (a[1] - b[1]) < 0) crossings += a[2] * b[2];
                    }
                }
            });
            return crossings;
        }

        _placeVertically() {
            const gap = this.options.containerGap;

            const stack = rank => {
                let y = 0;
                rank.containers.forEach(c => { c.y = y; y += c.height + gap; });
                const height = Math.max(0, y - gap);
                const offset = -height / 2;
                rank.containers.forEach(c => { c.y += offset; });
            };
            this.ranks.forEach(stack);

            // Pull each container towards the average of its neighbours, then
            // re-separate so the rank keeps its order and minimum gaps.
            const edges = this.containerEdges();
            const successors = new Map();
            const predecessors = new Map();
            this.containers.forEach(c => { successors.set(c.id, []); predecessors.set(c.id, []); });
            edges.forEach(e => {
                successors.get(e.source).push(e);
                predecessors.get(e.target).push(e);
            });

            const align = (rank, neighbours, endpoint) => {
                rank.containers.forEach(c => {
                    const links = neighbours.get(c.id);
                    let sum = 0;
                    let weight = 0;
                    links.forEach(link => {
                        const other = this.containers.get(link[endpoint]);
                        if (!other) return;
                        sum += (other.y + other.height / 2) * link.weight;
                        weight += link.weight;
                    });
                    if (weight) c.y = sum / weight - c.height / 2;
                });
                this._separate(rank.containers, gap);
            };

            for (let pass = 0; pass < 3; pass++) {
                for (let r = 1; r < this.ranks.length; r++) align(this.ranks[r], predecessors, 'source');
                for (let r = this.ranks.length - 2; r >= 0; r--) align(this.ranks[r], successors, 'target');
            }
        }

        /** Push overlapping boxes apart while preserving their order. */
        _separate(containers, gap) {
            if (!containers.length) return;
            const centre = containers.reduce((sum, c) => sum + c.y + c.height / 2, 0) / containers.length;
            for (let i = 1; i < containers.length; i++) {
                const previous = containers[i - 1];
                const minimum = previous.y + previous.height + gap;
                if (containers[i].y < minimum) containers[i].y = minimum;
            }
            for (let i = containers.length - 2; i >= 0; i--) {
                const next = containers[i + 1];
                const maximum = next.y - gap - containers[i].height;
                if (containers[i].y > maximum) containers[i].y = maximum;
            }
            const newCentre = containers.reduce((sum, c) => sum + c.y + c.height / 2, 0) / containers.length;
            const shift = centre - newCentre;
            containers.forEach(c => { c.y += shift; });
        }

        _placeChildren() {
            const opt = this.options;
            this.containers.forEach(c => {
                if (!c.expanded) {
                    c.children.forEach(child => {
                        child.x = c.x;
                        child.y = c.y + c.height / 2;
                        child.width = 0;
                        child.height = 0;
                    });
                    return;
                }
                let y = c.y + opt.headerHeight + opt.padding;
                c.children.forEach(child => {
                    child.x = c.x + opt.padding;
                    child.y = y;
                    child.width = c.width - opt.padding * 2;
                    child.height = opt.rowHeight;
                    y += opt.rowHeight + opt.rowGap;
                });
            });
        }

        _routeEdges() {
            return this.visibleEdges().map(edge => {
                const source = this._anchorGeometry(edge.source, 'out');
                const target = this._anchorGeometry(edge.target, 'in');
                const dx = Math.max(40, Math.abs(target.x - source.x) * 0.45);
                return {
                    source: edge.source,
                    target: edge.target,
                    weight: edge.weight,
                    flows: edge.flows,
                    x1: source.x, y1: source.y,
                    x2: target.x, y2: target.y,
                    path: 'M ' + source.x + ' ' + source.y +
                        ' C ' + (source.x + dx) + ' ' + source.y +
                        ', ' + (target.x - dx) + ' ' + target.y +
                        ', ' + target.x + ' ' + target.y
                };
            });
        }

        /**
         * Rows connect at their own height but on the container's edge, so a
         * flow never crosses the box it comes from.
         */
        _anchorGeometry(id, direction) {
            const node = this.nodes.get(id);
            const container = this.containers.get(this.containerOf(id));
            const isRow = node && node.role !== 'container' && container.expanded;
            const y = isRow ? node.y + node.height / 2 : container.y + container.height / 2;
            const x = direction === 'out' ? container.x + container.width : container.x;
            return { x: x, y: y };
        }

        _countCrossings(edges) {
            let crossings = 0;
            const byGutter = new Map();
            edges.forEach(edge => {
                const key = Math.round(edge.x1);
                if (!byGutter.has(key)) byGutter.set(key, []);
                byGutter.get(key).push(edge);
            });
            byGutter.forEach(list => {
                for (let i = 0; i < list.length; i++) {
                    for (let j = i + 1; j < list.length; j++) {
                        const a = list[i];
                        const b = list[j];
                        if ((a.y1 - b.y1) * (a.y2 - b.y2) < 0) crossings++;
                    }
                }
            });
            return crossings;
        }

        /** Everything upstream and downstream of a visible anchor. */
        connectedSet(anchorId) {
            const visible = this.visibleEdges();
            const forward = new Map();
            const backward = new Map();
            visible.forEach(edge => {
                if (!forward.has(edge.source)) forward.set(edge.source, []);
                if (!backward.has(edge.target)) backward.set(edge.target, []);
                forward.get(edge.source).push(edge.target);
                backward.get(edge.target).push(edge.source);
            });
            const found = new Set([anchorId]);
            const walk = (start, map) => {
                const queue = [start];
                while (queue.length) {
                    const current = queue.shift();
                    (map.get(current) || []).forEach(next => {
                        if (found.has(next)) return;
                        found.add(next);
                        queue.push(next);
                    });
                }
            };
            walk(anchorId, forward);
            walk(anchorId, backward);
            return found;
        }

        stats() {
            let items = 0;
            this.containers.forEach(c => { items += c.children.length; });
            return {
                containers: this.containers.size,
                tasks: this.nodes.size,
                items: items,
                flows: this.edges.length,
                ranks: this.ranks.length,
                unresolvedFlows: this.diagnostics.unresolvedFlows,
                crossings: this.diagnostics.crossings
            };
        }
    }

    // ---------------------------------------------------------------------
    // Viewer: SVG rendering, zoom/pan and interaction
    // ---------------------------------------------------------------------

    const SVG_NS = 'http://www.w3.org/2000/svg';
    const FONT_TITLE = 'bold 12px Arial, sans-serif';
    const FONT_ROW = '11px Arial, sans-serif';

    function svgEl(name, attributes) {
        const element = document.createElementNS(SVG_NS, name);
        if (attributes) {
            Object.keys(attributes).forEach(key => element.setAttribute(key, attributes[key]));
        }
        return element;
    }

    const DIAGRAM_STYLE = [
        '.bpmn-container { cursor: pointer; }',
        '.bpmn-container-box { fill: #ffffff; stroke-width: 1.5; }',
        '.bpmn-container-title { font: ' + FONT_TITLE + '; }',
        '.bpmn-container-meta { font: 10px Arial, sans-serif; fill: #6b7780; }',
        '.bpmn-row rect { stroke-width: 1; }',
        '.bpmn-row text { font: ' + FONT_ROW + '; }',
        '.bpmn-flow { fill: none; stroke: #7c8b95; stroke-linecap: round; }',
        '.bpmn-flow-weight { font: 9px Arial, sans-serif; fill: #6b7780; }',
        '.bpmn-stage-label { font: bold 11px Arial, sans-serif; fill: #55636d; letter-spacing: .06em; }',
        '.bpmn-stage-band { fill: #000; opacity: .022; }',
        '.dimmed { opacity: .12; }',
        '.match rect.bpmn-container-box, .match > rect { stroke: #f9a825; stroke-width: 2.5; }'
    ].join('\n');

    class BPMNLineageViewer {
        constructor(containerId, options) {
            options = options || {};
            this.container = typeof containerId === 'string'
                ? document.getElementById(containerId)
                : containerId;
            if (!this.container) throw new Error('BPMNLineageViewer: container not found');

            this.options = Object.assign({
                height: options.height || 720,
                minZoom: 0.08,
                maxZoom: 3,
                showStages: true,
                showFlowWeights: true,
                // Legacy option names still accepted from existing callers.
                containerWidth: options.nodeWidth || 252,
                rankGap: options.levelGap || 180,
                containerGap: options.nodeGap || 34
            }, options);

            this.model = new BPMNLineageModel({
                containerWidth: this.options.containerWidth,
                rankGap: this.options.rankGap,
                containerGap: this.options.containerGap
            });

            this.transform = { x: 0, y: 0, k: 1 };
            this.focusId = null;
            this.searchTerm = '';
            this.lineage = null;
            this.onStateChange = options.onStateChange || null;

            this._initSVG();
            this._bindInteractions();
        }

        // -- setup ----------------------------------------------------------

        _initSVG() {
            this.container.innerHTML = '';
            this.container.style.position = 'relative';
            this.container.style.overflow = 'hidden';
            if (!this.container.style.height) this.container.style.height = this.options.height + 'px';

            this.svg = svgEl('svg', {
                xmlns: SVG_NS,
                width: '100%',
                height: '100%',
                'font-family': 'Arial, sans-serif'
            });
            this.svg.style.display = 'block';
            this.svg.style.background = '#fbfcfd';
            this.svg.style.cursor = 'grab';
            this.svg.style.touchAction = 'none';

            const defs = svgEl('defs');
            const style = svgEl('style');
            style.textContent = DIAGRAM_STYLE;
            defs.appendChild(style);
            defs.appendChild(this._arrowMarker('bpmn-arrow', '#7c8b95'));
            defs.appendChild(this._arrowMarker('bpmn-arrow-active', '#1a73e8'));
            this.svg.appendChild(defs);

            this.stageLayer = svgEl('g', { class: 'bpmn-stages' });
            this.edgeLayer = svgEl('g', { class: 'bpmn-flows' });
            this.nodeLayer = svgEl('g', { class: 'bpmn-nodes' });

            this.viewport = svgEl('g', { class: 'bpmn-viewport' });
            this.viewport.appendChild(this.stageLayer);
            this.viewport.appendChild(this.edgeLayer);
            this.viewport.appendChild(this.nodeLayer);
            this.svg.appendChild(this.viewport);
            this.container.appendChild(this.svg);

            if (typeof ResizeObserver !== 'undefined') {
                this._resizeObserver = new ResizeObserver(() => this._syncViewBox());
                this._resizeObserver.observe(this.container);
            }
            this._syncViewBox();
        }

        _arrowMarker(id, colour) {
            const marker = svgEl('marker', {
                id: id,
                markerWidth: 9,
                markerHeight: 7,
                refX: 8.5,
                refY: 3.5,
                orient: 'auto',
                markerUnits: 'userSpaceOnUse'
            });
            marker.appendChild(svgEl('polygon', { points: '0 0, 9 3.5, 0 7', fill: colour }));
            return marker;
        }

        _syncViewBox() {
            const width = this.container.clientWidth || 1200;
            const height = this.container.clientHeight || this.options.height;
            this.viewSize = { width: width, height: height };
            this.svg.setAttribute('viewBox', '0 0 ' + width + ' ' + height);
        }

        _bindInteractions() {
            let dragging = false;
            let start = null;

            this.svg.addEventListener('mousedown', event => {
                if (event.button !== 0) return;
                dragging = true;
                start = { x: event.clientX, y: event.clientY, tx: this.transform.x, ty: this.transform.y };
                this.svg.style.cursor = 'grabbing';
            });
            window.addEventListener('mousemove', event => {
                if (!dragging) return;
                this.transform.x = start.tx + (event.clientX - start.x);
                this.transform.y = start.ty + (event.clientY - start.y);
                this._applyTransform();
            });
            window.addEventListener('mouseup', () => {
                dragging = false;
                this.svg.style.cursor = 'grab';
            });

            this.svg.addEventListener('wheel', event => {
                event.preventDefault();
                const rect = this.svg.getBoundingClientRect();
                const factor = Math.exp(-event.deltaY * 0.0015);
                this.zoomAt(event.clientX - rect.left, event.clientY - rect.top, factor);
            }, { passive: false });

            this.svg.addEventListener('click', event => {
                if (event.target === this.svg || event.target.classList.contains('bpmn-stage-band')) {
                    this.clearFocus();
                }
            });
            this.svg.addEventListener('dblclick', event => {
                if (event.target === this.svg) this.fit();
            });
        }

        // -- data -----------------------------------------------------------

        async loadData(datapointId) {
            try {
                const response = await fetch('/pybirdai/datapoint/' + encodeURIComponent(datapointId) + '/bpmn_metadata_lineage/process/');
                const data = await response.json();
                if (data.success && data.lineage) {
                    this.renderLineage(data.lineage);
                    return data.lineage;
                }
                throw new Error(data.error || 'Failed to load lineage data');
            } catch (error) {
                console.error('Error loading BPMN lineage data:', error);
                this.showError(error.message);
                throw error;
            }
        }

        renderLineage(lineage) {
            this.lineage = lineage;
            this.focusId = null;
            this.model.build(lineage);
            this.draw();
            this.fit();
            return this.model.stats();
        }

        // -- drawing --------------------------------------------------------

        draw() {
            const layout = this.model.layout();
            this.stageLayer.innerHTML = '';
            this.edgeLayer.innerHTML = '';
            this.nodeLayer.innerHTML = '';

            if (!layout.containers.length) {
                this._drawEmptyState();
                if (this.onStateChange) this.onStateChange(this.model.stats());
                return;
            }

            if (this.options.showStages) this._drawStages(layout.bounds);
            layout.edges.forEach(edge => this._drawEdge(edge));
            layout.containers.forEach(container => this._drawContainer(container));

            this._applyHighlight();
            if (this.onStateChange) this.onStateChange(this.model.stats());
        }

        _drawEmptyState() {
            this._syncViewBox();
            this.transform = { x: 0, y: 0, k: 1 };
            this._applyTransform();
            const text = svgEl('text', {
                x: this.viewSize.width / 2,
                y: this.viewSize.height / 2,
                'text-anchor': 'middle',
                fill: '#8a97a0',
                'font-size': 14
            });
            text.textContent = 'No BPMN lineage yet — run "Process BPMN Lineage" to generate the workflow.';
            this.nodeLayer.appendChild(text);
        }

        _drawStages(bounds) {
            const top = bounds.y - 46;
            this.model.ranks.forEach((rank, index) => {
                if (index % 2 === 1) {
                    this.stageLayer.appendChild(svgEl('rect', {
                        class: 'bpmn-stage-band',
                        x: rank.x - 18,
                        y: top - 8,
                        width: rank.width + 36,
                        height: bounds.height + 62
                    }));
                }
                const label = svgEl('text', {
                    class: 'bpmn-stage-label',
                    x: rank.x,
                    y: top
                });
                label.textContent = rank.stage.toUpperCase();
                this.stageLayer.appendChild(label);
            });
        }

        _drawEdge(edge) {
            const group = svgEl('g', { class: 'bpmn-flow-group' });
            group.dataset.source = edge.source;
            group.dataset.target = edge.target;

            const path = svgEl('path', {
                class: 'bpmn-flow',
                d: edge.path,
                'stroke-width': Math.min(4, 1 + Math.log2(edge.weight + 1) * 0.7),
                'stroke-opacity': 0.75,
                'marker-end': 'url(#bpmn-arrow)'
            });
            group.appendChild(path);

            if (this.options.showFlowWeights && edge.weight > 1) {
                const midX = (edge.x1 + edge.x2) / 2;
                const midY = (edge.y1 + edge.y2) / 2;
                const text = svgEl('text', { class: 'bpmn-flow-weight', x: midX, y: midY - 4, 'text-anchor': 'middle' });
                text.textContent = edge.weight + ' flows';
                group.appendChild(text);
            }

            this._attachTooltip(group, () => this._edgeTooltip(edge));
            this.edgeLayer.appendChild(group);
        }

        _drawContainer(container) {
            const palette = container.palette;
            const group = svgEl('g', { class: 'bpmn-container', transform: 'translate(' + container.x + ',' + container.y + ')' });
            group.dataset.id = container.id;

            const box = svgEl('rect', {
                class: 'bpmn-container-box',
                width: container.width,
                height: container.height,
                rx: 10,
                ry: 10,
                stroke: palette.stroke
            });
            group.appendChild(box);

            const bandHeight = container.expanded ? this.model.options.headerHeight : container.height;
            const band = svgEl('path', {
                d: this._bandPath(container.width, bandHeight, container.expanded ? 10 : 10, container.expanded),
                fill: palette.fill
            });
            group.appendChild(band);

            const icon = svgEl('g', { transform: 'translate(10,' + (container.expanded ? 10 : 12) + ')' });
            this._drawMarker(icon, container.node.bpmn, palette.stroke);
            group.appendChild(icon);

            const titleY = container.expanded ? 24 : 26;
            const maxTitleWidth = container.width - 60;
            const title = svgEl('text', { class: 'bpmn-container-title', x: 32, y: titleY, fill: palette.text });
            title.textContent = ellipsize(container.label, FONT_TITLE, maxTitleWidth);
            group.appendChild(title);

            const count = container.children.length;
            const columns = count + (count === 1 ? ' column' : ' columns');
            if (count) {
                const meta = svgEl('text', {
                    class: 'bpmn-container-meta',
                    x: container.expanded ? container.width - 12 : 32,
                    y: container.expanded ? 24 : 44,
                    'text-anchor': container.expanded ? 'end' : 'start'
                });
                meta.textContent = container.expanded ? columns : columns + ' · click to expand';
                group.appendChild(meta);
            } else if (!container.expanded) {
                const meta = svgEl('text', { class: 'bpmn-container-meta', x: 32, y: 44 });
                meta.textContent = container.node.bpmn === 'EndEvent' ? 'final result' : 'no column lineage';
                group.appendChild(meta);
            }

            container.children.forEach(child => {
                if (!container.expanded) return;
                group.appendChild(this._drawRow(container, child));
            });

            group.addEventListener('click', event => {
                event.stopPropagation();
                if (event.shiftKey || !container.children.length) {
                    this.focus(container.id);
                } else {
                    this.toggle(container.id);
                }
            });
            this._attachTooltip(group, () => this._nodeTooltip(container.node, container));

            this.nodeLayer.appendChild(group);
        }

        /** Header band that keeps the container's rounded top corners. */
        _bandPath(width, height, radius, squareBottom) {
            if (!squareBottom) {
                return 'M 0 ' + radius +
                    ' A ' + radius + ' ' + radius + ' 0 0 1 ' + radius + ' 0' +
                    ' H ' + (width - radius) +
                    ' A ' + radius + ' ' + radius + ' 0 0 1 ' + width + ' ' + radius +
                    ' V ' + (height - radius) +
                    ' A ' + radius + ' ' + radius + ' 0 0 1 ' + (width - radius) + ' ' + height +
                    ' H ' + radius +
                    ' A ' + radius + ' ' + radius + ' 0 0 1 0 ' + (height - radius) + ' Z';
            }
            return 'M 0 ' + radius +
                ' A ' + radius + ' ' + radius + ' 0 0 1 ' + radius + ' 0' +
                ' H ' + (width - radius) +
                ' A ' + radius + ' ' + radius + ' 0 0 1 ' + width + ' ' + radius +
                ' V ' + height + ' H 0 Z';
        }

        _drawRow(container, child) {
            const palette = child.palette;
            const row = svgEl('g', { class: 'bpmn-row' });
            row.dataset.id = child.id;
            const localX = child.x - container.x;
            const localY = child.y - container.y;

            row.appendChild(svgEl('rect', {
                x: localX,
                y: localY,
                width: child.width,
                height: child.height,
                rx: 5,
                ry: 5,
                fill: palette.fill,
                stroke: palette.stroke,
                'stroke-opacity': 0.55
            }));

            const marker = svgEl('g', { transform: 'translate(' + (localX + 5) + ',' + (localY + 5) + ') scale(0.75)' });
            this._drawMarker(marker, child.bpmn, palette.stroke);
            row.appendChild(marker);

            const text = svgEl('text', {
                x: localX + 24,
                y: localY + child.height / 2 + 4,
                fill: palette.text
            });
            text.textContent = ellipsize(child.label, FONT_ROW, child.width - 32);
            row.appendChild(text);

            row.addEventListener('click', event => {
                event.stopPropagation();
                this.focus(child.id);
            });
            this._attachTooltip(row, () => this._nodeTooltip(child, container));
            return row;
        }

        /** BPMN task-type markers. */
        _drawMarker(group, type, colour) {
            if (type === 'UserTask') {
                group.appendChild(svgEl('circle', {
                    cx: 7, cy: 5, r: 3, fill: 'none', stroke: colour, 'stroke-width': 1.4
                }));
                group.appendChild(svgEl('path', {
                    d: 'M 1.5 15 Q 1.5 9.5 7 9.5 Q 12.5 9.5 12.5 15',
                    fill: 'none', stroke: colour, 'stroke-width': 1.4
                }));
                return;
            }
            if (type === 'EndEvent') {
                group.appendChild(svgEl('circle', {
                    cx: 7, cy: 8, r: 6.5, fill: 'none', stroke: colour, 'stroke-width': 2.6
                }));
                return;
            }
            // ServiceTask: gear
            const teeth = 8;
            let d = '';
            for (let i = 0; i < teeth; i++) {
                const angle = (i / teeth) * Math.PI * 2;
                const next = ((i + 0.5) / teeth) * Math.PI * 2;
                const outer = 7;
                const inner = 5;
                d += (i === 0 ? 'M ' : 'L ') + (7 + Math.cos(angle) * outer).toFixed(2) + ' ' + (8 + Math.sin(angle) * outer).toFixed(2);
                d += ' L ' + (7 + Math.cos(next) * inner).toFixed(2) + ' ' + (8 + Math.sin(next) * inner).toFixed(2);
            }
            d += ' Z';
            group.appendChild(svgEl('path', { d: d, fill: 'none', stroke: colour, 'stroke-width': 1.2 }));
            group.appendChild(svgEl('circle', { cx: 7, cy: 8, r: 2.2, fill: 'none', stroke: colour, 'stroke-width': 1.2 }));
        }

        // -- interaction ----------------------------------------------------

        toggle(containerId) {
            const container = this.model.containers.get(containerId);
            if (!container) return;
            this.model.setExpanded(containerId, !container.expanded);
            this.draw();
        }

        expandAll() {
            this.model.setAllExpanded(true);
            this.draw();
            this.fit();
        }

        collapseAll() {
            this.model.setAllExpanded(false);
            this.draw();
            this.fit();
        }

        focus(anchorId) {
            this.focusId = this.focusId === anchorId ? null : anchorId;
            this._applyHighlight();
        }

        clearFocus() {
            if (!this.focusId && !this.searchTerm) return;
            this.focusId = null;
            this._applyHighlight();
        }

        search(term) {
            this.searchTerm = (term || '').trim().toLowerCase();
            if (this.searchTerm) {
                // Reveal matching columns so a hit is never hidden in a collapsed box.
                let changed = false;
                this.model.containers.forEach(container => {
                    const hit = container.children.some(child => this._matches(child));
                    if (hit && !container.expanded) {
                        container.expanded = true;
                        changed = true;
                    }
                });
                if (changed) this.draw();
            }
            this._applyHighlight();
            return this._matchCount();
        }

        _matches(node) {
            if (!this.searchTerm) return false;
            return (node.name || '').toLowerCase().indexOf(this.searchTerm) > -1 ||
                (node.reference || '').toLowerCase().indexOf(this.searchTerm) > -1 ||
                node.id.toLowerCase().indexOf(this.searchTerm) > -1;
        }

        _matchCount() {
            let count = 0;
            this.model.nodes.forEach(node => { if (this._matches(node)) count++; });
            return count;
        }

        _applyHighlight() {
            const connected = this.focusId ? this.model.connectedSet(this.focusId) : null;

            const setDim = (element, dimmed) => element.classList.toggle('dimmed', dimmed);

            this.nodeLayer.querySelectorAll('.bpmn-container').forEach(element => {
                const id = element.dataset.id;
                const container = this.model.containers.get(id);
                const inFocus = !connected || connected.has(id) ||
                    container.children.some(child => connected.has(child.id));
                setDim(element, !inFocus);
                element.classList.toggle('match', this._matches(container.node));

                element.querySelectorAll('.bpmn-row').forEach(row => {
                    const child = this.model.nodes.get(row.dataset.id);
                    const rowInFocus = !connected || connected.has(row.dataset.id) || connected.has(id);
                    setDim(row, !rowInFocus);
                    row.classList.toggle('match', this._matches(child));
                });
            });

            this.edgeLayer.querySelectorAll('.bpmn-flow-group').forEach(element => {
                const active = !connected ||
                    (connected.has(element.dataset.source) && connected.has(element.dataset.target));
                setDim(element, !active);
                const path = element.querySelector('path');
                path.setAttribute('stroke', active && connected ? '#1a73e8' : '#7c8b95');
                path.setAttribute('marker-end', active && connected ? 'url(#bpmn-arrow-active)' : 'url(#bpmn-arrow)');
            });
        }

        // -- viewport -------------------------------------------------------

        _applyTransform() {
            this.viewport.setAttribute('transform',
                'translate(' + this.transform.x + ',' + this.transform.y + ') scale(' + this.transform.k + ')');
        }

        zoomAt(screenX, screenY, factor) {
            const next = Math.min(this.options.maxZoom, Math.max(this.options.minZoom, this.transform.k * factor));
            const ratio = next / this.transform.k;
            this.transform.x = screenX - (screenX - this.transform.x) * ratio;
            this.transform.y = screenY - (screenY - this.transform.y) * ratio;
            this.transform.k = next;
            this._applyTransform();
        }

        zoomIn() {
            this._syncViewBox();
            this.zoomAt(this.viewSize.width / 2, this.viewSize.height / 2, 1.25);
        }

        zoomOut() {
            this._syncViewBox();
            this.zoomAt(this.viewSize.width / 2, this.viewSize.height / 2, 0.8);
        }

        fit(padding) {
            const bounds = this.model.bounds;
            if (!bounds || !bounds.width) return;
            this._syncViewBox();
            padding = padding === undefined ? 36 : padding;
            const width = this.viewSize.width - padding * 2;
            const height = this.viewSize.height - padding * 2;
            const stageRoom = this.options.showStages ? 56 : 0;
            const scale = Math.min(
                this.options.maxZoom,
                Math.max(this.options.minZoom,
                    Math.min(width / bounds.width, height / (bounds.height + stageRoom)))
            );
            this.transform.k = scale;
            this.transform.x = padding + (width - bounds.width * scale) / 2 - bounds.x * scale;
            this.transform.y = padding + (height - (bounds.height + stageRoom) * scale) / 2
                - (bounds.y - stageRoom) * scale;
            this._applyTransform();
        }

        resetView() {
            this.searchTerm = '';
            this.focusId = null;
            this.model.setAllExpanded(false);
            this.draw();
            this.fit();
        }

        // -- tooltip --------------------------------------------------------

        _attachTooltip(element, contentFactory) {
            element.addEventListener('mouseenter', event => this._showTooltip(event, contentFactory()));
            element.addEventListener('mousemove', event => this._moveTooltip(event));
            element.addEventListener('mouseleave', () => this.hideTooltip());
        }

        _nodeTooltip(node, container) {
            let html = '<strong>' + escapeHtml(node.name) + '</strong>';
            html += '<br><span style="opacity:.75">' + escapeHtml(node.bpmn) + '</span>';
            if (node.description) html += '<br><em>' + escapeHtml(node.description) + '</em>';
            if (node.reference) html += '<br><small>Reference: ' + escapeHtml(node.reference) + '</small>';
            if (container && container.node === node && container.children.length) {
                html += '<br><small>' + container.children.length + ' columns · click to ' +
                    (container.expanded ? 'collapse' : 'expand') + ', shift-click to trace</small>';
            } else {
                html += '<br><small>Click to trace upstream and downstream</small>';
            }
            return html;
        }

        _edgeTooltip(edge) {
            const first = edge.flows[0];
            let html = '<strong>' + escapeHtml(first.name || 'Sequence flow') + '</strong>';
            if (edge.weight > 1) html += '<br><small>' + edge.weight + ' sequence flows merged</small>';
            if (first.description) html += '<br><em>' + escapeHtml(first.description) + '</em>';
            return html;
        }

        _showTooltip(event, html) {
            this.hideTooltip();
            const tooltip = document.createElement('div');
            tooltip.id = 'bpmn-tooltip';
            tooltip.style.cssText = 'position:fixed;background:#263238;color:#fff;padding:8px 12px;' +
                'border-radius:4px;font:12px Arial, sans-serif;line-height:1.45;z-index:2000;max-width:340px;' +
                'pointer-events:none;box-shadow:0 4px 14px rgba(0,0,0,.25);';
            tooltip.innerHTML = html;
            document.body.appendChild(tooltip);
            this._moveTooltip(event);
        }

        _moveTooltip(event) {
            const tooltip = document.getElementById('bpmn-tooltip');
            if (!tooltip) return;
            const pad = 14;
            let left = event.clientX + pad;
            let top = event.clientY + pad;
            if (left + tooltip.offsetWidth > window.innerWidth - 8) left = event.clientX - tooltip.offsetWidth - pad;
            if (top + tooltip.offsetHeight > window.innerHeight - 8) top = event.clientY - tooltip.offsetHeight - pad;
            tooltip.style.left = left + 'px';
            tooltip.style.top = top + 'px';
        }

        hideTooltip() {
            const tooltip = document.getElementById('bpmn-tooltip');
            if (tooltip) tooltip.remove();
        }

        // -- misc -----------------------------------------------------------

        showError(message) {
            this.container.innerHTML = '<div style="text-align:center;padding:50px;color:#666;">' +
                '<h3>Error Loading BPMN Lineage</h3><p>' + escapeHtml(message) + '</p></div>';
        }

        stats() {
            return this.model.stats();
        }

        exportAsSVG(filename) {
            const bounds = this.model.bounds;
            if (!bounds) return;
            const margin = 40;
            const stageRoom = this.options.showStages ? 56 : 0;
            const width = bounds.width + margin * 2;
            const height = bounds.height + stageRoom + margin * 2;

            const svg = svgEl('svg', {
                xmlns: SVG_NS,
                'xmlns:xlink': 'http://www.w3.org/1999/xlink',
                width: Math.round(width),
                height: Math.round(height),
                viewBox: [bounds.x - margin, bounds.y - stageRoom - margin, width, height].join(' ')
            });
            svg.appendChild(this.svg.querySelector('defs').cloneNode(true));
            svg.appendChild(svgEl('rect', {
                x: bounds.x - margin,
                y: bounds.y - stageRoom - margin,
                width: width,
                height: height,
                fill: '#ffffff'
            }));
            const content = this.viewport.cloneNode(true);
            content.removeAttribute('transform');
            svg.appendChild(content);

            const source = '<?xml version="1.0" encoding="UTF-8"?>\n' + new XMLSerializer().serializeToString(svg);
            const url = URL.createObjectURL(new Blob([source], { type: 'image/svg+xml' }));
            const link = document.createElement('a');
            link.href = url;
            link.download = filename || 'bpmn-metadata-lineage.svg';
            link.click();
            URL.revokeObjectURL(url);
        }
    }

    function escapeHtml(value) {
        return String(value === undefined || value === null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    const api = { BPMNLineageViewer: BPMNLineageViewer, BPMNLineageModel: BPMNLineageModel };

    if (typeof module !== 'undefined' && module.exports) {
        module.exports = api;
        module.exports.BPMNLineageViewer = BPMNLineageViewer;
    }
    if (global) {
        global.BPMNLineageViewer = BPMNLineageViewer;
        global.BPMNLineageModel = BPMNLineageModel;
    }
})(typeof window !== 'undefined' ? window : (typeof globalThis !== 'undefined' ? globalThis : this));
