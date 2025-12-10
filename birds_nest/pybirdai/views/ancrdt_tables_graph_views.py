# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# SPDX-License-Identifier: EPL-2.0

"""
ANCRDT Tables Graph Views

Provides views for visualizing ANCRDT table relationships.
"""

from django.shortcuts import render


def ancrdt_tables_graph_viewer(request):
    """
    Main view for displaying ANCRDT tables relationship visualization.

    Shows an interactive Cytoscape.js graph with:
    - ANCRDT output cubes (blue nodes)
    - IL tables (green nodes)
    - Assignment/linking tables (orange nodes)
    - Join relationships as edges
    """
    context = {
        'page_title': 'ANCRDT Tables Relationship Graph',
        'description': 'Interactive visualization of ANCRDT output cubes and their joins to BIRD Input Layer tables.'
    }
    return render(request, 'pybirdai/ancrdt_tables_graph.html', context)
