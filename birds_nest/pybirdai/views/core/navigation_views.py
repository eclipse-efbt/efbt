# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
"""
Navigation views for basic page rendering.
"""
from django.shortcuts import render


def home_view(request):
    """Render the home page."""
    return render(request, 'pybirdai/home.html')


def automode_view(request):
    """Render the automode page."""
    return render(request, 'pybirdai/automode.html')


def show_report(request, report_id):
    """Display a generic report."""
    return render(request, 'pybirdai/' + report_id)


def bird_diffs_and_corrections(request):
    """View function for displaying BIRD diffs and corrections page."""
    return render(request, 'pybirdai/bird_diffs_and_corrections.html')
