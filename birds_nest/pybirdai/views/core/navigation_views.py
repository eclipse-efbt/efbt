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
from django.conf import settings


def home_view(request):
    """Render the home page with optional admin credentials display."""
    context = {}

    # Only show credentials in DEBUG mode (development/codespace)
    if settings.DEBUG:
        context['admin_credentials'] = {
            'username': 'admin',
            'password': 'password',
            'email': 'a@b.com',
            'admin_url': '/admin/'
        }

    return render(request, 'pybirdai/home.html', context)


def automode_view(request):
    """Render the automode page."""
    return render(request, 'pybirdai/miscellaneous/automode.html')


def show_report(request, report_id):
    """Display a generic report."""
    return render(request, 'pybirdai/' + report_id)


def bird_diffs_and_corrections(request):
    """View function for displaying BIRD diffs and corrections page."""
    return render(request, 'pybirdai/reports/validation/bird_diffs_and_corrections.html')
