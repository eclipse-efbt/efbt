# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
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
URL configuration for birds_nest project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from birds_nest.views import homepage_redirect
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

def discover_extension_urls():
    """Dynamically discover and include extension URLs"""
    extension_patterns = []

    for app_name in settings.DISCOVERED_EXTENSIONS:
        # Convert 'extensions.risk_analytics' to 'risk_analytics'
        extension_name = app_name.split('.')[-1]

        # Try to import the extension's urls module
        urls_module = f"{app_name}.urls"
        extension_patterns.append(
            path(f'pybirdai/extensions/{extension_name}/', include(urls_module))
        )
        logger.info(f"Added URL pattern for extension: {extension_name}")

    return extension_patterns

# Base URL patterns
urlpatterns = [
    path('admin/', admin.site.urls),
    path('pybirdai/', include('pybirdai.urls')),
    path('', homepage_redirect, name='homepage_redirect'),
]

# Add discovered extension URLs
urlpatterns += discover_extension_urls()
