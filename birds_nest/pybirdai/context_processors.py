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

from django.conf import settings

def extensions_context(request):
    """
    Context processor to make discovered extensions available in templates.
    """
    return {
        'discovered_extensions': getattr(settings, 'DISCOVERED_EXTENSIONS', []),
        'extension_count': len(getattr(settings, 'DISCOVERED_EXTENSIONS', [])),
    }