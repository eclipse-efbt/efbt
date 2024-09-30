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


from django.contrib import admin


from .sdd_models import DOMAIN				
admin.site.register(DOMAIN)
from .sdd_models import FACET_COLLECTION				
admin.site.register(FACET_COLLECTION)
from .sdd_models import MAINTENANCE_AGENCY				
admin.site.register(MAINTENANCE_AGENCY)
from .sdd_models import MEMBER				
admin.site.register(MEMBER)
from .sdd_models import MEMBER_HIERARCHY				
admin.site.register(MEMBER_HIERARCHY)
from .sdd_models import MEMBER_HIERARCHY_NODE				
admin.site.register(MEMBER_HIERARCHY_NODE)
from .sdd_models import VARIABLE				
admin.site.register(VARIABLE)
from .sdd_models import FRAMEWORK				
admin.site.register(FRAMEWORK)
from .sdd_models import MEMBER_MAPPING				
admin.site.register(MEMBER_MAPPING)
from .sdd_models import MEMBER_MAPPING_ITEM				
admin.site.register(MEMBER_MAPPING_ITEM)
from .sdd_models import VARIABLE_MAPPING_ITEM				
admin.site.register(VARIABLE_MAPPING_ITEM)
from .sdd_models import VARIABLE_MAPPING				
admin.site.register(VARIABLE_MAPPING)
from .sdd_models import MAPPING_TO_CUBE				
admin.site.register(MAPPING_TO_CUBE)
from .sdd_models import MAPPING_DEFINITION				
admin.site.register(MAPPING_DEFINITION)
from .sdd_models import AXIS				
admin.site.register(AXIS)
from .sdd_models import AXIS_ORDINATE				
admin.site.register(AXIS_ORDINATE)
from .sdd_models import CELL_POSITION				
admin.site.register(CELL_POSITION)
from .sdd_models import ORDINATE_ITEM				
admin.site.register(ORDINATE_ITEM)
from .sdd_models import TABLE				
admin.site.register(TABLE)
from .sdd_models import TABLE_CELL				
admin.site.register(TABLE_CELL)
from .sdd_models import CUBE
admin.site.register(CUBE)
from .sdd_models import CUBE_STRUCTURE     
admin.site.register(CUBE_STRUCTURE)
from .sdd_models import CUBE_STRUCTURE_ITEM
admin.site.register(CUBE_STRUCTURE_ITEM)
from .sdd_models import CUBE_LINK
admin.site.register(CUBE_LINK)
from .sdd_models import CUBE_STRUCTURE_ITEM_LINK
admin.site.register(CUBE_STRUCTURE_ITEM_LINK)
from .sdd_models import COMBINATION
admin.site.register(COMBINATION)
from .sdd_models import COMBINATION_ITEM
admin.site.register(COMBINATION_ITEM)
from .sdd_models import CUBE_TO_COMBINATION
admin.site.register(CUBE_TO_COMBINATION)
