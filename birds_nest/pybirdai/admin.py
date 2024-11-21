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


from .bird_meta_data_model import DOMAIN				
admin.site.register(DOMAIN)
from .bird_meta_data_model import SUBDOMAIN				
admin.site.register(SUBDOMAIN)



from .bird_meta_data_model import FACET_COLLECTION				
admin.site.register(FACET_COLLECTION)
from .bird_meta_data_model import MAINTENANCE_AGENCY				
admin.site.register(MAINTENANCE_AGENCY)
from .bird_meta_data_model import MEMBER				
admin.site.register(MEMBER)
from .bird_meta_data_model import MEMBER_HIERARCHY				
admin.site.register(MEMBER_HIERARCHY)
from .bird_meta_data_model import MEMBER_HIERARCHY_NODE				
admin.site.register(MEMBER_HIERARCHY_NODE)
from .bird_meta_data_model import VARIABLE				
admin.site.register(VARIABLE)
from .bird_meta_data_model import FRAMEWORK				
admin.site.register(FRAMEWORK)
from .bird_meta_data_model import MEMBER_MAPPING				
admin.site.register(MEMBER_MAPPING)
from .bird_meta_data_model import MEMBER_MAPPING_ITEM				
admin.site.register(MEMBER_MAPPING_ITEM)
from .bird_meta_data_model import VARIABLE_MAPPING_ITEM				
admin.site.register(VARIABLE_MAPPING_ITEM)
from .bird_meta_data_model import VARIABLE_MAPPING				
admin.site.register(VARIABLE_MAPPING)
from .bird_meta_data_model import MAPPING_TO_CUBE				
admin.site.register(MAPPING_TO_CUBE)
from .bird_meta_data_model import MAPPING_DEFINITION				
admin.site.register(MAPPING_DEFINITION)
from .bird_meta_data_model import AXIS				
admin.site.register(AXIS)
from .bird_meta_data_model import AXIS_ORDINATE				
admin.site.register(AXIS_ORDINATE)
from .bird_meta_data_model import CELL_POSITION				
admin.site.register(CELL_POSITION)
from .bird_meta_data_model import ORDINATE_ITEM				
admin.site.register(ORDINATE_ITEM)
from .bird_meta_data_model import TABLE				
admin.site.register(TABLE)
from .bird_meta_data_model import TABLE_CELL				
admin.site.register(TABLE_CELL)
from .bird_meta_data_model import CUBE
admin.site.register(CUBE)
from .bird_meta_data_model import CUBE_STRUCTURE     
admin.site.register(CUBE_STRUCTURE)
from .bird_meta_data_model import CUBE_STRUCTURE_ITEM
admin.site.register(CUBE_STRUCTURE_ITEM)
from .bird_meta_data_model import CUBE_LINK
admin.site.register(CUBE_LINK)
from .bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK
admin.site.register(CUBE_STRUCTURE_ITEM_LINK)
from .bird_meta_data_model import COMBINATION
admin.site.register(COMBINATION)
from .bird_meta_data_model import COMBINATION_ITEM
admin.site.register(COMBINATION_ITEM)
from .bird_meta_data_model import CUBE_TO_COMBINATION
admin.site.register(CUBE_TO_COMBINATION)
