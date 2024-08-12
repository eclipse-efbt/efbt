

from django.contrib import admin


from .sdd_models import DOMAIN				
admin.site.register(DOMAIN)
from .sdd_models import FACET_COLLECTION				
admin.site.register(FACET_COLLECTION)
from .sdd_models import FACET_ENUMERATION				
admin.site.register(FACET_ENUMERATION)
from .sdd_models import facet_type				
admin.site.register(facet_type)
from .sdd_models import MAINTENANCE_AGENCY				
admin.site.register(MAINTENANCE_AGENCY)
from .sdd_models import MEMBER				
admin.site.register(MEMBER)
from .sdd_models import MEMBER_HIERARCHY				
admin.site.register(MEMBER_HIERARCHY)
from .sdd_models import MEMBER_HIERARCHY_NODE				
admin.site.register(MEMBER_HIERARCHY_NODE)
from .sdd_models import SUBDOMAIN				
admin.site.register(SUBDOMAIN)
from .sdd_models import SUBDOMAIN_ENUMERATION				
admin.site.register(SUBDOMAIN_ENUMERATION)
from .sdd_models import VARIABLE				
admin.site.register(VARIABLE)
from .sdd_models import VARIABLE_SET				
admin.site.register(VARIABLE_SET)
from .sdd_models import VARIABLE_SET_ENUMERATION				
admin.site.register(VARIABLE_SET_ENUMERATION)
from .sdd_models import FRAMEWORK				
admin.site.register(FRAMEWORK)
from .sdd_models import FRAMEWORK_SUBDOMAIN				
admin.site.register(FRAMEWORK_SUBDOMAIN)
from .sdd_models import FRAMEWORK_VARIABLE_SET				
admin.site.register(FRAMEWORK_VARIABLE_SET)
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
from .sdd_models import VARIABLE_SET_MAPPING				
admin.site.register(VARIABLE_SET_MAPPING)
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
