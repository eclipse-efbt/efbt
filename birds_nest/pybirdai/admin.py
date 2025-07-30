from django.contrib import admin

from .models.lineage_model import DatabaseTable
admin.site.register(DatabaseTable)
from .models.lineage_model import DerivedTable
admin.site.register(DerivedTable)
from .models.lineage_model import DatabaseField
admin.site.register(DatabaseField)
from .models.lineage_model import Function
admin.site.register(Function)
from .models.lineage_model import DatabaseRow
admin.site.register(DatabaseRow)
from .models.lineage_model import DerivedTableRow
admin.site.register(DerivedTableRow)
from .models.lineage_model import DatabaseColumnValue
admin.site.register(DatabaseColumnValue)
from .models.lineage_model import EvaluatedFunction
admin.site.register(EvaluatedFunction)
from .models.lineage_model import Trail
admin.site.register(Trail)
from .models.lineage_model import MetaDataTrail
admin.site.register(MetaDataTrail)
from .models.lineage_model import PopulatedDataBaseTable
admin.site.register(PopulatedDataBaseTable)
from .models.lineage_model import EvaluatedDerivedTable
admin.site.register(EvaluatedDerivedTable)
from .models.lineage_model import FunctionText
admin.site.register(FunctionText)
from .models.lineage_model import TableCreationFunction
admin.site.register(TableCreationFunction)
from .models.lineage_model import AortaTableReference
admin.site.register(AortaTableReference)
from .models.lineage_model import FunctionColumnReference
admin.site.register(FunctionColumnReference)
from .models.lineage_model import DerivedRowSourceReference
admin.site.register(DerivedRowSourceReference)
from .models.lineage_model import EvaluatedFunctionSourceValue
admin.site.register(EvaluatedFunctionSourceValue)
from .models.lineage_model import TableCreationSourceTable
admin.site.register(TableCreationSourceTable)

from .models.lineage_model import TableCreationFunctionColumn
admin.site.register(TableCreationFunctionColumn)
from .models.bird_meta_data_model import SUBDOMAIN
admin.site.register(SUBDOMAIN)
from .models.bird_meta_data_model import SUBDOMAIN_ENUMERATION
admin.site.register(SUBDOMAIN_ENUMERATION)
from .models.bird_meta_data_model import DOMAIN
admin.site.register(DOMAIN)
from .models.bird_meta_data_model import FACET_COLLECTION
admin.site.register(FACET_COLLECTION)
from .models.bird_meta_data_model import MAINTENANCE_AGENCY
admin.site.register(MAINTENANCE_AGENCY)
from .models.bird_meta_data_model import MEMBER
admin.site.register(MEMBER)
from .models.bird_meta_data_model import MEMBER_HIERARCHY
admin.site.register(MEMBER_HIERARCHY)
from .models.bird_meta_data_model import MEMBER_HIERARCHY_NODE
admin.site.register(MEMBER_HIERARCHY_NODE)
from .models.bird_meta_data_model import VARIABLE
admin.site.register(VARIABLE)
from .models.bird_meta_data_model import VARIABLE_SET
admin.site.register(VARIABLE_SET)
from .models.bird_meta_data_model import VARIABLE_SET_ENUMERATION
admin.site.register(VARIABLE_SET_ENUMERATION)
from .models.bird_meta_data_model import FRAMEWORK
admin.site.register(FRAMEWORK)
from .models.bird_meta_data_model import MEMBER_MAPPING
admin.site.register(MEMBER_MAPPING)
from .models.bird_meta_data_model import MEMBER_MAPPING_ITEM
admin.site.register(MEMBER_MAPPING_ITEM)
from .models.bird_meta_data_model import VARIABLE_MAPPING_ITEM
admin.site.register(VARIABLE_MAPPING_ITEM)
from .models.bird_meta_data_model import VARIABLE_MAPPING
admin.site.register(VARIABLE_MAPPING)
from .models.bird_meta_data_model import MAPPING_TO_CUBE
admin.site.register(MAPPING_TO_CUBE)
from .models.bird_meta_data_model import MAPPING_DEFINITION
admin.site.register(MAPPING_DEFINITION)
from .models.bird_meta_data_model import AXIS
admin.site.register(AXIS)
from .models.bird_meta_data_model import AXIS_ORDINATE
admin.site.register(AXIS_ORDINATE)
from .models.bird_meta_data_model import CELL_POSITION
admin.site.register(CELL_POSITION)
from .models.bird_meta_data_model import ORDINATE_ITEM
admin.site.register(ORDINATE_ITEM)
from .models.bird_meta_data_model import TABLE
admin.site.register(TABLE)
from .models.bird_meta_data_model import TABLE_CELL
admin.site.register(TABLE_CELL)
from .models.bird_meta_data_model import CUBE_STRUCTURE
admin.site.register(CUBE_STRUCTURE)
from .models.bird_meta_data_model import CUBE_STRUCTURE_ITEM
admin.site.register(CUBE_STRUCTURE_ITEM)
from .models.bird_meta_data_model import CUBE
admin.site.register(CUBE)
from .models.bird_meta_data_model import CUBE_LINK
admin.site.register(CUBE_LINK)
from .models.bird_meta_data_model import CUBE_STRUCTURE_ITEM_LINK
admin.site.register(CUBE_STRUCTURE_ITEM_LINK)
from .models.bird_meta_data_model import COMBINATION
admin.site.register(COMBINATION)
from .models.bird_meta_data_model import COMBINATION_ITEM
admin.site.register(COMBINATION_ITEM)
from .models.bird_meta_data_model import CUBE_TO_COMBINATION
admin.site.register(CUBE_TO_COMBINATION)
from .models.bird_meta_data_model import MEMBER_LINK
admin.site.register(MEMBER_LINK)
from .models.workflow_model import AutomodeConfiguration
admin.site.register(AutomodeConfiguration)
from .models.workflow_model import WorkflowTaskExecution
admin.site.register(WorkflowTaskExecution)
from .models.workflow_model import WorkflowTaskDependency
admin.site.register(WorkflowTaskDependency)
from .models.workflow_model import WorkflowSession
admin.site.register(WorkflowSession)
