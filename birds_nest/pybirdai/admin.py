from django.contrib import admin

from .models.bpmn_lite_models import Task
admin.site.register(Task)
from .models.bpmn_lite_models import ExclusiveGateway
admin.site.register(ExclusiveGateway)
from .models.bpmn_lite_models import InclusiveGateway
admin.site.register(InclusiveGateway)
from .models.bpmn_lite_models import ParallelGateway
admin.site.register(ParallelGateway)
from .models.bpmn_lite_models import SequenceFlow
admin.site.register(SequenceFlow)
from .models.bpmn_lite_models import DataConstraint
admin.site.register(DataConstraint)
from .models.bpmn_lite_models import Scenario
admin.site.register(Scenario)
from .models.bpmn_lite_models import ServiceTask
admin.site.register(ServiceTask)
from .models.bpmn_lite_models import ServiceTaskScenario
admin.site.register(ServiceTaskScenario)
from .models.bpmn_lite_models import SelectionLayer
admin.site.register(SelectionLayer)
from .models.bpmn_lite_models import ScriptTask
admin.site.register(ScriptTask)
from .models.bpmn_lite_models import ScriptTaskSelectionLayer
admin.site.register(ScriptTaskSelectionLayer)
from .models.bpmn_lite_models import UserTask
admin.site.register(UserTask)
from .models.bpmn_lite_models import SubProcess
admin.site.register(SubProcess)
from .models.bpmn_lite_models import SubProcessFlowElement
admin.site.register(SubProcessFlowElement)
from .models.bpmn_lite_models import ActivityTag
admin.site.register(ActivityTag)
from .models.bpmn_lite_models import ScenarioTag
admin.site.register(ScenarioTag)
from .models.bpmn_lite_models import WorkflowModule
admin.site.register(WorkflowModule)
from .models.bpmn_lite_models import WorkflowModuleContent
admin.site.register(WorkflowModuleContent)
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
from .models.bird_meta_data_model import TABLE_AMENDMENT
admin.site.register(TABLE_AMENDMENT)
from .models.workflow_model import AutomodeConfiguration
admin.site.register(AutomodeConfiguration)
from .models.workflow_model import WorkflowTaskExecution
admin.site.register(WorkflowTaskExecution)
from .models.workflow_model import WorkflowTaskDependency
admin.site.register(WorkflowTaskDependency)
from .models.workflow_model import WorkflowSession
admin.site.register(WorkflowSession)
from .models.workflow_model import DPMProcessExecution
admin.site.register(DPMProcessExecution)
from .models.workflow_model import AnaCreditProcessExecution
admin.site.register(AnaCreditProcessExecution)
from .models.workflow_model import FrameworkTestSuite
admin.site.register(FrameworkTestSuite)
from .models.requirements_text_models import RequirementType
admin.site.register(RequirementType)
from .models.requirements_text_models import AllowedTypes
admin.site.register(AllowedTypes)
from .models.requirements_text_models import AllowedTypesItem
admin.site.register(AllowedTypesItem)
from .models.requirements_text_models import RequirementsSection
admin.site.register(RequirementsSection)
from .models.requirements_text_models import RequirementsSectionText
admin.site.register(RequirementsSectionText)
from .models.requirements_text_models import RequirementsSectionImage
admin.site.register(RequirementsSectionImage)
from .models.requirements_text_models import TitledRequirementsSection
admin.site.register(TitledRequirementsSection)
from .models.requirements_text_models import TitledRequirementsSectionContent
admin.site.register(TitledRequirementsSectionContent)
from .models.requirements_text_models import RequirementsSectionLinkWithText
admin.site.register(RequirementsSectionLinkWithText)
from .models.requirements_text_models import Tag
admin.site.register(Tag)
from .models.requirements_text_models import TagRequirement
admin.site.register(TagRequirement)
from .models.requirements_text_models import TagGroup
admin.site.register(TagGroup)
from .models.requirements_text_models import TagGroupItem
admin.site.register(TagGroupItem)
from .models.requirements_text_models import RequirementsModule
admin.site.register(RequirementsModule)
from .models.requirements_text_models import RequirementsModuleRule
admin.site.register(RequirementsModuleRule)
from .models.bird_meta_data_model_extension import FRAMEWORK_TABLE
admin.site.register(FRAMEWORK_TABLE)
from .models.bird_meta_data_model_extension import FRAMEWORK_SUBDOMAIN
admin.site.register(FRAMEWORK_SUBDOMAIN)
from .models.bird_meta_data_model_extension import FRAMEWORK_HIERARCHY
admin.site.register(FRAMEWORK_HIERARCHY)
from .models.bird_meta_data_model_extension import MAPPING_ORDINATE_LINK
admin.site.register(MAPPING_ORDINATE_LINK)
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
from .models.lineage_model import CalculationUsedRow
admin.site.register(CalculationUsedRow)
from .models.lineage_model import CalculationUsedField
admin.site.register(CalculationUsedField)
from .models.lineage_model import TransformationStep
admin.site.register(TransformationStep)
from .models.lineage_model import TransformationStepInput
admin.site.register(TransformationStepInput)
from .models.lineage_model import TransformationStepOutput
admin.site.register(TransformationStepOutput)
from .models.lineage_model import CalculationChain
admin.site.register(CalculationChain)
from .models.lineage_model import CalculationChainStep
admin.site.register(CalculationChainStep)
from .models.lineage_model import DataFlowEdge
admin.site.register(DataFlowEdge)
from .models.lineage_model import CellLineage
admin.site.register(CellLineage)
from .models.lineage_model import CellSourceRow
admin.site.register(CellSourceRow)
from .models.lineage_model import LineageSummaryCache
admin.site.register(LineageSummaryCache)
