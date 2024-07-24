

from django.contrib import admin

from .models import SDDModule				
admin.site.register(SDDModule)
from .models import DOMAIN				
admin.site.register(DOMAIN)
from .models import FACET_COLLECTION				
admin.site.register(FACET_COLLECTION)
from .models import FACET_ENUMERATION				
admin.site.register(FACET_ENUMERATION)
from .models import facet_type				
admin.site.register(facet_type)
from .models import MAINTENANCE_AGENCY				
admin.site.register(MAINTENANCE_AGENCY)
from .models import MEMBER				
admin.site.register(MEMBER)
from .models import MEMBER_HIERARCHY				
admin.site.register(MEMBER_HIERARCHY)
from .models import MEMBER_HIERARCHY_NODE				
admin.site.register(MEMBER_HIERARCHY_NODE)
from .models import SUBDOMAIN				
admin.site.register(SUBDOMAIN)
from .models import SUBDOMAIN_ENUMERATION				
admin.site.register(SUBDOMAIN_ENUMERATION)
from .models import VARIABLE				
admin.site.register(VARIABLE)
from .models import VARIABLE_SET				
admin.site.register(VARIABLE_SET)
from .models import VARIABLE_SET_ENUMERATION				
admin.site.register(VARIABLE_SET_ENUMERATION)
from .models import COMBINATION				
admin.site.register(COMBINATION)
from .models import COMBINATION_ITEM				
admin.site.register(COMBINATION_ITEM)
from .models import CUBE				
admin.site.register(CUBE)
from .models import CUBE_GROUP				
admin.site.register(CUBE_GROUP)
from .models import CUBE_GROUP_ENUMERATION				
admin.site.register(CUBE_GROUP_ENUMERATION)
from .models import CUBE_HIERARCHY				
admin.site.register(CUBE_HIERARCHY)
from .models import CUBE_HIERARCHY_NODE				
admin.site.register(CUBE_HIERARCHY_NODE)
from .models import CUBE_RELATIONSHIP				
admin.site.register(CUBE_RELATIONSHIP)
from .models import CUBE_STRUCTURE				
admin.site.register(CUBE_STRUCTURE)
from .models import CUBE_STRUCTURE_ITEM				
admin.site.register(CUBE_STRUCTURE_ITEM)
from .models import CUBE_TO_COMBINATION				
admin.site.register(CUBE_TO_COMBINATION)
from .models import FRAMEWORK				
admin.site.register(FRAMEWORK)
from .models import FRAMEWORK_SUBDOMAIN				
admin.site.register(FRAMEWORK_SUBDOMAIN)
from .models import FRAMEWORK_VARIABLE_SET				
admin.site.register(FRAMEWORK_VARIABLE_SET)
from .models import CUBE_MAPPING				
admin.site.register(CUBE_MAPPING)
from .models import MEMBER_MAPPING				
admin.site.register(MEMBER_MAPPING)
from .models import MEMBER_MAPPING_ITEM				
admin.site.register(MEMBER_MAPPING_ITEM)
from .models import VARIABLE_MAPPING_ITEM				
admin.site.register(VARIABLE_MAPPING_ITEM)
from .models import VARIABLE_MAPPING				
admin.site.register(VARIABLE_MAPPING)
from .models import MAPPING_TO_CUBE				
admin.site.register(MAPPING_TO_CUBE)
from .models import VARIABLE_SET_MAPPING				
admin.site.register(VARIABLE_SET_MAPPING)
from .models import MAPPING_DEFINITION				
admin.site.register(MAPPING_DEFINITION)
from .models import AXIS				
admin.site.register(AXIS)
from .models import AXIS_ORDINATE				
admin.site.register(AXIS_ORDINATE)
from .models import CELL_POSITION				
admin.site.register(CELL_POSITION)
from .models import ORDINATE_ITEM				
admin.site.register(ORDINATE_ITEM)
from .models import TABLE				
admin.site.register(TABLE)
from .models import TABLE_CELL				
admin.site.register(TABLE_CELL)
from .models import DomainModule				
admin.site.register(DomainModule)
from .models import MemberHierarchyModule				
admin.site.register(MemberHierarchyModule)
from .models import MemberModule				
admin.site.register(MemberModule)
from .models import VariableModule				
admin.site.register(VariableModule)
from .models import VariableSetModule				
admin.site.register(VariableSetModule)
from .models import SubDomainModule				
admin.site.register(SubDomainModule)
from .models import SMCubesCoreModel				
admin.site.register(SMCubesCoreModel)
from .models import SMCubesModel				
admin.site.register(SMCubesModel)
from .models import SMCubesExtraModel				
admin.site.register(SMCubesExtraModel)
from .models import CombinationModule				
admin.site.register(CombinationModule)
from .models import CubeModule				
admin.site.register(CubeModule)
from .models import FrameworkModule				
admin.site.register(FrameworkModule)
from .models import MappingDefinitionModule				
admin.site.register(MappingDefinitionModule)
from .models import CubeMappingModule				
admin.site.register(CubeMappingModule)
from .models import MemberMappingModule				
admin.site.register(MemberMappingModule)
from .models import VariableMappingModule				
admin.site.register(VariableMappingModule)
from .models import AxisModule				
admin.site.register(AxisModule)
from .models import AxisOrdinateModule				
admin.site.register(AxisOrdinateModule)
from .models import CellPositionModule				
admin.site.register(CellPositionModule)
from .models import OrdinateItemModule				
admin.site.register(OrdinateItemModule)
from .models import ReportTableModule				
admin.site.register(ReportTableModule)
from .models import TableCellModule				
admin.site.register(TableCellModule)
from .models import MaintenanceAgencyModule				
admin.site.register(MaintenanceAgencyModule)
from .models import CUBE_LINK				
admin.site.register(CUBE_LINK)
from .models import CUBE_STRUCTURE_ITEM_LINK				
admin.site.register(CUBE_STRUCTURE_ITEM_LINK)
from .models import MEMBER_LINK				
admin.site.register(MEMBER_LINK)



from .models import Import				
admin.site.register(Import)
from .models import Module				
admin.site.register(Module)
from .models import ModuleList				
admin.site.register(ModuleList)
from .models import RulesForReport				
admin.site.register(RulesForReport)
from .models import RulesForILTable				
admin.site.register(RulesForILTable)
from .models import SelectColumn				
admin.site.register(SelectColumn)
from .models import SelectColumnMemberAs				
admin.site.register(SelectColumnMemberAs)
from .models import SelectColumnAttributeAs				
admin.site.register(SelectColumnAttributeAs)
from .models import SelectDerivedColumnAs				
admin.site.register(SelectDerivedColumnAs)
from .models import SelectValueAs				
admin.site.register(SelectValueAs)
from .models import TableFilter				
admin.site.register(TableFilter)
from .models import GenerationRulesModule				
admin.site.register(GenerationRulesModule)
from .models import RuleForILTablePart				
admin.site.register(RuleForILTablePart)
from .models import Predicate				
admin.site.register(Predicate)
from .models import AndPredicate				
admin.site.register(AndPredicate)
from .models import OrPredicate				
admin.site.register(OrPredicate)
from .models import NotPredicate				
admin.site.register(NotPredicate)
from .models import AttributePredicate				
admin.site.register(AttributePredicate)
from .models import ELAttribute				
admin.site.register(ELAttribute)
from .models import ELClass				
admin.site.register(ELClass)
from .models import ELClassifier				
admin.site.register(ELClassifier)
from .models import ELDataType				
admin.site.register(ELDataType)
from .models import ELEnum				
admin.site.register(ELEnum)
from .models import ELEnumLiteral				
admin.site.register(ELEnumLiteral)
from .models import ELModelElement				
admin.site.register(ELModelElement)
from .models import ELNamedElement				
admin.site.register(ELNamedElement)
from .models import ELOperation				
admin.site.register(ELOperation)
from .models import ELParameter				
admin.site.register(ELParameter)
from .models import ELPackage				
admin.site.register(ELPackage)
from .models import ELReference				
admin.site.register(ELReference)
from .models import ELStructuralFeature				
admin.site.register(ELStructuralFeature)
from .models import ELTypedElement				
admin.site.register(ELTypedElement)
from .models import ELAnnotation				
admin.site.register(ELAnnotation)
from .models import ELAnnotationDirective				
admin.site.register(ELAnnotationDirective)
from .models import ELStringToStringMapEntry				
admin.site.register(ELStringToStringMapEntry)
from .models import Report				
admin.site.register(Report)
from .models import CellBasedReport				
admin.site.register(CellBasedReport)
from .models import ReportRow				
admin.site.register(ReportRow)
from .models import ReportColumn				
admin.site.register(ReportColumn)
from .models import ReportCell				
admin.site.register(ReportCell)
from .models import Filter				
admin.site.register(Filter)
from .models import ReportModule				
admin.site.register(ReportModule)
from .models import RowColumnBasedReport				
admin.site.register(RowColumnBasedReport)
from .models import RowFilters				
admin.site.register(RowFilters)
from .models import ColumnFilters				
admin.site.register(ColumnFilters)
from .models import WholeReportFilters				
admin.site.register(WholeReportFilters)

