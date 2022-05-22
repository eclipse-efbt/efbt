"""Definition of meta model 'data_definition'."""
from functools import partial
import pyecore.ecore as Ecore
from pyecore.ecore import *


name = 'data_definition'
nsURI = 'http://www.eclipse.org/efbt/data_definition'
nsPrefix = 'data_definition'

eClass = EPackage(name=name, nsURI=nsURI, nsPrefix=nsPrefix)

eClassifiers = {}
getEClassifier = partial(Ecore.getEClassifier, searchspace=eClassifiers)
TYP_DMNSN = EEnum('TYP_DMNSN', literals=['B', 'M', 'T', 'U'])

TYP_RL = EEnum('TYP_RL', literals=['O', 'A', 'D'])


TYP_DMNSNObject = EDataType(
    'TYP_DMNSNObject', instanceClassName='org.eclipse.emf.common.util.Enumerator')

TYP_RLObject = EDataType('TYP_RLObject', instanceClassName='org.eclipse.emf.common.util.Enumerator')


class COMBINATION(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    combination_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    combination_items = EReference(ordered=True, unique=True,
                                   containment=True, derived=False, upper=-1)

    def __init__(self, *, code=None, combination_id=None, maintenance_agency_id=None, name=None, valid_from=None, valid_to=None, version=None, combination_items=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if combination_id is not None:
            self.combination_id = combination_id

        if name is not None:
            self.name = name

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if version is not None:
            self.version = version

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if combination_items:
            self.combination_items.extend(combination_items)


class COMBINATION_ITEM(EObject, metaclass=MetaEClass):

    member_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    subdomain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_set_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, member_id=None, subdomain_id=None, variable_id=None, variable_set_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if member_id is not None:
            self.member_id = member_id

        if subdomain_id is not None:
            self.subdomain_id = subdomain_id

        if variable_id is not None:
            self.variable_id = variable_id

        if variable_set_id is not None:
            self.variable_set_id = variable_set_id


class CUBE(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_type = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    is_allowed = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    published = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_structure_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    framework_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, cube_id=None, cube_structure_id=None, cube_type=None, description=None, framework_id=None, is_allowed=None, maintenance_agency_id=None, name=None, published=None, valid_from=None, valid_to=None, version=None, displayName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if cube_id is not None:
            self.cube_id = cube_id

        if cube_type is not None:
            self.cube_type = cube_type

        if description is not None:
            self.description = description

        if is_allowed is not None:
            self.is_allowed = is_allowed

        if name is not None:
            self.name = name

        if published is not None:
            self.published = published

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if version is not None:
            self.version = version

        if displayName is not None:
            self.displayName = displayName

        if cube_structure_id is not None:
            self.cube_structure_id = cube_structure_id

        if framework_id is not None:
            self.framework_id = framework_id

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class CUBE_GROUP(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_group_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, cube_group_id=None, description=None, maintenance_agency_id=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if cube_group_id is not None:
            self.cube_group_id = cube_group_id

        if description is not None:
            self.description = description

        if name is not None:
            self.name = name

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class CUBE_GROUP_ENUMERATION(EObject, metaclass=MetaEClass):

    order = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    cube_group_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    cube_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, cube_group_id=None, cube_id=None, order=None, valid_from=None, valid_to=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if order is not None:
            self.order = order

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if cube_group_id is not None:
            self.cube_group_id = cube_group_id

        if cube_id is not None:
            self.cube_id = cube_id


class CUBE_HIERARCHY(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_hierarchy_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_hierarchy_type = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    framework_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, cube_hierarchy_id=None, cube_hierarchy_type=None, framework_id=None, maintenance_agency_id=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if cube_hierarchy_id is not None:
            self.cube_hierarchy_id = cube_hierarchy_id

        if cube_hierarchy_type is not None:
            self.cube_hierarchy_type = cube_hierarchy_type

        if name is not None:
            self.name = name

        if framework_id is not None:
            self.framework_id = framework_id

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class CUBE_HIERARCHY_NODE(EObject, metaclass=MetaEClass):

    level = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    nODE_CODE = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    node_name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    order = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    cube_group_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    cube_hierarchy_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    parent_node_code = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, cube_group_id=None, cube_hierarchy_id=None, level=None, nODE_CODE=None, node_name=None, order=None, parent_node_code=None, valid_from=None, valid_to=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if level is not None:
            self.level = level

        if nODE_CODE is not None:
            self.nODE_CODE = nODE_CODE

        if node_name is not None:
            self.node_name = node_name

        if order is not None:
            self.order = order

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if cube_group_id is not None:
            self.cube_group_id = cube_group_id

        if cube_hierarchy_id is not None:
            self.cube_hierarchy_id = cube_hierarchy_id

        if parent_node_code is not None:
            self.parent_node_code = parent_node_code


class CUBE_RELATIONSHIP(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_relationship_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    establishes_integrity = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    foreign_cube_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    foreign_cube_variable_code = EReference(
        ordered=True, unique=True, containment=False, derived=False)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    primary_cube_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    primary_cube_variable_id = EReference(
        ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, cube_relationship_id=None, description=None, establishes_integrity=None, foreign_cube_id=None, foreign_cube_variable_code=None, maintenance_agency_id=None, name=None, primary_cube_id=None, primary_cube_variable_id=None, valid_from=None, valid_to=None, version=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if cube_relationship_id is not None:
            self.cube_relationship_id = cube_relationship_id

        if description is not None:
            self.description = description

        if establishes_integrity is not None:
            self.establishes_integrity = establishes_integrity

        if name is not None:
            self.name = name

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if version is not None:
            self.version = version

        if foreign_cube_id is not None:
            self.foreign_cube_id = foreign_cube_id

        if foreign_cube_variable_code is not None:
            self.foreign_cube_variable_code = foreign_cube_variable_code

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id

        if primary_cube_id is not None:
            self.primary_cube_id = primary_cube_id

        if primary_cube_variable_id is not None:
            self.primary_cube_variable_id = primary_cube_variable_id


class CUBE_STRUCTURE(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    cube_structure_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    valid_from = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    valid_to = EAttribute(eType=EDate, unique=True, derived=False, changeable=True)
    version = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    displayName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, cube_structure_id=None, description=None, maintenance_agency_id=None, name=None, valid_from=None, valid_to=None, version=None, displayName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if cube_structure_id is not None:
            self.cube_structure_id = cube_structure_id

        if description is not None:
            self.description = description

        if name is not None:
            self.name = name

        if valid_from is not None:
            self.valid_from = valid_from

        if valid_to is not None:
            self.valid_to = valid_to

        if version is not None:
            self.version = version

        if displayName is not None:
            self.displayName = displayName

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class CUBE_STRUCTURE_ITEM(EObject, metaclass=MetaEClass):

    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    dimension_type = EAttribute(eType=TYP_DMNSN, unique=True, derived=False, changeable=True)
    is_flow = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    is_mandatory = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    order = EAttribute(eType=EInt, unique=True, derived=False, changeable=True)
    role = EAttribute(eType=TYP_RL, unique=True, derived=False, changeable=True)
    isIdentifier = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    cube_variable_code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    attribute_associated_variable = EReference(
        ordered=True, unique=True, containment=False, derived=False)
    cube_structure_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    member_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    subdomain_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_set_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, attribute_associated_variable=None, cube_structure_id=None, description=None, dimension_type=None, is_flow=None, is_mandatory=None, member_id=None, order=None, role=None, subdomain_id=None, variable_id=None, variable_set_id=None, isIdentifier=None, cube_variable_code=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if description is not None:
            self.description = description

        if dimension_type is not None:
            self.dimension_type = dimension_type

        if is_flow is not None:
            self.is_flow = is_flow

        if is_mandatory is not None:
            self.is_mandatory = is_mandatory

        if order is not None:
            self.order = order

        if role is not None:
            self.role = role

        if isIdentifier is not None:
            self.isIdentifier = isIdentifier

        if cube_variable_code is not None:
            self.cube_variable_code = cube_variable_code

        if attribute_associated_variable is not None:
            self.attribute_associated_variable = attribute_associated_variable

        if cube_structure_id is not None:
            self.cube_structure_id = cube_structure_id

        if member_id is not None:
            self.member_id = member_id

        if subdomain_id is not None:
            self.subdomain_id = subdomain_id

        if variable_id is not None:
            self.variable_id = variable_id

        if variable_set_id is not None:
            self.variable_set_id = variable_set_id


class CUBE_TO_COMBINATION(EObject, metaclass=MetaEClass):

    combination_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    cube_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, combination_id=None, cube_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if combination_id is not None:
            self.combination_id = combination_id

        if cube_id is not None:
            self.cube_id = cube_id


class FRAMEWORK(EObject, metaclass=MetaEClass):

    code = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    description = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    framework_id = EAttribute(eType=EString, unique=True, derived=False, changeable=True, iD=True)
    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    maintenance_agency_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, code=None, description=None, framework_id=None, maintenance_agency_id=None, name=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if code is not None:
            self.code = code

        if description is not None:
            self.description = description

        if framework_id is not None:
            self.framework_id = framework_id

        if name is not None:
            self.name = name

        if maintenance_agency_id is not None:
            self.maintenance_agency_id = maintenance_agency_id


class FRAMEWORK_SUBDOMAIN(EObject, metaclass=MetaEClass):

    framework_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    subdomain_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, framework_id=None, subdomain_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if framework_id is not None:
            self.framework_id = framework_id

        if subdomain_id is not None:
            self.subdomain_id = subdomain_id


class FRAMEWORK_VARIABLE_SET(EObject, metaclass=MetaEClass):

    framework_id = EReference(ordered=True, unique=True, containment=False, derived=False)
    variable_set_id = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, framework_id=None, variable_set_id=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if framework_id is not None:
            self.framework_id = framework_id

        if variable_set_id is not None:
            self.variable_set_id = variable_set_id
