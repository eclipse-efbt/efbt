
from .core import getEClassifier, eClassifiers
from .core import name, nsURI, nsPrefix, eClass
from .core import DOMAIN, FACET_COLLECTION, FACET_ENUMERATION, facet_type, FACET_VALUE_TYPE, FACET_VALUE_TYPEObject, MAINTENANCE_AGENCY, MEMBER, MEMBER_HIERARCHY, MEMBER_HIERARCHY_NODE, SUBDOMAIN, SUBDOMAIN_ENUMERATION, VARIABLE, VARIABLE_SET, VARIABLE_SET_ENUMERATION


from . import core

__all__ = ['DOMAIN', 'FACET_COLLECTION', 'FACET_ENUMERATION', 'facet_type', 'FACET_VALUE_TYPE', 'FACET_VALUE_TYPEObject', 'MAINTENANCE_AGENCY',
           'MEMBER', 'MEMBER_HIERARCHY', 'MEMBER_HIERARCHY_NODE', 'SUBDOMAIN', 'SUBDOMAIN_ENUMERATION', 'VARIABLE', 'VARIABLE_SET', 'VARIABLE_SET_ENUMERATION']

eSubpackages = []
eSuperPackage = None
core.eSubpackages = eSubpackages
core.eSuperPackage = eSuperPackage

DOMAIN.facet_id.eType = FACET_COLLECTION
DOMAIN.maintenance_agency_id.eType = MAINTENANCE_AGENCY
FACET_COLLECTION.maintenance_agency_id.eType = MAINTENANCE_AGENCY
FACET_ENUMERATION.facet_id.eType = FACET_COLLECTION
FACET_ENUMERATION.facet_type.eType = facet_type
MEMBER.domain_id.eType = DOMAIN
MEMBER.maintenance_agency_id.eType = MAINTENANCE_AGENCY
MEMBER_HIERARCHY.domain_id.eType = DOMAIN
MEMBER_HIERARCHY.maintenance_agency_id.eType = MAINTENANCE_AGENCY
MEMBER_HIERARCHY_NODE.member_hierarchy_id.eType = MEMBER_HIERARCHY
MEMBER_HIERARCHY_NODE.member_id.eType = MEMBER
MEMBER_HIERARCHY_NODE.parent_member_id.eType = MEMBER
SUBDOMAIN.domain_id.eType = DOMAIN
SUBDOMAIN.facet_id.eType = FACET_COLLECTION
SUBDOMAIN.maintenance_agency_id.eType = MAINTENANCE_AGENCY
SUBDOMAIN.items.eType = SUBDOMAIN_ENUMERATION
SUBDOMAIN_ENUMERATION.member_id.eType = MEMBER
VARIABLE.domain_id.eType = DOMAIN
VARIABLE.maintenance_agency_id.eType = MAINTENANCE_AGENCY
VARIABLE_SET.maintenance_agency_id.eType = MAINTENANCE_AGENCY
VARIABLE_SET_ENUMERATION.subdomain_id.eType = SUBDOMAIN
VARIABLE_SET_ENUMERATION.variable_id.eType = VARIABLE
VARIABLE_SET_ENUMERATION.variable_set_id.eType = VARIABLE_SET

otherClassifiers = [FACET_VALUE_TYPE, FACET_VALUE_TYPEObject]

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
