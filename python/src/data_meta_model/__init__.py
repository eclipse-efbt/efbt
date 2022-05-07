
from .data_meta_model import getEClassifier, eClassifiers
from .data_meta_model import name, nsURI, nsPrefix, eClass
from .data_meta_model import Attribute, RelationshipAttribute, OneToOneRelationshipAttribute, ManyToOneRelationshipAttribute, OneToManyRelationshipAttribute, ManyToManyRelationshipAttribute, Classifier, Element, NamedElement, EntityModule, StructuralFeature, TypedElement, GeneratedEntity, Entity, DerivedEntity, BasicEntity

from core import VARIABLE

from . import data_meta_model

__all__ = ['Attribute', 'RelationshipAttribute', 'OneToOneRelationshipAttribute', 'ManyToOneRelationshipAttribute', 'OneToManyRelationshipAttribute', 'ManyToManyRelationshipAttribute',
           'Classifier', 'Element', 'NamedElement', 'EntityModule', 'StructuralFeature', 'TypedElement', 'GeneratedEntity', 'Entity', 'DerivedEntity', 'BasicEntity']

eSubpackages = []
eSuperPackage = None
data_meta_model.eSubpackages = eSubpackages
data_meta_model.eSuperPackage = eSuperPackage

Attribute.variable.eType = VARIABLE
RelationshipAttribute.entity.eType = Entity
EntityModule.entities.eType = Entity
TypedElement.classifier.eType = Classifier
GeneratedEntity.attributes.eType = Attribute
DerivedEntity.attributes.eType = Attribute
BasicEntity.attributes.eType = Attribute
BasicEntity.superClass.eType = BasicEntity

otherClassifiers = []

for classif in otherClassifiers:
    eClassifiers[classif.name] = classif
    classif.ePackage = eClass

for classif in eClassifiers.values():
    eClass.eClassifiers.append(classif.eClass)

for subpack in eSubpackages:
    eClass.eSubpackages.append(subpack.eClass)
