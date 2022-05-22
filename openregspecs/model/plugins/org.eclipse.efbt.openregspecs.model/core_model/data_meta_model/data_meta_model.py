"""Definition of meta model 'data_meta_model'."""
from functools import partial
import pyecore.ecore as Ecore
from pyecore.ecore import *
from module_management import Module


name = 'data_meta_model'
nsURI = 'http://www.eclipse.org/efbt/data_meta_model'
nsPrefix = 'data_meta_model'

eClass = EPackage(name=name, nsURI=nsURI, nsPrefix=nsPrefix)

eClassifiers = {}
getEClassifier = partial(Ecore.getEClassifier, searchspace=eClassifiers)


class Concept(EObject, metaclass=MetaEClass):

    conceptName = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, conceptName=None):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()

        if conceptName is not None:
            self.conceptName = conceptName


@abstract
class Element(EObject, metaclass=MetaEClass):

    def __init__(self):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()


class EnumMember(EObject, metaclass=MetaEClass):

    def __init__(self):
        # if kwargs:
        #    raise AttributeError('unexpected arguments: {}'.format(kwargs))

        super().__init__()


@abstract
class NamedElement(Element):

    name = EAttribute(eType=EString, unique=True, derived=False, changeable=True)

    def __init__(self, *, name=None, **kwargs):

        super().__init__(**kwargs)

        if name is not None:
            self.name = name


class EntityModule(Module):

    nsURI = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    nsPrefix = EAttribute(eType=EString, unique=True, derived=False, changeable=True)
    entities = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, nsURI=None, nsPrefix=None, entities=None, **kwargs):

        super().__init__(**kwargs)

        if nsURI is not None:
            self.nsURI = nsURI

        if nsPrefix is not None:
            self.nsPrefix = nsPrefix

        if entities:
            self.entities.extend(entities)


@abstract
class Classifier(NamedElement):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


@abstract
class TypedElement(NamedElement):

    classifier = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, classifier=None, **kwargs):

        super().__init__(**kwargs)

        if classifier is not None:
            self.classifier = classifier


@abstract
class Entity(NamedElement):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


@abstract
class StructuralFeature(TypedElement):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class GeneratedEntity(Entity):

    attributes = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, attributes=None, **kwargs):

        super().__init__(**kwargs)

        if attributes:
            self.attributes.extend(attributes)


class DerivedEntity(Entity):

    attributes = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)

    def __init__(self, *, attributes=None, **kwargs):

        super().__init__(**kwargs)

        if attributes:
            self.attributes.extend(attributes)


class BasicEntity(Entity):

    attributes = EReference(ordered=True, unique=True, containment=True, derived=False, upper=-1)
    superClass = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, attributes=None, superClass=None, **kwargs):

        super().__init__(**kwargs)

        if attributes:
            self.attributes.extend(attributes)

        if superClass is not None:
            self.superClass = superClass


class Attribute(StructuralFeature):

    isPK = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    ordered = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    concept = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, isPK=None, concept=None, ordered=None, **kwargs):

        super().__init__(**kwargs)

        if isPK is not None:
            self.isPK = isPK

        if ordered is not None:
            self.ordered = ordered

        if concept is not None:
            self.concept = concept


class RelationshipAttribute(Attribute):

    containment = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    mandatory = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    dominant = EAttribute(eType=EBoolean, unique=True, derived=False, changeable=True)
    entity = EReference(ordered=True, unique=True, containment=False, derived=False)

    def __init__(self, *, entity=None, containment=None, mandatory=None, dominant=None, **kwargs):

        super().__init__(**kwargs)

        if containment is not None:
            self.containment = containment

        if mandatory is not None:
            self.mandatory = mandatory

        if dominant is not None:
            self.dominant = dominant

        if entity is not None:
            self.entity = entity


class OneToOneRelationshipAttribute(RelationshipAttribute):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class ManyToOneRelationshipAttribute(RelationshipAttribute):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class OneToManyRelationshipAttribute(RelationshipAttribute):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)


class ManyToManyRelationshipAttribute(RelationshipAttribute):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
