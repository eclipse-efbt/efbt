# coding=UTF-8
# Copyright (c) 2026 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#
"""The Django-shaped model blueprint built in stage one of the SQL Developer import.

Importing the BIRD model happens in two stages:

1. **Build** - the SQL Developer CSVs are read into the blueprint graph defined
   here. The graph is deliberately forgiving: classes can be created before
   their superclasses, relationships can point at classes that do not exist yet,
   and arcs can be resolved later.
2. **Emit** - the finished graph is written out as Django source
   (``bird_data_model.py`` and ``admin.py``) and as the discriminator CSVs the
   ETL generator consumes.

The blueprint mirrors Django concepts rather than an ECore metamodel: a
:class:`ModelClass` becomes a Django model, a :class:`Field` becomes a model
field, a :class:`Relationship` becomes a ``ForeignKey``, and an
:class:`Enumeration` becomes a ``choices`` dictionary. Blueprints are plain
Python objects on purpose - they must never be loaded into Django's app
registry, because the graph is still incomplete while it is being built.

Identity is object identity throughout (no value equality), because the import
keeps maps keyed on these objects and repeatedly asks "is this the same class?".
"""

from __future__ import annotations

from typing import Iterator

#: ``upper_bound`` value meaning "no upper limit", i.e. a to-many relationship.
UNBOUNDED = -1


class Annotation:
    """A named group of key/value details attached to a blueprint element.

    Annotations carry the facts SQL Developer expresses outside the plain
    structure: which long name an element had, whether a relationship is
    identifying, which hierarchy a class belongs to, and which input-layer
    column an attribute was forward-engineered into.
    """

    def __init__(self, source: AnnotationDirective | None = None, details: list[AnnotationDetail] | None = None):
        self.source = source
        self.details: list[AnnotationDetail] = list(details) if details else []

    def add_detail(self, key: str, value: str) -> AnnotationDetail:
        detail = AnnotationDetail(key=key, value=value)
        self.details.append(detail)
        return detail

    def detail_value(self, key: str) -> str | None:
        """Return the last value recorded for ``key``.

        Details accumulate rather than replace - SQL Developer can map the same
        attribute more than once - and the most recent mapping is the one that
        counts.
        """

        found = None
        for detail in self.details:
            if detail.key == key:
                found = detail.value
        return found

    def __repr__(self) -> str:
        source_name = self.source.name if self.source is not None else None
        return f"Annotation(source={source_name!r}, details={len(self.details)})"


class AnnotationDetail:
    """One key/value pair inside an :class:`Annotation`."""

    def __init__(self, key: str = "", value: str = ""):
        self.key = key
        self.value = value

    def __repr__(self) -> str:
        return f"AnnotationDetail(key={self.key!r}, value={self.value!r})"


class AnnotationDirective:
    """A declared annotation kind, such as ``long_name`` or ``il_mapping``."""

    def __init__(self, name: str = "", source_uri: str = ""):
        self.name = name
        self.source_uri = source_uri

    def __repr__(self) -> str:
        return f"AnnotationDirective(name={self.name!r})"


class ModelElement:
    """Anything in the blueprint that has a name and can carry annotations."""

    def __init__(self, name: str = ""):
        self.name = name
        self.annotations: list[Annotation] = []

    def add_annotation(self, annotation: Annotation) -> Annotation:
        self.annotations.append(annotation)
        return annotation

    def annotation_with_source(self, source_name: str) -> Annotation | None:
        """Return the last annotation declared under ``source_name``."""

        found = None
        for annotation in self.annotations:
            if annotation.source is not None and annotation.source.name == source_name:
                found = annotation
        return found

    def annotation_detail(self, source_name: str, key: str) -> str | None:
        annotation = self.annotation_with_source(source_name)
        return annotation.detail_value(key) if annotation is not None else None

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name!r})"


class DataType(ModelElement):
    """A primitive logical type, such as ``String`` or ``Date``."""


class EnumerationValue:
    """One member of an :class:`Enumeration`.

    ``code`` is what is stored (the SQL Developer domain value, e.g. ``"1"``)
    and ``label`` is the readable name it maps to. Together they become one
    entry of a Django ``choices`` dictionary: ``{code: label}``.
    """

    def __init__(self, code: str = "", label: str = "", sequence: int | None = None):
        self.code = code
        self.label = label
        self.sequence = sequence

    def __repr__(self) -> str:
        return f"EnumerationValue(code={self.code!r}, label={self.label!r})"


class Enumeration(DataType):
    """A domain of coded values, emitted as a Django ``choices`` dictionary."""

    def __init__(self, name: str = ""):
        super().__init__(name=name)
        self.values: list[EnumerationValue] = []

    def add_value(self, value: EnumerationValue) -> EnumerationValue:
        self.values.append(value)
        return value

    def contains_code(self, code: str) -> bool:
        return any(value.code.lower() == code.lower() for value in self.values)

    def contains_label(self, label: str) -> bool:
        return any(value.label.lower() == label.lower() for value in self.values)

    def __repr__(self) -> str:
        return f"Enumeration(name={self.name!r}, values={len(self.values)})"


class Member(ModelElement):
    """A field or relationship declared on a :class:`ModelClass`."""

    def __init__(self, name: str = "", lower_bound: int = 0, upper_bound: int = 1):
        super().__init__(name=name)
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        #: The class this member belongs to, set by :meth:`ModelClass.add_member`.
        self.owner: ModelClass | None = None

    @property
    def is_many(self) -> bool:
        return self.upper_bound == UNBOUNDED

    @property
    def is_mandatory(self) -> bool:
        return self.lower_bound > 0


class Field(Member):
    """A plain attribute, emitted as a Django model field.

    ``data_type`` is either a primitive :class:`DataType` or an
    :class:`Enumeration`; an enumeration makes the emitted field a
    ``CharField`` with ``choices``.
    """

    def __init__(
        self,
        name: str = "",
        data_type: DataType | None = None,
        is_identifier: bool = False,
        lower_bound: int = 0,
        upper_bound: int = 1,
    ):
        super().__init__(name=name, lower_bound=lower_bound, upper_bound=upper_bound)
        self.data_type = data_type
        #: True for the generated surrogate key, emitted as ``primary_key=True``.
        self.is_identifier = is_identifier


class Relationship(Member):
    """A link to another class, emitted as a Django ``ForeignKey``.

    Only the to-one side is emitted; the to-many side exists in the blueprint so
    traversal can walk the graph in both directions.
    """

    def __init__(
        self,
        name: str = "",
        target: ModelClass | None = None,
        lower_bound: int = 0,
        upper_bound: int = 1,
        is_containment: bool = False,
    ):
        super().__init__(name=name, lower_bound=lower_bound, upper_bound=upper_bound)
        self.target = target
        self.is_containment = is_containment
        #: The relationship on the other class describing the same link.
        self.opposite: Relationship | None = None

    @property
    def is_delegate(self) -> bool:
        """True for the ``{arc}_delegate`` link that models disjoint subtyping."""

        return self.name.endswith("_delegate")


class ModelClass(ModelElement):
    """One entity, emitted as a Django model class.

    ``superclasses`` keeps a list rather than a single value because SQL
    Developer subtyping and arc membership are recorded independently; the first
    entry is the one Django inheritance is generated from.
    """

    def __init__(self, name: str = "", is_abstract: bool = False):
        super().__init__(name=name)
        self.is_abstract = is_abstract
        self.superclasses: list[ModelClass] = []
        self.members: list[Member] = []
        #: The package this class belongs to, set by :meth:`ModelPackage.add_classifier`.
        self.package: ModelPackage | None = None
        #: The entity name before it was turned into a valid identifier.
        self.original_name = ""
        #: LDM annotation contract input, see specs/BIRD_LDM_ANNOTATIONS_SPEC.md.
        self.entity_metadata: dict = {}
        self.key_metadata: dict = {}

    @property
    def superclass(self) -> ModelClass | None:
        """The class Django inheritance is generated from, if there is one."""

        return self.superclasses[0] if self.superclasses else None

    @property
    def fields(self) -> list[Field]:
        return [member for member in self.members if isinstance(member, Field)]

    @property
    def relationships(self) -> list[Relationship]:
        return [member for member in self.members if isinstance(member, Relationship)]

    def add_member(self, member: Member) -> Member:
        member.owner = self
        self.members.append(member)
        return member

    def remove_member(self, member: Member) -> None:
        self.members.remove(member)
        if member.owner is self:
            member.owner = None

    def add_superclass(self, superclass: ModelClass) -> None:
        self.superclasses.append(superclass)

    def member_named(self, name: str) -> Member | None:
        return next((member for member in self.members if member.name == name), None)

    def __repr__(self) -> str:
        return f"ModelClass(name={self.name!r}, members={len(self.members)})"


class ModelPackage(ModelElement):
    """A named group of classes, enumerations and primitive types.

    The import builds one package for LDM entities, one for LDM domains, and the
    equivalent pair for the input layer.
    """

    def __init__(self, name: str = "", ns_uri: str = "", ns_prefix: str = ""):
        super().__init__(name=name)
        self.ns_uri = ns_uri
        self.ns_prefix = ns_prefix
        self.classifiers: list[ModelElement] = []
        self.annotation_directives: list[AnnotationDirective] = []
        self.imports: list[str] = []

    def add_classifier(self, classifier: ModelElement) -> ModelElement:
        if isinstance(classifier, ModelClass):
            classifier.package = self
        self.classifiers.append(classifier)
        return classifier

    def remove_classifier(self, classifier: ModelElement) -> None:
        self.classifiers.remove(classifier)
        if isinstance(classifier, ModelClass) and classifier.package is self:
            classifier.package = None

    @property
    def model_classes(self) -> Iterator[ModelClass]:
        return (classifier for classifier in self.classifiers if isinstance(classifier, ModelClass))

    @property
    def enumerations(self) -> Iterator[Enumeration]:
        return (classifier for classifier in self.classifiers if isinstance(classifier, Enumeration))

    def annotation_directive(self, name: str) -> AnnotationDirective | None:
        found = None
        for directive in self.annotation_directives:
            if directive.name == name:
                found = directive
        return found

    def class_named(self, name: str) -> ModelClass | None:
        return next((model_class for model_class in self.model_classes if model_class.name == name), None)

    def __repr__(self) -> str:
        return f"ModelPackage(name={self.name!r}, classifiers={len(self.classifiers)})"


def annotate(element: ModelElement, package: ModelPackage | None, source_name: str, key: str, value: str) -> Annotation:
    """Add ``key``/``value`` to ``element`` under the ``source_name`` directive.

    An existing annotation with that source is extended rather than duplicated,
    which is how SQL Developer's repeated input-layer mappings accumulate.
    """

    annotation = element.annotation_with_source(source_name)
    if annotation is None:
        directive = package.annotation_directive(source_name) if package is not None else None
        annotation = element.add_annotation(Annotation(source=directive))
    annotation.add_detail(key, value)
    return annotation
