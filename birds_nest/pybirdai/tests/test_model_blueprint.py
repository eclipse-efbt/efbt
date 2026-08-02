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

import io

from pybirdai.model_blueprint import (
    UNBOUNDED,
    Annotation,
    AnnotationDirective,
    Enumeration,
    EnumerationValue,
    Field,
    ModelClass,
    ModelPackage,
    PrimitiveTypes,
    Relationship,
    annotate,
)
from pybirdai.process_steps.sqldeveloper_import.emit_django_from_blueprint import BlueprintToDjango


def _package_with_directives():
    package = ModelPackage(name="ldm_entities")
    for name in ("long_name", "relationship_type", "il_mapping"):
        package.annotation_directives.append(AnnotationDirective(name=name, source_uri=name))
    return package


def test_adding_a_classifier_and_member_records_the_owner():
    package = _package_with_directives()
    model_class = package.add_classifier(ModelClass(name="ROOT"))
    field = model_class.add_member(Field(name="ROOT_ID", data_type=PrimitiveTypes.e_string))

    assert model_class.package is package
    assert field.owner is model_class
    assert package.class_named("ROOT") is model_class
    assert list(package.model_classes) == [model_class]

    model_class.remove_member(field)
    package.remove_classifier(model_class)

    assert field.owner is None
    assert model_class.package is None
    assert list(package.model_classes) == []


def test_blueprint_identity_is_object_identity_not_value_equality():
    # The import keeps maps keyed on these objects, so two same-named classes
    # must never compare equal.
    assert ModelClass(name="ROOT") != ModelClass(name="ROOT")
    assert Enumeration(name="A_domain") != Enumeration(name="A_domain")


def test_annotate_extends_an_existing_annotation_and_last_detail_wins():
    package = _package_with_directives()
    model_class = package.add_classifier(ModelClass(name="ROOT"))

    annotate(model_class, package, "il_mapping", "il_table", "FIRST_TABLE")
    annotate(model_class, package, "il_mapping", "il_table", "SECOND_TABLE")

    assert len(model_class.annotations) == 1
    assert model_class.annotations[0].source.name == "il_mapping"
    # SQL Developer can map the same element twice; the most recent one counts.
    assert model_class.annotation_detail("il_mapping", "il_table") == "SECOND_TABLE"
    assert model_class.annotation_detail("il_mapping", "missing") is None
    assert model_class.annotation_detail("long_name", "long_name") is None


def test_annotation_with_source_returns_the_most_recent_annotation():
    model_class = ModelClass(name="ROOT")
    first = model_class.add_annotation(Annotation(source=AnnotationDirective(name="long_name")))
    second = model_class.add_annotation(Annotation(source=AnnotationDirective(name="long_name")))

    assert first is not second
    assert model_class.annotation_with_source("long_name") is second


def test_enumeration_uniqueness_checks_are_case_insensitive():
    enumeration = Enumeration(name="STTS_domain")
    enumeration.add_value(EnumerationValue(code="1", label="Active", sequence=1))

    assert enumeration.contains_code("1")
    assert enumeration.contains_label("active")
    assert not enumeration.contains_code("2")
    assert not enumeration.contains_label("Inactive")


def test_delegate_and_cardinality_helpers():
    arc_class = ModelClass(name="INSTRMNT_RL", is_abstract=True)
    delegate = Relationship(name="INSTRMNT_RL_delegate", target=arc_class)
    plain = Relationship(name="OWNS", target=arc_class, upper_bound=UNBOUNDED, lower_bound=1)

    assert delegate.is_delegate
    assert not delegate.is_many
    assert not plain.is_delegate
    assert plain.is_many
    assert plain.is_mandatory


def test_emitting_a_blueprint_writes_ordered_django_source():
    package = _package_with_directives()

    status_domain = Enumeration(name="STTS_domain")
    status_domain.add_value(EnumerationValue(code="1", label="Active", sequence=1))

    # The subclass is added first on purpose: emission must still order the
    # superclass ahead of it so the generated module is valid Python.
    leaf = package.add_classifier(ModelClass(name="LEAF"))
    root = package.add_classifier(ModelClass(name="ROOT"))
    leaf.add_superclass(root)

    root.add_member(Field(name="ROOT_uniqueID", data_type=PrimitiveTypes.e_string, is_identifier=True))
    root.add_member(Field(name="STTS", data_type=status_domain))
    root.add_member(Field(name="AMNT", data_type=PrimitiveTypes.e_int))
    leaf.add_member(Relationship(name="theROOT", target=root))
    # A to-many side exists only so traversal can walk back; it is not emitted.
    leaf.add_member(Relationship(name="theROOTs", target=root, upper_bound=UNBOUNDED))
    annotate(root, package, "long_name", "long_name", "Root_entity")

    output = io.StringIO()
    output.close = lambda: None
    BlueprintToDjango.createDjangoForPackage(None, package, output, context=None)
    generated = output.getvalue()

    assert generated.index("class ROOT(") < generated.index("class LEAF(ROOT):")
    assert "STTS_domain = {\t\t\"1\":\"Active\",\n}" in generated
    assert "ROOT_uniqueID = models.CharField(\"ROOT_uniqueID\",max_length=255, primary_key=True)" in generated
    assert "AMNT = models.BigIntegerField(\"AMNT\"" in generated
    assert "theROOT = models.ForeignKey(\"ROOT\"" in generated
    assert "theROOTs = models.ForeignKey" not in generated
    assert "verbose_name = 'Root_entity'" in generated
    assert "verbose_name = 'LEAF'" in generated


def test_emitting_a_blueprint_writes_the_ldm_annotation_contract():
    package = _package_with_directives()
    model_class = package.add_classifier(ModelClass(name="ROOT"))
    model_class.entity_metadata = {"entity_id": "ENT1", "entity_name": "Root_entity"}
    model_class.key_metadata = {
        "primary_key": ["ROOT_ID"],
        "foreign_keys": [{"relation_id": "REL1", "identifying": "Y", "fields": ["ROOT_ID"]}],
    }

    annotations = BlueprintToDjango.django_annotations(None, model_class)

    # Only contract keys survive, and flags become booleans.
    assert annotations == {
        "ldm": {
            "primary_key": ["ROOT_ID"],
            "foreign_keys": [{"identifying": True, "fields": ["ROOT_ID"]}],
        }
    }


def test_emitting_a_class_without_metadata_writes_no_annotations():
    assert BlueprintToDjango.django_annotations(None, ModelClass(name="ROOT")) is None
