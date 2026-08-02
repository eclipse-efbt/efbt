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
"""The Django-shaped intermediate the SQL Developer import builds and emits from."""

from pybirdai.model_blueprint.blueprint import (
    UNBOUNDED,
    Annotation,
    AnnotationDetail,
    AnnotationDirective,
    DataType,
    Enumeration,
    EnumerationValue,
    Field,
    Member,
    ModelClass,
    ModelElement,
    ModelPackage,
    Relationship,
    annotate,
)
from pybirdai.model_blueprint.primitive_types import PrimitiveTypes

__all__ = [
    "UNBOUNDED",
    "Annotation",
    "AnnotationDetail",
    "AnnotationDirective",
    "DataType",
    "Enumeration",
    "EnumerationValue",
    "Field",
    "Member",
    "ModelClass",
    "ModelElement",
    "ModelPackage",
    "PrimitiveTypes",
    "Relationship",
    "annotate",
]
