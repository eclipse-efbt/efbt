# coding=UTF-8#
# Copyright (c) 2024 Bird Software Solutions Ltd
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

"""
Convert RegDNA ELPackage objects to Eclipse XCore format.
Based on the XTend snippet pattern for generating XCore files.
"""

import os
from pybirdai.regdna import ELPackage, ELClass, ELAttribute, ELReference, ELEnum, ELDataType, ELOperation
from pybirdai.context.context import Context


def get_cardinality_string(element):
    """
    Generate cardinality string based on lower and upper bounds.
    Returns empty string for default cardinality [0..1], otherwise returns [lower..upper].
    """
    if element.upperBound == -1:
        return "[] "
    elif not ((element.lowerBound == 0) and ((element.upperBound == 1) or (element.upperBound == 0))):
        return f"[{element.lowerBound}..{element.upperBound}]"
    return ""


def convert_annotation_details(annotation):
    """Convert annotation details to XCore format."""
    details = []
    for detail in annotation.details:
        details.append(f'{detail.key}="{detail.value}"')
    return f'@{annotation.source.name} ({", ".join(details)})'


def convert_attribute(attribute):
    """Convert ELAttribute to XCore format."""
    lines = []
    
    # Add annotations
    for annotation in attribute.eAnnotations:
        lines.append(f'\t{convert_annotation_details(annotation)}')
    
    # Build attribute line
    attr_line = "\t"
    if attribute.iD:
        attr_line += "id "
    
    type_name = attribute.eAttributeType.name if attribute.eAttributeType else attribute.eType.name
    cardinality = get_cardinality_string(attribute)
    attr_line += f"{type_name} {cardinality}{attribute.name}"
    
    lines.append(attr_line)
    return lines


def convert_reference(reference):
    """Convert ELReference to XCore format."""
    lines = []
    
    is_identifying_realtionship = False
    # Add annotations
    for annotation in reference.eAnnotations:
        lines.append(f'\t{convert_annotation_details(annotation)}')
        details = []
        for detail in annotation.details:
            if detail.key  == 'is_identifying_relationship' and detail.value == 'true':
                is_identifying_realtionship = True
            
    
    # Build reference line
    ref_line = "\t"
    if reference.containment:
        ref_line += "contains "
    elif is_identifying_realtionship:
        ref_line += "contains "
    else:
        ref_line += "refers "
    
    type_name = reference.eReferenceType.name if reference.eReferenceType else reference.eType.name
    cardinality = get_cardinality_string(reference)
    ref_line += f"{type_name} {cardinality}{reference.name}"
    
    lines.append(ref_line)
    return lines


def convert_operation(operation):
    """Convert ELOperation to XCore format."""
    lines = []
    
    # Add annotations
    for annotation in operation.eAnnotations:
        lines.append(f'\t{convert_annotation_details(annotation)}')
    
    # Build operation line
    op_line = "\top "
    type_name = operation.eType.name if operation.eType else "void"
    cardinality = get_cardinality_string(operation)
    op_line += f"{type_name} {cardinality}{operation.name}()"
    lines.append(op_line)
    lines.append("\t{")
    
    # Add body
    if operation.body:
        lines.append(f"\t\t{operation.body}")
    elif type_name == "double":
        lines.append("\t\treturn 0")
    elif type_name == "int":
        lines.append("\t\treturn 0")
    elif type_name == "boolean":
        lines.append("\t\treturn true")
    
    lines.append("\t}")
    return lines


def convert_class(elclass, package_name):
    """Convert ELClass to XCore format."""
    lines = []
    
    # Add class annotations
    for annotation in elclass.eAnnotations:
        lines.append(convert_annotation_details(annotation))
    
    # Build class declaration
    class_line = ""
    if elclass.eAbstract:
        class_line += "abstract "
    class_line += f"class {elclass.name}"
    
    if elclass.eSuperTypes and len(elclass.eSuperTypes) == 1:
        class_line += f" extends {elclass.eSuperTypes[0].name}"
    
    class_line += " {"
    lines.append(class_line)
    
    # Add structural features (attributes and references)
    for feature in elclass.eStructuralFeatures:
        if isinstance(feature, ELAttribute):
            lines.extend(convert_attribute(feature))
        elif isinstance(feature, ELReference):
            lines.extend(convert_reference(feature))
    
    # Add operations
    for operation in elclass.eOperations:
        lines.extend(convert_operation(operation))
    
    lines.append("}")
    
    # Add table class for non-ldm_entities packages
    if package_name.strip() != "ldm_entities":
        lines.append(f"class {elclass.name}_Table {{")
        lines.append(f"\tcontains {elclass.name} [] {elclass.name}s")
        lines.append("}")
    
    return lines


def convert_enum(elenum):
    """Convert ELEnum to XCore format."""
    literals = []
    for literal in elenum.eLiterals:
        literals.append(f'{literal.name} as "{literal.literal}" = {literal.value}')
    
    return [f"enum {elenum.name} {{ {' '.join(literals)} }}"]


def convert_datatype(datatype):
    """Convert ELDataType to XCore format."""
    if isinstance(datatype, ELEnum):
        return []  # Enums are handled separately
    
    wraps_type = "java.util.Date" if datatype.name == "Date" else datatype.name
    return [f"type {datatype.name} wraps {wraps_type}"]


def convert_package_to_xcore(elpackage):
    """
    Convert an ELPackage to XCore format string.
    
    Args:
        elpackage: ELPackage instance to convert
        
    Returns:
        str: XCore formatted content
    """
    lines = []
    
    # Package declaration
    lines.append(f"package {elpackage.name}")
    lines.append("")
    
    # Imports (skip for types package)
    if elpackage.name.strip() != "types":
        for import_stmt in elpackage.imports:
            if import_stmt.importedNamespace.strip() != "types.*":
                lines.append(f"import {import_stmt.importedNamespace}")
        
        # Annotation directives
        for directive in elpackage.annotationDirectives:
            lines.append(f'annotation "{directive.sourceURI}" as {directive.name}')
        
        if elpackage.imports or elpackage.annotationDirectives:
            lines.append("")
    
    # Classes
    for classifier in elpackage.eClassifiers:
        if isinstance(classifier, ELClass):
            lines.extend(convert_class(classifier, elpackage.name))
            lines.append("")
    
    # Enums
    for classifier in elpackage.eClassifiers:
        if isinstance(classifier, ELEnum):
            lines.extend(convert_enum(classifier))
            lines.append("")
    
    # DataTypes (only for types package based on XTend logic)
    if elpackage.name.strip() == "types":
        for classifier in elpackage.eClassifiers:
            if isinstance(classifier, ELDataType) and not isinstance(classifier, ELEnum):
                lines.extend(convert_datatype(classifier))
                lines.append("")
    
    return "\n".join(lines)


def save_xcore_file(elpackage, output_directory):
    """
    Save ELPackage as XCore file.
    
    Args:
        elpackage: ELPackage instance to convert and save
        output_directory: Directory path where to save the XCore file
    """
    xcore_content = convert_package_to_xcore(elpackage)
    
    # Ensure output directory exists
    os.makedirs(output_directory, exist_ok=True)
    
    # Write XCore file
    output_path = os.path.join(output_directory, f"{elpackage.name}.xcore")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(xcore_content)
    
    print(f"Generated XCore file: {output_path}")


def convert_context_packages_to_xcore(context, output_directory):
    """
    Convert the five main packages from Context to XCore files.
    
    Args:
        context: Context instance containing the packages
        output_directory: Directory path where to save the XCore files
    """
    packages = [
        context.types_package,
        context.ldm_domains_package,
        context.ldm_entities_package,
        context.il_domains_package,
        context.il_tables_package
    ]
    
    for package in packages:
        if package and package.eClassifiers:  # Only process if package has content
            save_xcore_file(package, output_directory)


# Example usage
if __name__ == "__main__":
    # Create a context instance
    context = Context()
    
    # You would populate the context packages here with your RegDNA model data
    # For example:
    # context.types_package = ... (populated ELPackage)
    # context.ldm_domains_package = ... (populated ELPackage)
    # etc.
    
    # Convert to XCore
    output_dir = "results/xcore_output"
    convert_context_packages_to_xcore(context, output_dir)