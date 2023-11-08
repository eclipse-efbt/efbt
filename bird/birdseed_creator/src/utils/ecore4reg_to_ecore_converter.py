# coding=UTF-8#
# Copyright (c) 2020 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDE-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#


'''
Created on 22 Jan 2022

@author: Neil
'''
from pyecore.ecore import EInt, EDouble, EDate, EString, EBoolean
from pyecore.ecore import EPackage, EEnum, EEnumLiteral, EClass
from pyecore.ecore import EAttribute, EReference, EOperation,EAnnotation

from regdna import ELClass, ELAttribute, ELReference, ELEnum




class regdnaToEcoreConverter(object):
    '''
    Documentation for regdnaToEcoreConverter
    '''

    def convert_packages_in_context(self, context):
        '''
        Documentation for convertPackagesInContext
        '''
        context.il_domains_ecore_package = regdnaToEcoreConverter.convert(
            self, context.il_domains_package, context)
        context.sdd_domains_ecore_package = regdnaToEcoreConverter.convert(
            self, context.sdd_domains_package, context)
        context.input_tables_ecore_package = regdnaToEcoreConverter.convert(
            self, context.input_tables_package, context)
        context.output_tables_ecore_package = regdnaToEcoreConverter.convert(
            self, context.output_tables_package, context)

    def convert(self, el_package, context):
        '''
        Documentation for convert
        '''
        ecore_package = EPackage(
            name=el_package.name, nsURI=el_package.nsURI, nsPrefix=el_package.nsPrefix)

        for classifier in el_package.eClassifiers:
            if isinstance(classifier, ELEnum):
                e_enum = EEnum(name=classifier.name)
                for literal in classifier.eLiterals:
                    e_enum_literal = EEnumLiteral(literal.name)
                    e_enum_literal.value = literal.value
                    e_enum_literal.literal = literal.literal
                    e_enum.eLiterals.append(e_enum_literal)
                annotation = classifier.eAnnotations
                if not (annotation is None):
                    e_annotation = EAnnotation()
                    e_annotation.source = annotation.source
                    for detail in annotation.details:
                        key = detail.key
                        value = detail.value
                        e_annotation.details[key] = value
                    e_enum.eAnnotations.append(e_annotation)
                ecore_package.eClassifiers.append(e_enum)

        for classifier in el_package.eClassifiers:
            if isinstance(classifier, ELClass):
                
                    
                e_class = EClass(name=classifier.name)
                annotation = classifier.eAnnotations
                if not (annotation is None):
                    e_annotation = EAnnotation()
                    e_annotation.source = annotation.source
                    for detail in annotation.details:
                        key = detail.key
                        value = detail.value
                        e_annotation.details[key] = value
                    e_class.eAnnotations.append(e_annotation)
                    
                if len(classifier.eSuperTypes) > 0:
                    e_class.superTypeName = classifier.eSuperTypes[0].name
                e_class.abstract = classifier.eAbstract
                for structural_feature in classifier.eStructuralFeatures:

                    if isinstance(structural_feature, ELAttribute):
                        e_attribute = EAttribute(name=structural_feature.name)
                        e_attribute.upperBound = structural_feature.upperBound
                        e_attribute.lowerBound = structural_feature.lowerBound
                        type_name = structural_feature.eType.name
                        if structural_feature.iD:
                            e_attribute.iD = True

                        if isinstance(structural_feature.eAttributeType, ELEnum):
                            e_enum = regdnaToEcoreConverter.find_enum(
                                self, structural_feature.eAttributeType.name, ecore_package, context)
                            e_attribute.eAttributeType = e_enum
                            e_attribute.eType = e_enum
                        elif type_name == 'double':
                            e_attribute.eType = EDouble
                        elif type_name == 'String':
                            e_attribute.eType = EString
                        elif type_name == 'int':
                            e_attribute.eType = EInt
                        elif type_name == 'Date':
                            e_attribute.eType = EDate
                        elif type_name == 'boolean':
                            e_attribute.eType = EBoolean

                        annotation = structural_feature.eAnnotations
                        if not (annotation is None):
                            e_annotation = EAnnotation()
                            e_annotation.source = annotation.source
                            for detail in annotation.details:
                                key = detail.key
                                value = detail.value
                                e_annotation.details[key] = value
                            e_attribute.eAnnotations.append(e_annotation)
                        e_class.eStructuralFeatures.append(e_attribute)

                    if isinstance(structural_feature, ELReference):
                        e_reference = EReference(name=structural_feature.name)
                        e_reference.upperBound = structural_feature.upperBound
                        e_reference.lowerBound = structural_feature.lowerBound
                        e_reference.containment = structural_feature.containment
                        annotation = structural_feature.eAnnotations
                        if not (annotation is None):
                            e_annotation = EAnnotation()
                            e_annotation.source = annotation.source
                            for detail in annotation.details:
                                key = detail.key
                                value = detail.value
                                e_annotation.details[key] = value
                            e_reference.eAnnotations.append(e_annotation)
                        e_class.eStructuralFeatures.append(e_reference)
                        type_name = structural_feature.eType.name
                        e_reference.typeName = type_name

                for operation in classifier.eOperations:
                    e_operation = EOperation(name=operation.name)
                    e_class.eOperations.append(e_operation)
                    type_name = operation.eType.name
                    e_operation.typeName = type_name

                ecore_package.eClassifiers.append(e_class)

        # set the types for refernces, supercalss, and operations
        for classifier in ecore_package.eClassifiers:
            if isinstance(classifier, EClass):
                if hasattr(classifier, 'superTypeName'):
                    classifier.eSuperTypes.append(regdnaToEcoreConverter.find_eclass(
                        self, classifier.superTypeName, ecore_package))

                for structural_feature in classifier.eStructuralFeatures:
                    if isinstance(structural_feature, EReference):
                        the_class = regdnaToEcoreConverter.find_eclass(
                            self, structural_feature.typeName, ecore_package)
                        structural_feature.eType = the_class
                        structural_feature.eReferenceType = the_class
                for operation in classifier.eOperations:
                    the_class = regdnaToEcoreConverter.find_eclass(
                        self, operation.typeName, ecore_package)
                    if not (the_class is None):
                        operation.eType = the_class
                    elif operation.typeName == 'double':
                        operation.eType = EDouble
                    elif operation.typeName == 'String':
                        operation.eType = EString
                    elif operation.typeName == 'int':
                        operation.eType = EInt
                    elif operation.typeName == 'Date':
                        operation.eType = EDate
                    elif operation.typeName == 'boolean':
                        operation.eType = EBoolean
                    else:
                        e_enum = regdnaToEcoreConverter.find_enum(
                            self, operation.typeName, ecore_package, context)
                        operation.eType = e_enum

        return ecore_package

    def find_eclass(self, supertype_name, ecore_package):
        '''
        Documentation for findEClass
        '''
        return_eclass = None
        for classifier in ecore_package.eClassifiers:
            if isinstance(classifier, EClass):
                if classifier.name == supertype_name:
                    return_eclass = classifier

        return return_eclass

    def find_enum(self, type_name, ecore_package, context):
        '''
        Documentation for findEnum
        '''
        return_enum = None

        if (ecore_package.name == 'input_tables') and not (context.load_eil_from_website):
            for classifier in context.il_domains_ecore_package.eClassifiers:
                if isinstance(classifier, EEnum):
                    if classifier.name == type_name:
                        return_enum = classifier

        if (ecore_package.name == 'input_tables') and context.load_eil_from_website:
            for classifier in context.sdd_domains_ecore_package.eClassifiers:
                if isinstance(classifier, EEnum):
                    if classifier.name == type_name:
                        return_enum = classifier 
                        

        return return_enum
