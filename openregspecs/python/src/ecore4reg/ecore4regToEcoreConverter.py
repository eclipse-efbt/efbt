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
from pickle import TRUE

'''
Created on 22 Jan 2022

@author: Neil
'''
from ecore4reg import *
from pyecore.ecore import *

from context import Context


class Ecore4regToEcoreConverter(object):
    
    def convertPackagesInContext(self,context):
        context.inputLayerEnumsEcorePackage = Ecore4regToEcoreConverter.convert(self,context.inputLayerEnumsPackage,context)
        context.outputLayerEnumsEcorePackage = Ecore4regToEcoreConverter.convert(self,context.outputLayerEnumsPackage,context)
        context.inputLayerEntitiesEcorePackage = Ecore4regToEcoreConverter.convert(self,context.inputLayerEntitiesPackage,context)
        context.outputLayerEntitiesEcorePackage = Ecore4regToEcoreConverter.convert(self,context.outputLayerEntitiesPackage,context)
        
    def convert(self,elPackage,context):
        ecorePackage = EPackage(name=elPackage.name, nsURI=elPackage.nsURI, nsPrefix=elPackage.nsPrefix)
        for classifier in elPackage.eClassifiers:
            if isinstance(classifier,ELEnum):
                eEnum = EEnum(name = classifier.name )
                for literal in classifier.eLiterals:
                    eEnumLiteral = EEnumLiteral(literal.name)
                    eEnumLiteral.value = literal.value
                    eEnumLiteral.literal = literal.literal
                    eEnum.eLiterals.append(eEnumLiteral)
                ecorePackage.eClassifiers.append(eEnum)
        
        for classifier in elPackage.eClassifiers:    
            if isinstance(classifier,ELClass):
                eClass = EClass(name=classifier.name)
                if len(classifier.eSuperTypes) > 0:
                    eClass.superTypeName = classifier.eSuperTypes[0].name
                eClass.abstract = classifier.abstract
                for structuralFeature in classifier.eStructuralFeatures:
                    
                    if isinstance(structuralFeature,ELAttribute):
                        eAttribute = EAttribute(name=structuralFeature.name)
                        eAttribute.upperBound = structuralFeature.upperBound
                        eAttribute.lowerBound = structuralFeature.lowerBound
                        print(eAttribute.upperBound)
                        print(structuralFeature.name)
                        typeName = structuralFeature.eAttributeType.name
                        print(typeName)
                        if isinstance(structuralFeature.eAttributeType,ELEnum):
                            eEnum = Ecore4regToEcoreConverter.findEnum(self,typeName,ecorePackage,context)
                            eAttribute.eAttributeType = eEnum
                            eAttribute.eType = eEnum
                        elif typeName == 'double':
                            eAttribute.eType = EDouble
                        elif typeName == 'String':
                            eAttribute.eType = EString
                        elif typeName == 'int':
                            eAttribute.eType = EInt
                        elif typeName == 'Date':
                            eAttribute.eType = EDate
                        elif typeName == 'boolean':
                            eAttribute.eType = EBoolean
                            
                        eClass.eStructuralFeatures.append(eAttribute)
                        
                    if isinstance(structuralFeature,ELReference):
                        eReference=EReference(name=structuralFeature.name)
                        eReference.upperBound = structuralFeature.upperBound
                        eReference.lowerBound = structuralFeature.lowerBound
                        eReference.containment = structuralFeature.containment
                        eClass.eStructuralFeatures.append(eReference)
                        typeName = structuralFeature.eType.name
                        eReference.typeName = typeName 
                        
                for operation in classifier.eOperations:
                    eOperation = EOperation(name = operation.name) 
                    eClass.eOperations.append(eOperation) 
                    typeName = operation.eType.name
                    eOperation.typeName = typeName 
                                
                ecorePackage.eClassifiers.append(eClass)
                
                
        #set the types for refernces, supercalss, and operations   
        for classifier in ecorePackage.eClassifiers:    
            if isinstance(classifier,EClass):
                if hasattr(classifier, 'superTypeName'):
                    classifier.eSuperTypes.append(Ecore4regToEcoreConverter.findEClass(self,classifier.superTypeName,ecorePackage))
                
                for structuralFeature in classifier.eStructuralFeatures:
                    if isinstance(structuralFeature,EReference):
                        theClass = Ecore4regToEcoreConverter.findEClass(self,structuralFeature.typeName,ecorePackage)
                        structuralFeature.eType = theClass
                        structuralFeature.eReferenceType =theClass
                for operation in classifier.eOperations:
                    theClass = Ecore4regToEcoreConverter.findEClass(self,operation.typeName,ecorePackage)
                    if not (theClass is None):
                        operation.eType = theClass                    
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
                        eEnum = Ecore4regToEcoreConverter.findEnum(self,operation.typeName,ecorePackage,context)
                        operation.eType = eEnum
                        

        return ecorePackage
    
    def findEClass(self,superTypeName,ecorePackage):
        returnEClass = None
        for classifier in ecorePackage.eClassifiers:    
            if isinstance(classifier,EClass):
                if classifier.name == superTypeName:
                    returnEClass = classifier
                    
        return returnEClass
                
    
    def findEnum(self,typeName,ecorePackage,context):
        returnEnum = None
        
        if ecorePackage.name == 'input_layer_entities':
            for classifier in context.inputLayerEnumsEcorePackage.eClassifiers:
                if isinstance(classifier,EEnum):
                    if classifier.name == typeName:
                        returnEnum = classifier
                        
        if ecorePackage.name == 'output_layer_entities':
            for classifier in context.outputLayerEnumsEcorePackage.eClassifiers:
                if isinstance(classifier,EEnum):
                    if classifier.name == typeName:
                        returnEnum = classifier
                    
        #if returnEnum == None:
        #    returnEnum = EEnum(name=typeName)
        return returnEnum
                    
        
        