# coding=UTF-8#
# Copyright (c) 2020 Bird Software Solutions Ltd
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
from pyecore.resources import ResourceSet, URI
from pyecore import *
import os
import csv
       
        
class StandardMatchingQueries(object):
    
   
    inputLayerEnumsModel = None
    inputLayerEntitiesModel = None
    outputLayerEnumsModel = None
    outputLayerEntitiesModel = None
    typesModel = None
    
    def query1(self,fileDirectory,inScopeFileDirectory): 
        rset = ResourceSet()
        
        types_resource = rset.get_resource(URI(fileDirectory + os.sep + "types.ecore"))
        types_root = types_resource.contents[0]
        rset.metamodel_registry[types_root.nsURI] = types_root
        self.typesModel = types_root
        
        inputLayerEnums_resource = rset.get_resource(URI(fileDirectory + os.sep + "input_layer_enums.ecore"))
        inputLayerEnums_root = inputLayerEnums_resource.contents[0]
        rset.metamodel_registry[inputLayerEnums_root.nsURI] = inputLayerEnums_root
        self.inputLayerEnumsModel = inputLayerEnums_root
        
        outputLayerEnums_resource = rset.get_resource(URI(fileDirectory + os.sep + "output_layer_enums.ecore"))
        outputLayerEnums_root = outputLayerEnums_resource.contents[0]
        rset.metamodel_registry[outputLayerEnums_root.nsURI] = outputLayerEnums_root
        self.outputLayerEnumsModel = outputLayerEnums_root
        
        inputLayerEntities_resource = rset.get_resource(URI(fileDirectory + os.sep + "input_layer_entities.ecore"))
        inputLayerEntities_root = inputLayerEntities_resource.contents[0]
        rset.metamodel_registry[inputLayerEntities_root.nsURI] = inputLayerEntities_root
        self.inputLayerEntitiesModel = inputLayerEntities_root
        
        outputLayerEntities_resource = rset.get_resource(URI(fileDirectory + os.sep + "output_layer_entities.ecore"))
        outputLayerEntities_root = outputLayerEntities_resource.contents[0]
        rset.metamodel_registry[outputLayerEntities_root.nsURI] = outputLayerEntities_root
        self.outputLayerEntitiesModel = outputLayerEntities_root

        StandardMatchingQueries.createInputLayerToOutputLayerMatches(self,fileDirectory)
        
        
    
    def createInputLayerToOutputLayerMatches(self,fileDirectory): 
        outputLayers = StandardMatchingQueries.getOutputLayers(self)
        csvStrings = []
        headerString = "outputLayer,Variable,SubDomain,Domain,MatchingAttribute,MatchingAttributeILDomain,MatchingAttributeEntity,AttributeMatchType,SubDomainMember,InputLayerDomainMember,MemberMatchType"
        csvStrings.append(headerString)
        for outputLayer in outputLayers:
            outputLayerName = outputLayer.name
            print("outputLayerName")
            print(outputLayerName)
            if StandardMatchingQueries.isReportInScope(self,outputLayerName,inScopeFileDirectory):
                for attribute in outputLayer.eOperations:
                    attributeType = attribute.eType
                    atributeTypeName = attributeType.name
                    attributeName = attribute.name
                    print("attributeName")
                    print(attributeName)
                    if not(StandardMatchingQueries.inExcludedList(self,attributeName)):
                        print("atributeTypeName")
                        print(atributeTypeName)
                        domainName = None
                        try:
                            indexOfISSUBDMAOINOF = atributeTypeName.index('_ISSUBDOMAINOF_')
                            length = len(atributeTypeName)
                            domainName = atributeTypeName[indexOfISSUBDMAOINOF + 15:length]
                        except:
                            domainName =atributeTypeName
                        print("domainName")
                        print(domainName)
                        
                        relatedInputLayerAttributes = StandardMatchingQueries.getRelatedInputLayerAttributes(self,attributeName,attributeType)
                        
                        if  len(relatedInputLayerAttributes) > 0:
                            for relatedInputLayerAttribute in relatedInputLayerAttributes:
                                print(relatedInputLayerAttribute)
                                print("relatedInputLayerAttribute")
                                relatedInputLayerDomain = relatedInputLayerAttribute.eType
                                print("relatedInputLayerDomain")
                                print(relatedInputLayerDomain)
                                attributeMatchType = relatedInputLayerAttribute.matchType
                                if(hasattr( relatedInputLayerAttribute, "subStringGuess")):
                                   subStringGuess = "SUBSTRING"
                                   
                                csvTextString = None
                                if hasattr(attributeType, "eLiterals"):
                                    for literal in attributeType.eLiterals:
                                        literalName = literal.name
                                        literalMatchType = "NONE"
                                        
                                        relatedLiteral = StandardMatchingQueries.getRelatedInputLayerLiteral(self,literal,relatedInputLayerDomain)
                                        relatedLiteralName = "None"
                                        if not (relatedLiteral is None):
                                            relatedLiteralName = relatedLiteral.name
                                            literalMatchType = relatedLiteral.matchType
                                        csvTextString = outputLayerName + "," + attributeName + "," +  atributeTypeName  + "," + domainName + "," +relatedInputLayerAttribute.name + "," +relatedInputLayerAttribute.eType.name+ "," +relatedInputLayerAttribute.eContainer().name + "," + attributeMatchType +","+ literalName +"," +relatedLiteralName + "," + literalMatchType
                                        csvStrings.append(csvTextString)
                                else:
                                    csvTextString = outputLayerName + "," + attributeName + "," + atributeTypeName + "," + domainName + "," + relatedInputLayerAttribute.name + "," +relatedInputLayerAttribute.eType.name + "," +relatedInputLayerAttribute.eContainer().name +"," +attributeMatchType  +",,,"
                                    csvStrings.append(csvTextString)
                        else:
                            csvTextString = outputLayerName + "," + attributeName + "," + atributeTypeName + "," + domainName + ",,NONE,,,,,"
                            csvStrings.append(csvTextString)
    
        f = open(fileDirectory + os.sep + 'matches.csv', "a",  encoding='utf-8') 
        for theString in csvStrings:
             f.write(theString)
             f.write('\n') 
        f.close()
               
    def inExcludedList(self,theString):
        excludedList = False
        if(theString.strip() == 'Held_for_trading_indicator'):
            excludedList = True 
        if(theString.strip() == 'Held_to_maturity_indicator'):
            excludedList = True
        if(theString.strip() == 'Accounting_portfolio_valuation_rules'):
            excludedList = True
        if(theString.strip() == 'Accounting_framework'):
            excludedList = True
        if(theString.strip() == 'Subject_to_impairment_indicator'):
            excludedList = True
        if(theString.strip() == 'Reference_date'):
            excludedList = True
        if(theString.strip() == 'Value_Decimal'):
            excludedList = True
        if(theString.strip() == 'The_RIAD_code_of_the_reporting_agent'):
            excludedList = True
        if(theString.strip() == 'Observation_value'):
            excludedList = True
        if(theString.strip() == 'SUBA_Reporting_Identifier'):
            excludedList = True
            
        return excludedList
            
                
    def getRelatedInputLayerLiteral(self,literal,relatedInputLayerDomain):
        returnLiteral = None
        if(hasattr(relatedInputLayerDomain, "eLiterals")):
            for theLiteral in relatedInputLayerDomain.eLiterals:
                if theLiteral.name.lower() == literal.name.lower():
                    returnLiteral = theLiteral
                    theLiteral.matchType ="EXACT"
                    
            if returnLiteral is None:
                for theLiteral in relatedInputLayerDomain.eLiterals:
                    if literal.name.lower() == theLiteral.name.lower()[0:len(literal.name)]:
                        returnLiteral = theLiteral
                        returnLiteral.matchType ="STARTSWITH"
                        
            if returnLiteral is None:
                for theLiteral in relatedInputLayerDomain.eLiterals:
                    if literal.name.lower() in theLiteral.name.lower():
                        returnLiteral = theLiteral
                        returnLiteral.matchType ="SUBSTRING"
                    
        return returnLiteral
        
    def getRelatedInputLayerAttributes(self,theAttributeName,attributeType):
        classifiers = self.inputLayerEntitiesModel.eClassifiers
        returnAttributes = []
        returnAttribute = None
        for classifier in classifiers:
            attributes = classifier.eAttributes
            for attribute in attributes:
                attributeName = attribute.name.lower()
                if (theAttributeName.lower() == attributeName):
                    attribute.matchType = "EXACT"
                    returnAttributes.append(attribute)
                elif theAttributeName in attributeName:
                    attribute.matchType = "SUBSTRING"
                    returnAttributes.append(attribute)
                    
                    
                
        return returnAttributes
                    

        
    def getOutputLayers(self): 
        classifiers = self.outputLayerEntitiesModel.eClassifiers
        outputLayers = []
        for classifier in classifiers:
            if classifier.name.endswith("_OutputItem"):
                outputLayers.append(classifier)
        
        return outputLayers
    
    def isReportInScope(self,classname, inScopeFileDirectory):
        fileLocation = inScopeFileDirectory + os.sep + "in_scope_reports.csv"
        cutClassName = classname
        
        try:
            print("classname")
            print(classname)
            theindex = classname.index('_REF_OutputItem') 
            print("theindex")
            print(theindex)
            cutClassName = classname[0:theindex]
        except:
            print("hit exception")
            cutClassName = classname
        
        headerSkipped = False
        # Load all the entities from the csv file, make an XClass per entity,
        # and add the XClass to the package
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not headerSkipped):
                    headerSkipped = True
                else:
                    reportTemplate = row[0];
                    if reportTemplate == cutClassName:
                        return True
                    
                    
        print("report Is Out Of Scope") 
        print(classname)          
        return False    
    
if __name__ == '__main__':
    
    fileDirectory = '/workspaces/efbt/openregspecs/python/results'
    inScopeFileDirectory = '/workspaces/efbt/openregspecs/python/resources'
    
    
    standardMatchingQueries = StandardMatchingQueries()
    standardMatchingQueries.query1(fileDirectory,inScopeFileDirectory)
    