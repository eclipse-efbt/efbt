from pyecore.resources import ResourceSet, URI
from pyecore import *
import os





        
        
class StandardMatchingQueries(object):
    
   
    inputLayerEnumsModel = None
    inputLayerEntitiesModel = None
    outputLayerEnumsModel = None
    outputLayerEntitiesModel = None
    typesModel = None
    
    def query1(self,fileDirectory): 
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
        for outputLayer in outputLayers:
            outputLayerName = outputLayer.name
            print("outputLayerName")
            print(outputLayerName)
            
            for attribute in outputLayer.eOperations:
                attributeType = attribute.eType
                atributeTypeName = attributeType.name
                attributeName = attribute.name
                if not(StandardMatchingQueries.inBlacklist(self,attributeName)):
                    print("atributeTypeName")
                    print(atributeTypeName)
                    indexOfISSUBDMAOINOF = atributeTypeName.index('_ISSUBDOMAINOF_')
                    length = len(atributeTypeName)
                    domainName = atributeTypeName[indexOfISSUBDMAOINOF + 15:length]
                    print("domainName")
                    print(domainName)
                    relatedInputLayerDomain = StandardMatchingQueries.getRelatedInputLayerDomain(self,domainName,attributeType)
                    print("relatedInputLayerDomain")
                    print(relatedInputLayerDomain)
                    csvTextString = None
                    if not( relatedInputLayerDomain is None):
                        for literal in attributeType.eLiterals:
                            literalName = literal.name
                            relatedLiteral = StandardMatchingQueries.getRelatedInputLayerLiteral(self,literal,relatedInputLayerDomain)
                            relatedLiteralName = "None"
                            if not (relatedLiteral is None):
                                relatedLiteralName = relatedLiteral.name
                            csvTextString = outputLayerName + "," + attributeName + "," +  atributeTypeName  + "," + domainName + "," +relatedInputLayerDomain.name + "," + literalName +"," +relatedLiteralName
                            csvStrings.append(csvTextString)
                    else:
                        csvTextString = outputLayerName + "," + attributeName + "," + atributeTypeName + "," + domainName + ",None,,"
                        csvStrings.append(csvTextString)

        f = open(fileDirectory + os.sep + 'matches.csv', "a",  encoding='utf-8') 
        for theString in csvStrings:
             f.write(theString)
             f.write('\n') 
        f.close()
               
    def inBlacklist(self,theString):
        blacklist = False
        if(theString.strip() == 'HLD_TRDNG_INDCTR'):
            blacklist = True 
        if(theString.strip() == 'HLD_MTRTY_INDCTR'):
            blacklist = True
        if(theString.strip() == 'ACCNTNG_PRTFL_VLTN_RL'):
            blacklist = True
        if(theString.strip() == 'ACCNTNG_FRMWRK'):
            blacklist = True
        if(theString.strip() == 'SBJCT_IMPRMNT_INDCTR'):
            blacklist = True
        if(theString.strip() == 'DT_RFRNC'):
            blacklist = True
        if(theString.strip() == 'VALUE_DECIMAL'):
            blacklist = True
        if(theString.strip() == 'ENTTY_RIAD_CD_RPRTNG_AGNT'):
            blacklist = True
        if(theString.strip() == 'MTRCS'):
            blacklist = True
        if(theString.strip() == 'SUBA_CD'):
            blacklist = True
            
        return blacklist
            
                
    def getRelatedInputLayerLiteral(self,literal,relatedInputLayerDomain):
        returnLiteral = None
        for theLiteral in relatedInputLayerDomain.eLiterals:
            if theLiteral.name.lower() == literal.name.lower():
                returnLiteral = theLiteral
                
        if returnLiteral is None:
            for theLiteral in relatedInputLayerDomain.eLiterals:
                if literal.name.lower() in theLiteral.name.lower():
                    returnLiteral = theLiteral
                    
        return returnLiteral
        
        
    def getRelatedInputLayerDomain(self,theDomainName,attributeType):
        classifiers = self.inputLayerEntitiesModel.eClassifiers
        returnDomain = None
        domainName=theDomainName.lower()
        for classifier in classifiers:
            if classifier.name.endswith("_domain") and not ("ISSUBDOMAINOF" in classifier.name):
                indexOfPostfix = classifier.name.index("_domain")
                domainPrefix = classifier.name[0:indexOfPostfix].lower()
                
                if domainPrefix == domainName:
                    returnDomain = classifier
                elif domainPrefix == domainName + "_Input_Layer_":
                    returnDomain = classifier
                elif domainPrefix == domainName + "_IL_":
                    returnDomain = classifier
                elif domainPrefix == domainName + "_indicator":
                    returnDomain = classifier
                elif domainPrefix == domainName + "_indicator" + "_Input_Layer_":
                    returnDomain = classifier
                elif domainPrefix.endswith("_domain") and  domainPrefix[0:len(domainPrefix) -7] == domainName + "_indicator" + "_Input_Layer_":
                    returnDomain = classifier
        
        if returnDomain == None:
            returnDomain = StandardMatchingQueries.getEnumWithSimilarMembers(self,attributeType)
        return returnDomain
        
    def getEnumWithSimilarMembers(self,attributeType):
    
        enumListLength = len(attributeType.eLiterals)
        enum1Name = "NOTEXISTS!"
        enum2Name = "NOTEXISTS!"
        if(enumListLength > 2):
            enum1Name = attributeType.eLiterals[2].name
        if(enumListLength > 3):
            enum2Name = attributeType.eLiterals[3].name
        
        returnEnum =  None
        enum1Exists = False
        enum2Exists = False
        
        classifiers = self.inputLayerEnumsModel.eClassifiers
        for classifier in classifiers:
            if classifier.name.endswith("_domain") and not ("ISSUBDOMAINOF" in classifier.name):
                for literal in classifier.eLiterals:
                    if literal.name == enum1Name:
                       enum1Exists = True
                    if literal.name == enum2Name:
                       enum2Exists = True
                if enum1Exists and enum2Exists:
                    returnEnum = classifier
                    break
        
        return returnEnum

        
    def getOutputLayers(self): 
        classifiers = self.outputLayerEntitiesModel.eClassifiers
        outputLayers = []
        for classifier in classifiers:
            if classifier.name.endswith("_OutputItem"):
                outputLayers.append(classifier)
        
        return outputLayers
    
  
    
if __name__ == '__main__':
    fileDirectory = '/workspaces/efbt/openregspecs/python/results'
    standardMatchingQueries = StandardMatchingQueries()
    standardMatchingQueries.query1(fileDirectory)
    