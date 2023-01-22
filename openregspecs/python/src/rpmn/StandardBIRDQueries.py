from pyecore.resources import ResourceSet, URI
from pyecore import *




        
        
class StandardBIRDQueries(object):
    
   
    birdModel = None
    
    def query1(self,fileDirectory): 
        rset = ResourceSet()
        resource = rset.get_resource(URI(fileDirectory + "\\bird.ecore"))
        mm_root = resource.contents[0]
        rset.metamodel_registry[mm_root.nsURI] = mm_root
        print(mm_root.eClassifiers[0])
        self.birdModel = mm_root 
        StandardBIRDQueries.createInputLayerToOutputLayerMatches(self,fileDirectory)
    
    def createInputLayerToOutputLayerMatches(self,fileDirectory): 
        outputLayers = StandardBIRDQueries.getOutputLayers(self)
        csvStrings = []
        for outputLayer in outputLayers:
            outputLayerName = outputLayer.name
            print("outputLayerName")
            print(outputLayerName)
            
            for attribute in outputLayer.eOperations:
                attributeType = attribute.eType
                atributeTypeName = attributeType.name
                attributeName = attribute.name
                print("atributeTypeName")
                print(atributeTypeName)
                indexOfISSUBDMAOINOF = atributeTypeName.index('_ISSUBDOMAINOF_')
                length = len(atributeTypeName)
                domainName = atributeTypeName[indexOfISSUBDMAOINOF + 15:length]
                print("domainName")
                print(domainName)
                relatedInputLayerDomain = StandardBIRDQueries.getRelatedInputLayerDomain(self,domainName,attributeType)
                print("relatedInputLayerDomain")
                print(relatedInputLayerDomain)
                csvTextString = None
                if not( relatedInputLayerDomain is None):
                    for literal in attributeType.eLiterals:
                        literalName = literal.name
                        relatedLiteral = StandardBIRDQueries.getRelatedInputLayerLiteral(self,literal,relatedInputLayerDomain)
                        relatedLiteralName = "None"
                        if not (relatedLiteral is None):
                            relatedLiteralName = relatedLiteral.name
                        csvTextString = outputLayerName + "," + attributeName + "," +  atributeTypeName  + "," + domainName + "," +relatedInputLayerDomain.name + "," + literalName +"," +relatedLiteralName
                        csvStrings.append(csvTextString)
                else:
                    csvTextString = outputLayerName + "," + attributeName + "," + atributeTypeName + "," + domainName + ",None,,"
                    csvStrings.append(csvTextString)

        f = open(fileDirectory + '\\matches.csv', "a",  encoding='utf-8') 
        for theString in csvStrings:
             f.write(theString)
             f.write('\n') 
        f.close()
                        
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
        classifiers = self.birdModel.eClassifiers
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
            returnDomain = StandardBIRDQueries.getEnumWithSimilarMembers(self,attributeType)
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
        
        classifiers = self.birdModel.eClassifiers
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
        classifiers = self.birdModel.eClassifiers
        outputLayers = []
        for classifier in classifiers:
            if classifier.name.endswith("_OutputItem"):
                outputLayers.append(classifier)
        
        return outputLayers
    
if __name__ == '__main__':
    fileDirectory = 'C:\\Users\\LENOVO\\freebirdtools-develop-dec22\\git\\efbt\\openregspecs\\python\\resources'
    standardBIRDQueries = StandardBIRDQueries()
    standardBIRDQueries.query1(fileDirectory)
    