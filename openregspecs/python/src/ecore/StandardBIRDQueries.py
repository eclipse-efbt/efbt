from pyecore.resources import ResourceSet, URI
from pyecore import *
import os




        
        
class StandardBIRDQueries(object):
    
   
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
        
        print("\n\n package Name")
        print(self.inputLayerEntitiesModel.name)
        
        print("\n\n All Classifiers - entities and domains")
        entitiesAndDomains = self.inputLayerEntitiesModel.eClassifiers
        print(entitiesAndDomains)
        
        print("\n\n First Classifiers - An Entity")
        anEntity = entitiesAndDomains[0]
        print(anEntity.name)
        
        print("\n\n An Entities attributes")
        theAttributes = anEntity.eAttributes
        print(theAttributes)
        
        print("\n\n  An Entities first attribute ")
        firstAttribute = theAttributes[0]
        print(firstAttribute.name)
        
        print("\n\n  the  attribute type ")
        attributeType = firstAttribute.eType
        print(attributeType.name)
        
        print("\n\n  the  attribute allowed Values ")
        attributeAllowedValues = firstAttribute.eType.eLiterals
        print(attributeAllowedValues)
              
        
        rset2 = ResourceSet()
        types_resource2 = rset2.create_resource(URI(fileDirectory + os.sep + "types2.ecore"))  # This will create an XMI resource
        types_resource2.append(self.typesModel)  # we add the EPackage instance in the resource
        types_resource2.save()  # we then serialize it
        inputLayerEnums_resource2 = rset2.create_resource(URI(fileDirectory + os.sep + "inputLayerEnums2.ecore"))  # This will create an XMI resource
        inputLayerEnums_resource2.append(self.inputLayerEnumsModel)  # we add the EPackage instance in the resource
        inputLayerEnums_resource2.save()
        outputLayerEnums_resource2 = rset2.create_resource(URI(fileDirectory + os.sep + "outputLayerEnums2.ecore"))  # This will create an XMI resource
        outputLayerEnums_resource2.append(self.outputLayerEnumsModel)  # we add the EPackage instance in the resource
        outputLayerEnums_resource2.save()
        inputLayerEntities_resource2 = rset2.create_resource(URI(fileDirectory + os.sep + "inputLayerEntities2.ecore"))  # This will create an XMI resource
        inputLayerEntities_resource2.append(self.inputLayerEntitiesModel)  # we add the EPackage instance in the resource
        inputLayerEntities_resource2.save()
        outputLayerEntities_resource2 = rset2.create_resource(URI(fileDirectory + os.sep + "outputLayerEntities2.ecore"))  # This will create an XMI resource
        outputLayerEntities_resource2.append(self.outputLayerEntitiesModel)  # we add the EPackage instance in the resource
        outputLayerEntities_resource2.save()
        
    
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

        f = open(fileDirectory + os.sep + 'matches.csv', "a",  encoding='utf-8') 
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
    fileDirectory = 'C:\\Users\\LENOVO\\freebirdtools-develop-dec22\\git\\efbt\\openregspecs\\python\\resources'
    standardBIRDQueries = StandardBIRDQueries()
    standardBIRDQueries.query1(fileDirectory)
    