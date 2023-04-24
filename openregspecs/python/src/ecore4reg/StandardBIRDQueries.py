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
from pyecore.ecore import *
import os
    
class StandardBIRDQueries(object):
    
   
    inputLayerEnumsModel = None
    inputLayerEntitiesModel = None
    outputLayerEnumsModel = None
    outputLayerEntitiesModel = None
    typesModel = None
    
    def query1(self,fileDirectory): 
        rset = ResourceSet()
        
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
        if isinstance(attributeType,EEnum):
            attributeAllowedValues = firstAttribute.eType.eLiterals
            print(attributeAllowedValues)
              
        #StandardBIRDQueries.createInputLayerToOutputLayerMatches(self,fileDirectory)
        rset2 = ResourceSet()
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
        
  
if __name__ == '__main__':
    fileDirectory = 'C:\\Users\\LENOVO\\freebirdtools-develop-mar23\\git\\efbt\\ecore4reg\\python\\results'
    standardBIRDQueries = StandardBIRDQueries()
    standardBIRDQueries.query1(fileDirectory)
    