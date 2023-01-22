
from pyecore.resources import ResourceSet, URI
from pyecore.ecore import *
from pyecore.resources.xmi import XMIResource
from pyecore.resources.xmi import XMIOptions
from Utils import Utils
from context import Context
from collections import Counter

class PersistToFile:
    def saveModelAsHumanReadable(self,context ):
        PersistToFile.persistEntityModel(self,context,context.inputLayerEntitiesPackage,"xcore",context.inputLayerEnumsPackage)
        PersistToFile.persistEntityModel(self,context,context.outputLayerEntitiesPackage,"xcore",context.outputLayerEnumsPackage)
        PersistToFile.persistEnumModel(self,context,context.inputLayerEnumsPackage,"xcore")
        PersistToFile.persistEnumModel(self,context,context.outputLayerEnumsPackage,"xcore")
        #PersistToFile.persistTypesModel(self,context,context.typesPackage,"xcore")
        
        PersistToFile.persistEntityModel(self,context,context.inputLayerEntitiesPackage,"rpmn",context.inputLayerEnumsPackage)
        PersistToFile.persistEntityModel(self,context,context.outputLayerEntitiesPackage,"rpmn",context.outputLayerEnumsPackage)
        PersistToFile.persistEnumModel(self,context,context.inputLayerEnumsPackage,"rpmn")
        PersistToFile.persistEnumModel(self,context,context.outputLayerEnumsPackage,"rpmn")
        PersistToFile.persistTypesModel(self,context,context.typesPackage,"rpmn")
        
        
        
    def persistEntityModel(self,context,thePackage,extension,importedPackage):
            
        f = open(context.outputDirectory + thePackage.name  +'.' +extension, "a",  encoding='utf-8')
        f.write("\t\t package " + thePackage.name + "\r")  
        f.write("\t\t import " + importedPackage.name + ".*\r")   
        if extension == "rpmn":
            f.write("\t\t import types.*\r")    
        for classifier in  thePackage.eClassifiers:
            if isinstance(classifier,EClass):
                f.write("\t\t\t")
                if classifier.abstract==True:
                    f.write("abstract ")
                f.write("class " + classifier.name)
                if (hasattr(classifier, "superTypes")  and len(classifier.superTypes) > 0):
                    f.write(" extends " +  classifier.superTypes[0].name) 
                f.write( " {\r")
                for member in classifier.eStructuralFeatures:
                    
                    if isinstance(member, EReference):
                        if (member.containment):
                            f.write("\t\t\t\tcontains "  )
                        else:
                            f.write("\t\t\t\trefers "  )
                        
                        f.write(member.eType.name + " " )
                        if member.upperBound == -1:
                            f.write("[] ")
                        elif ( (member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            f.write("[" + str(member.lowerBound) + ".." +str(member.upperBound) + "] ")
                        f.write(member.name)
                        f.write(" \r"  )
                    elif isinstance(member, EAttribute):
                        f.write("\t\t\t\t")
                        # if member.iD:
                        #   f.write("id ")
                        
                        if (member.eType.name == "EString"):
                            f.write( "String  " )
                        elif (member.eType.name == "EDouble"):
                            f.write( "double  " )
                        elif (member.eType.name == "EInt"):
                            f.write( "int  " )
                        elif (member.eType.name == "EDate"):
                            f.write( "Date  " )
                        else:   
                            f.write(member.eType.name + " " )
                                
                            
                            
                        if member.upperBound == -1:
                            f.write("[] ")
                        elif ( (member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            f.write("[" + str(member.lowerBound) + ".." +str(member.upperBound) + "] ")
                        f.write(member.name)
                        f.write(" \r"  )
                for operation in classifier.eOperations:
                    
                    if isinstance(operation, EOperation):
                        f.write("\t\t\t\top ")
                          
                        f.write(operation.eType.name + " " )
                    if operation.upperBound == -1:
                        f.write("[] ")
                    elif ( (operation.lowerBound == 0) and (operation.upperBound == 1)):
                        f.write(" ")
                    else:
                        f.write("[" + str(operation.lowerBound) + ".." +str(operation.upperBound) + "] ")
                    f.write(operation.name)
                    f.write("() {}")
                    f.write(" \r"  )
                        
                f.write("\t\t\t}\r")
            
            
       
        f.close()
    
    def persistTypesModel(self,context,thePackage,extension):
        f = open(context.outputDirectory + thePackage.name  +'.' +extension, "a",  encoding='utf-8')
        f.write("\t\t package " + thePackage.name + "\r")  
        f.write("\t\t\ttype double wraps Double\r")
        f.write("\t\t\ttype String wraps String\r")
        f.write("\t\t\ttype int wraps Integer\r") 
        if extension == "rpmn":
            f.write("\t\t\ttype Date wraps Date\r")
        else:
            f.write("\t\t\ttype Date wraps java.util.Date\r")  
        f.close()
        
    def persistEnumModel(self,context,thePackage,extension):
            
        f = open(context.outputDirectory + thePackage.name  +'.' +extension, "a",  encoding='utf-8')
        f.write("\t\t package " + thePackage.name + "\r")    
        for classifier in  thePackage.eClassifiers:
            
            if isinstance(classifier,EEnum):
                f.write("\t\t\tenum " + classifier.name)
                
                f.write(" { ")  
                counter = 0
                splitcount = 1
                for theLiteral in classifier.eLiterals:
                    counter=counter+1
                    
                    if counter < 100:
                        f.write(" " + theLiteral.name + " as \"" + theLiteral.literal  + "\" = " + str(theLiteral.value)  )
                    else:
                        counter = 0
                        splitcount = splitcount+1
                        f.write(" }\r")
                        f.write("\t\t\tenum " + classifier.name + "_" + str(splitcount))
                        f.write(" { ")  
                        print("splitting enum " + classifier.name)
                    
                f.write(" }\r")
            
       
        f.close()  
    
   
    
    def saveModelAsXMIFile(self,context):
        '''
         save model as an xmi file representing an object tree.
        '''
        rset2 = ResourceSet()
        types_resource2 = rset2.create_resource(URI(context.outputDirectory + "\\types.ecore"))  # This will create an XMI resource
        types_resource2.append(context.typesPackage)  # we add the EPackage instance in the resource
        types_resource2.save()  # we then serialize it
        inputLayerEnums_resource2 = rset2.create_resource(URI(context.outputDirectory + "\\input_layer_enums.ecore"))  # This will create an XMI resource
        inputLayerEnums_resource2.append(context.inputLayerEnumsPackage)  # we add the EPackage instance in the resource
        inputLayerEnums_resource2.save()
        outputLayerEnums_resource2 = rset2.create_resource(URI(context.outputDirectory + "\\output_layer_enums.ecore"))  # This will create an XMI resource
        outputLayerEnums_resource2.append(context.outputLayerEnumsPackage)  # we add the EPackage instance in the resource
        outputLayerEnums_resource2.save()
        inputLayerEntities_resource2 = rset2.create_resource(URI(context.outputDirectory + "\\input_layer_entities.ecore"))  # This will create an XMI resource
        inputLayerEntities_resource2.append(context.inputLayerEntitiesPackage)  # we add the EPackage instance in the resource
        inputLayerEntities_resource2.save()
        outputLayerEntities_resource2 = rset2.create_resource(URI(context.outputDirectory + "\\output_layer_entities.ecore"))  # This will create an XMI resource
        outputLayerEntities_resource2.append(context.outputLayerEntitiesPackage)  # we add the EPackage instance in the resource
        outputLayerEntities_resource2.save()