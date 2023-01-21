
from pyecore.resources import ResourceSet, URI
from pyecore.ecore import *
from pyecore.resources.xmi import XMIResource
from pyecore.resources.xmi import XMIOptions
from Utils import Utils
from context import Context
from collections import Counter

class PersistToFile:
    def saveModelAsRPMNFile(self,context ):
        PersistToFile.persistDataModel(self,context)
        
    def persistDataModel(self,context):
            
        f = open(context.outputDirectory + 'data_model.xcore', "a",  encoding='utf-8')
        f.write("\t\t package " + context.rpmnPackage.name + "\r")    
        for classifier in  context.rpmnPackage.eClassifiers:
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
                            f.write( "Double  " )
                        elif (member.eType.name == "EInt"):
                            f.write( "Integer  " )
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
                        f.write("\t\t\t\top " + operation.eType.name + " " )
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
            if isinstance(classifier,EEnum):
                f.write("\t\t\tenum " + classifier.name)
                
                f.write(" { ")  
                counter = 0
                splitcount = 1
                for theLiteral in classifier.eLiterals:
                    counter=counter+1
                    
                    if counter < 100:
                        f.write(" " + theLiteral.literal + " as \"" + theLiteral.name + "\" = " + str(theLiteral.value)  )
                    else:
                        counter = 0
                        splitcount = splitcount+1
                        f.write(" }\r")
                        f.write("\t\t\tenum " + classifier.name + "_" + str(splitcount))
                        f.write(" { ")  
                        print("splitting enum " + classifier.name)
                    
                f.write(" }\r")
            
        f.write("\t\t\ttype Double wraps Double\r")
        f.write("\t\t\ttype String wraps String\r")
        f.write("\t\t\ttype Integer wraps Integer\r")  
        f.write("\t\t\ttype Date wraps java.util.Date\r")        
        f.close()
    
        
    
   
    
    def saveModelAsXMIFile(self,context):
        '''
         save model as an xmi file representing an object tree.
        '''
        rset = ResourceSet()

        resource = rset.create_resource(URI(context.outputDirectory + 'pybird.ecore'))  # This will create an XMI resource
        resource.append(context.rpmnPackage)
        resource.save()