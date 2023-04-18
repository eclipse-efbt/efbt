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
from open_reg_specs import *
from pyecore.resources import ResourceSet, URI
from pyecore.ecore import *
from pyecore.resources.xmi import XMIResource
from pyecore.resources.xmi import XMIOptions
from Utils import Utils
from context import Context
from collections import Counter
import os

class PersistToFile:
    def saveModelAsXCoreLFile(self,context ):
        
        PersistToFile.persistEntityModel(self,context,context.inputLayerEntitiesPackage,"xcorel",context.inputLayerEnumsPackage)
        PersistToFile.persistEntityModel(self,context,context.outputLayerEntitiesPackage,"xcorel",context.outputLayerEnumsPackage)
        PersistToFile.persistEnumModel(self,context,context.inputLayerEnumsPackage,"xcorel")
        PersistToFile.persistEnumModel(self,context,context.outputLayerEnumsPackage,"xcorel")
        PersistToFile.persistTypesModel(self,context,context.typesPackage,"xcorel")
        PersistToFile.persistWorkflow(self,context)
        PersistToFile.persistGenerationTransformations(self,context)
        for package in context.logicPackages:
            PersistToFile.persistEntityModel(self,context,package,"xcorel",context.outputLayerEnumsPackage)
        
   
        
    def persistEntityModel(self,context,thePackage,extension,importedPackage):
            
        f = open(context.outputDirectory + os.sep +thePackage.name  +'.' +extension, "a",  encoding='utf-8')
        f.write("\t\t package " + thePackage.name + "\r")  
        f.write("\t\t import " + importedPackage.name + ".*\r")   
        if thePackage ==context.outputLayerEntitiesPackage: 
            for importString in context.importLogicStrings:
                f.write("\t\t import " + importString + ".*\r")  
        if extension == "xcorel":
            f.write("\t\t import types.*\r")    
        for classifier in  thePackage.classifiers:
            if isinstance(classifier,XClass):
                f.write("\t\t\t")
                if classifier.abstract==True:
                    f.write("abstract ")
                f.write("class " + classifier.name)
                if (hasattr(classifier, "superTypes")  and len(classifier.superTypes) > 0):
                    f.write(" extends " +  classifier.superTypes[0].name) 
                f.write( " {\r")
                for member in classifier.members:
                    
                    if isinstance(member, XReference):
                        if (member.containment):
                            f.write("\t\t\t\tcontains "  )
                        else:
                            f.write("\t\t\t\trefers "  )
                        
                        f.write(member.type.name + " " )
                        if ( (member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            f.write("[" + str(member.lowerBound) + ".." +str(member.upperBound) + "] ")
                    
                        f.write(member.name)
                        f.write(" \r"  )
                    elif isinstance(member, XAttribute):
                        f.write("\t\t\t\t")
                        if member.iD:
                           f.write("id ")
                        
                        if (member.type.name == "String"):
                            f.write( "String  " )
                        elif (member.type.name == "Double"):
                            f.write( "double  " )
                        elif (member.type.name == "Integer"):
                            f.write( "int  " )
                        elif (member.type.name == "Date"):
                            f.write( "Date  " )
                        else:   
                            f.write(member.type.name + " " )
                                
                            
                            
                        if ( (member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            f.write("[" + str(member.lowerBound) + ".." +str(member.upperBound) + "] ")
                    
                        f.write(member.name)
                        f.write(" \r"  )

                    elif isinstance(member, XOperation):
                            f.write("\t\t\t\top ")
                            
                            f.write(member.type.name + " " )
                            if ( (member.lowerBound == 0) and (member.upperBound == 1)):
                                f.write(" ")
                            else:
                                f.write("[" + str(member.lowerBound) + ".." +str(member.upperBound) + "] ")
                     
                            f.write(member.name)
                            if extension == "xcorel" and context.addExecutableStubs:
                                if hasattr(member, "xcorelText"):
                                    f.write("() {\n\t\t\t\t\t\"" + member.xcorelText + "\"\n\t\t\t\t\t}")
                                else:
                                    f.write("() {}")
                            else:
                                f.write("() {}")
        
                            f.write(" \r"  )
                        
                f.write("\t\t\t}\r")
            
            
       
        f.close()
    
    def persistTypesModel(self,context,thePackage,extension):
        f = open(context.outputDirectory + os.sep + thePackage.name  +'.' +extension, "a",  encoding='utf-8')
        f.write("\t\t package " + thePackage.name + "\r")  
        f.write("\t\t\ttype double wraps double\r")
        f.write("\t\t\ttype String wraps String\r")
        f.write("\t\t\ttype int wraps int\r") 
        if extension == "xcorel":
            f.write("\t\t\ttype Date wraps Date\r")
        else:
            f.write("\t\t\ttype Date wraps java.util.Date\r")
        f.write("\t\t\ttype boolean wraps boolean\r")  
        f.close()
        
    def persistEnumModel(self,context,thePackage,extension):
            
        f = open(context.outputDirectory + os.sep + thePackage.name  +'.' +extension, "a",  encoding='utf-8')
        f.write("\t\t package " + thePackage.name + "\r")    
        for classifier in  thePackage.classifiers:
            
            if isinstance(classifier,XEnum):
                f.write("\t\t\tenum " + classifier.name)
                
                f.write(" { ")  
                counter = 0
                splitcount = 1
                for theLiteral in classifier.literals:
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
    
                    

    def getVTLTextForLayer(self,context,layer):
        
        output = "" 
        if context.persistVTLComments:
            output = output + "/** VTL intermediate layer and report combination specific VTL \r"
            intermediateLayer = None
            for vtl in context.vtlModule.VTLForSelectionLayers.vTLForSelectionLayers:
                if vtl.selectionLayer == layer:
                    intermediateLayer = vtl.intermediateLayer
                    for combo in vtl.outputLayer.VTLForOutputLayerAndIntemedateLayerCombinations:
                        if (combo.intermediateLayer == vtl.intermediateLayer) and (combo.outputLayer.outputLayer == layer.selectionLayer.generatedEntity):
                            for trans in combo.transformations:
                                output = output + PersistToFile.removeCommentChars(self,trans.expression) + "\r"
            output = output +  "*/\r\r"
            
            output = output + "/** VTL intermediate layer specific VTL \r"
            intermediateLayer = None
            for vtl in context.vtlModule.VTLForSelectionLayers.vTLForSelectionLayers:
                if vtl.selectionLayer == layer:
                    intermediateLayer = vtl.intermediateLayer
                    for trans in intermediateLayer.transformations:
                        output = output + PersistToFile.removeCommentChars(self,trans.expression) + "\r"
            output = output +  "*/\r\r"
            
            output = output + "/** assocated enriched layer in VTL \r"
            enrichedLayer = intermediateLayer.dependant_enriched_cubes
            if not(enrichedLayer is None):
                output = output + "enriched Layer :" + enrichedLayer.name + "\r"
                output = output + "enriched Layer transformations:\r"
                for exp in enrichedLayer.transformations:
                    output = output + PersistToFile.removeCommentChars(self,exp.expression) + "\r"
            output = output +  "*/\r\r"    
           
            output = output + "/** associated input layer table and filter \r"
            for link in context.vtlModule.entityToVTLIntermediateLayerLinks.entityToVTLIntermediateLayerLinks:
                if link.VTLIntermediateLayer == intermediateLayer:
                    output = output +  "input layer entity: " + link.entity.name + "\r"        
                    output = output +  "filter: " + link.filter + "\r"  
            
            output = output +  "*/\r\r" 
        return output
            
        
    def getVTLTextForView(self,context,view):
        output = "" 
        if context.persistVTLComments:
            output = output + "/** View specific VTL \r"
            for vtl in context.vtlModule.VTLForViews.vTLForViews:
                if vtl.view == view:
                    output = output + "output layer = " + vtl.vtl.outputLayer.name + "\r"
            output = output +  "*/\r\r"
        
        return output
        
        
    def countNonNoneLayers(self,element ): 
        counter = 0
        for layer in element.selectionLayers:
            if not(layer.name is None):
                counter = counter + 1  
        return counter 
    
    def saveModelAsXMIFile(self,context):
        '''
         save model as an xmi file representing an object tree.
        '''
        rset = ResourceSet()

        resource = rset.create_resource(URI(context.outputDirectory + os.sep + 'VTL.xmi'))  # This will create an XMI resource
        resource.append(context.moduleList)
        resource.save()
        
    def persistWorkflow(self,context):
        f = open(context.outputDirectory+ os.sep + 'workflow.xcorel', "a",  encoding='utf-8')
        f.write("WorkflowModule " + context.workflowModule.name + "\r{\r")
        f.write("\t\tsubProcess {\r")    
        for process in  context.workflowModule.subProcess:
            f.write("\t\t\t")
            f.write("SubProcess " + process.name + " flowElements {\r"  )
            f.write("\t\t\t\tParallelGateway gw1 outgoing (")
            counter = 0
            for element in process.flowElements:
                if not(counter == 0):
                    f.write(",")
                counter= counter +1
                f.write("sf" + str(counter))
            f.write("),\r")  
            counter = 0
            for element in process.flowElements:
                if isinstance(element,ScriptTask):
                    counter= counter +1
                    f.write("\t\t\t\t")
                    f.write("ScriptTask " + element.name + " incoming (sf" +str(counter) +")")
                    
                    counter2=0
                    if PersistToFile.countNonNoneLayers(self,element) > 0:
                        f.write("\r\t\t\t\t\tselectionLayers {\r")
                        for layer in element.selectionLayers:
                            if not(layer.name is None):
                                if not(counter2 == 0):
                                    f.write(",")
                                f.write("\r\t\t\t\t\t\tSelectionLayer " + Utils.makeValidID(str(layer.name)) + "{generatedEntity output_layer_entities." + layer.generatedEntity.name + " }")
                                counter2 = counter2+1
                        f.write("\r\t\t\t\t\t}")
                    f.write(",\r")
                                
            counter = 0
            
            for element in process.flowElements:
                if not (counter == 0):
                    f.write(",\r")
                counter= counter +1
                f.write("SequenceFlow sf" + str(counter) + " from gw1 to " + element.name )
            f.write("\r}\r") 
            f.write("}\r")  
            f.write("}\r")  
            f.close()
            
            
    def persistGenerationTransformations (self,context):
        views = context.viewModule.views

        for view in views:
            f = open(context.outputDirectory + os.sep + view.name + '_view.xcorel', "a",  encoding='utf-8')
            f.write("ViewModule " + view.name + "_viewModule\r{\r")
            f.write("\tviews " + "{\r")
            f.write("\t\tView " + view.name + "_view {\r")
            for layer in view.selectionLayerSQL:
                if not(layer.selectionLayer.name is None):
                    f.write("\t\t\tLayerSQL {\r")
                    f.write("selectionLayer finrepWorkflow.finrepReports.task_" + view.name[5:len(view.name)] + "." + Utils.makeValidID(str(layer.selectionLayer.name)) + "\r")
                    for column in layer.columns:
                        f.write("\t\t\t\tSelectMember   input_layer_enums.Group_type_domain.Internal_group as output_layer_entities."+view.name[5:len(view.name)] + "_REF_OutputItem." + column.asAttribute.name +"\r")
                    f.write("\t\t\t}\r")
                    f.write(PersistToFile.getVTLTextForLayer(self,context,layer))
            f.write("\t\t}\r")
            f.write("\t}\r")
            f.write("}\r")
            f.write(PersistToFile.getVTLTextForView(self,context,view))
            f.close()
            
    def removeCommentChars(self, theString):
        return theString.replace("/**", "").replace("/*", "").replace("*/", "")