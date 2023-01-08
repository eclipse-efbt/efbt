from open_reg_specs import *
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
        PersistToFile.persistWorkflow(self,context)
        PersistToFile.persistGenerationTransformations(self,context)
        
    def persistDataModel(self,context):
            
        f = open(context.outputDirectory + 'data_model.rpmn', "a",  encoding='utf-8')
        f.write("\t\t package " + context.rpmnPackage.name + "\r")    
        for classifier in  context.rpmnPackage.classifiers:
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
                        if member.upperBound == -1:
                            f.write("[] ")
                        elif ( (member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            f.write("[" + str(member.lowerBound) + ".." +str(member.upperBound) + "] ")
                        f.write(member.name)
                        f.write(" \r"  )
                    elif isinstance(member, XAttribute):
                        f.write("\t\t\t\t")
                        # if member.iD:
                        #   f.write("id ")
                        f.write(member.type.name + " " )
                        
                        if member.upperBound == -1:
                            f.write("[] ")
                        elif ( (member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            f.write("[" + str(member.lowerBound) + ".." +str(member.upperBound) + "] ")
                        f.write(member.name)
                        f.write(" \r"  )
                    elif isinstance(member, XOperation):
                        f.write("\t\t\t\top " + member.type.name + " " )
                        if member.upperBound == -1:
                            f.write("[] ")
                        elif ( (member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            f.write("[" + str(member.lowerBound) + ".." +str(member.upperBound) + "] ")
                        f.write(member.name)
                        f.write("() {}")
                        f.write(" \r"  )
                        
                f.write("\t\t\t}\r")
            if isinstance(classifier,XEnum):
                f.write("\t\t\tenum " + classifier.name)
                
                f.write(" { ")  
                for theLiteral in classifier.literals:
                    f.write(" " + theLiteral.literal + " as \"" + theLiteral.name + "\" = " + str(theLiteral.value)  )
                    
                f.write(" }\r")
            
        f.write("\t\t\ttype Double wraps Double\r")
        f.write("\t\t\ttype String wraps String\r")
        f.write("\t\t\ttype Integer wraps Integer\r")  
        f.write("\t\t\ttype Date wraps Date\r")        
        f.close()
    
        
    def persistWorkflow(self,context):
        f = open(context.outputDirectory + 'workflow.rpmn', "a",  encoding='utf-8')
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
                                f.write("\r\t\t\t\t\t\tSelectionLayer " + Utils.makeValidID(str(layer.name)) + "{generatedEntity \"bird." + layer.generatedEntity.name + "\" }")
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
            f = open(context.outputDirectory + view.name + '_view.rpmn', "a",  encoding='utf-8')
            f.write("ViewModule " + view.name + "_viewModule\r{\r")
            f.write("\tviews " + "{\r")
            f.write("\t\tView " + view.name + "_view {\r")
            for layer in view.selectionLayerSQL:
                if not(layer.selectionLayer.name is None):
                    f.write("\t\t\tLayerSQL {\r")
                    f.write("selectionLayer \"finrepWorkflow.finrepReports.task_" + view.name[5:len(view.name)] + "." + layer.selectionLayer.name + "\"\r")
                    for column in layer.columns:
                        f.write("\t\t\t\tSelectMember   \"\" as \"bird."+view.name[5:len(view.name)] + "_REF_OutputItem." + column.asAttribute.name +"\"\r")
                    f.write("\t\t\t}\r")
                    f.write(PersistToFile.getVTLTextForLayer(self,context,layer))
            f.write("\t\t}\r")
            f.write("\t}\r")
            f.write("}\r")
            f.write(PersistToFile.getVTLTextForView(self,context,view))
            f.close()
                    

    def getVTLTextForLayer(self,context,layer):
        
        output = "" 
        output = output + "/** VTL intermediate layer and report combination specific VTL \r"
        intermediateLayer = None
        for vtl in context.vtlModule.VTLForSelectionLayers:
            if vtl.selectionLayer == layer:
                intermediateLayer = vtl.intermediateLayer
                for combo in vtl.outputLayer.VTLForOutputLayerAndIntemedateLayerCombinations:
                    if (combo.intermediateLayer == vtl.intermediateLayer) and (combo.outputLayer.outputLayer == layer.selectionLayer.generatedEntity):
                        for trans in combo.transformations:
                            output = output + trans.expression + "\r"
        output = output +  "*/\r\r"
        
        output = output + "/** VTL intermediate layer specific VTL \r"
        intermediateLayer = None
        for vtl in context.vtlModule.VTLForSelectionLayers:
            if vtl.selectionLayer == layer:
                intermediateLayer = vtl.intermediateLayer
                for trans in intermediateLayer.transformations.expressions:
                    output = output + trans.expression + "\r"
        output = output +  "*/\r\r"
        
        output = output + "/** assocated enriched layer in VTL \r"
        enrichedLayer = intermediateLayer.dependant_enriched_cubes
        if not(enrichedLayer is None):
            output = output + "enriched Layer :" + enrichedLayer.scheme_id + "\r"
            output = output + "enriched Layer transformations:\r"
            for exp in enrichedLayer.expressions:
                output = output + exp.expression + "\r"
        output = output +  "*/\r\r"    
       
        output = output + "/** associated input layer table and filter \r"
        for link in context.vtlModule.entityToVTLIntermediateLayerLinks:
            if link.VTLIntermediateLayer == intermediateLayer:
                output = output +  "input layer entity: " + link.entity.name + "\r"        
                output = output +  "filter: " + link.filter + "\r"  
        
        output = output +  "*/\r\r" 
        return output
            
        
    def getVTLTextForView(self,context,view):
        output = "" 
        if context.persistVTLComments:
            output = output + "/** View specific VTL \r"
            for vtl in context.vtlModule.VTLForViews:
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

        resource = rset.create_resource(URI(context.outputDirectory + 'VTL.xmi'))  # This will create an XMI resource
        resource.append(context.moduleList)
        resource.save()