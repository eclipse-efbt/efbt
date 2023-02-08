'''
Created on 22 Jan 2022

@author: Neil
'''
import csv
from open_reg_specs import *
from pickle import TRUE
import os

class ImportFinrepVTL(object):
    '''
    1.) Look throug in scope reports
    2.) Fro each report:
    3.) get the list of column items
    4.) get the union command
    5.) get the list of layers
    6.) for each layer: 
    7.) get the Input Layers.
    8.) for each input layer:
    9.) write the rpmn volumns:
    10.) write the filter condition
    11.) write the VTL that is on top of the layer to make the report
    12.) write the VTL that is for the layer
    13.) write the VTL that is for the enrichment cube under the layer

    Thats all.
    Document this in a powerpoint.
    '''

    def doImport(self,context):
        
        #ImportFinrepVTL.buildOutputLayerToVTLLayerMap(self,context)
        subProcess = SubProcess(name = "finrepReports")
        context.workflowModule.subProcess.extend([subProcess])
        #ImportFinrepVTL.addReports(self,context)
        ImportFinrepVTL.importTransformationsAndSchemes(self,context)
        ImportFinrepVTL.buildListOfIntermediateFinrepLayers(self,context)
        ImportFinrepVTL.buildROLToIntermediateLayerLink(self,context)
        ImportFinrepVTL.buildIntermediateLayerToInputLayer(self,context)
        ImportFinrepVTL.addReports(self, context)
        
    def importTransformationsAndSchemes(self,context):
        fileLocation = context.fileDirectory + os.sep + "transformations.csv"

        headerSkipped = False
        # Load all the entities from the csv file, make an XClass per entity,
        # and add the XClass to the package
        with open(fileLocation,encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not headerSkipped):
                    headerSkipped = True
                else:
                    expression = row[2]
                    scheme = row[7]
                    description = row[1]
                    transformationID = row[6]
                    order = row[5]
                    vtlScheme = None
                    if not(ImportFinrepVTL.schemesContains(self,context,scheme)):
                        #new scheme
                        vtlScheme = VTLScheme()
                        vtlScheme.scheme_id = scheme
                        context.vtlModule.VTLSchemes.append(vtlScheme) 
                    else:
                        vtlScheme = ImportFinrepVTL.lookupScheme(self,context,scheme)
                     
                      
                    transformation = VTLTransformation()
                    transformation.transformationID = transformationID
                    transformation.description = description
                    transformation.order_in_scheme = order
                    transformation.expression = expression
                    vtlScheme.expressions.append(transformation)
                    
                    #if scheme.startswith("G_F") and scheme.endswith("_FINREP_1"):
                    #    if "union" in expressio
        
    def schemesContains(self,context,scheme_id):
        for scheme in context.vtlModule.VTLSchemes:
            if scheme.scheme_id == scheme_id:
                return True
        return False
    
    def lookupScheme(self,context,scheme_id):
        for scheme in context.vtlModule.VTLSchemes:
            if scheme.scheme_id == scheme_id:
                return scheme
        return None
    
    def buildROLToIntermediateLayerLink(self,context):
        for scheme in context.vtlModule.VTLSchemes:
            #print("scheme")
            #print(scheme)
            if scheme.scheme_id.startswith("G_F") and scheme.scheme_id.endswith("_REF_UNFLDD_FINREP_1"):
                #print("intermediatescheme")
                #print(scheme)
                # indexOfSchemeStart = scheme.scheme_id.find('G_')
                indexOfSchemeEnd= scheme.scheme_id.find('_REF_UNFLDD_FINREP_1')
                output_layer_name = scheme.scheme_id[2:indexOfSchemeEnd]
                
                outputLayer=VTLGeneratedOutputlayer(name=output_layer_name)
                #print("output_layer_name1")
                #print(output_layer_name)
                outputLayer.outputLayer = ImportFinrepVTL.findEntity(self,context,output_layer_name + "_REF_OutputItem")
                #print("outputLayer.outputLayer")
                #print(outputLayer.outputLayer)
                context.vtlModule.VTLGeneratedOutputLayers.append(outputLayer)
                for transformation in scheme.expressions:
                    expression = transformation.expression
                    if "union" in expression:
                        indexOfExpressionOpenBracket = expression.find('(')
                        indexOfExpressionClosedBracket = expression.find(')')
                        print(indexOfExpressionOpenBracket)
                        print(indexOfExpressionClosedBracket)
                        vtl_layer_list = expression[indexOfExpressionOpenBracket+1:indexOfExpressionClosedBracket].split(',')
                        for vtl_layer in vtl_layer_list:
                            #print("vtl_layer")
                            #print(vtl_layer)
                            
                            intermediateLayer = ImportFinrepVTL.findIntermediateLayer(self,context,"G_" + vtl_layer.strip() + "_1")
                            #print("intermediateLayer")
                            #print(intermediateLayer)
                            if not(intermediateLayer is None):
                                outputLayer.dependant_intermediate_layers.append(intermediateLayer)
                                combo = VTLForOutputLayerAndIntermediateLayerCombination()
                                combo.outputLayer = outputLayer
                                combo.intermediateLayer = intermediateLayer
                                #get the special commands specific to the output layer and intermediate layer
                                for  trans in scheme.expressions:
                                    indexOfColon = trans.expression.find(':')
                                    lhs = trans.expression[0:indexOfColon].strip()
                                    if ("G_" + lhs +"_1") == intermediateLayer.transformations.scheme_id:
                                        combo.transformations.append(trans)
                                outputLayer.VTLForOutputLayerAndIntemedateLayerCombinations.append(combo)
                                    

    def findEntity(self,context,outputLayerName):
        #print("findEntity")
        #print(outputLayerName)
        #print("outputLayerName")
        for theEntity in context.inputLayerEntitiesPackage.classifiers:
            if isinstance(theEntity, XClass):
                #print("rol.name")
                #print(rol.name)
                if theEntity.name == outputLayerName:
                    return theEntity
                
        for theEntity in context.outputLayerEntitiesPackage.classifiers:
            if isinstance(theEntity, XClass):
                #print("rol.name")
                #print(rol.name)
                if theEntity.name == outputLayerName:
                    return theEntity
                
        return None
            
        
    def findIntermediateLayer(self,context,layerName):
        
        #print("layerName")
        #print(layerName)
        for layer in context.vtlModule.VTLGeneratedIntermediateLayers:
            #print(layer.transformations.scheme_id)
            #print("layer.transformations.scheme_id")
            if layer.transformations.scheme_id == layerName:
                return layer
            
        return None
                
    
    def buildOutputLayerToVTLLayerMap(self,context):
        fileLocation = context.fileDirectory + os.sep + "transformations.csv"

        headerSkipped = False
        # Load all the entities from the csv file, make an XClass per entity,
        # and add the XClass to the package
        with open(fileLocation,encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not headerSkipped):
                    headerSkipped = True
                else:
                    expression = row[2]
                    scheme = row[7]
                    if scheme.startswith("G_F") and scheme.endswith("_FINREP_1"):
                        if "union" in expression:
                            #print("expression")
                            #print(expression)
                            indexOfExpressionOpenBracket = expression.find('(')
                            indexOfExpressionClosedBracket = expression.find(')')
                            #print(indexOfExpressionOpenBracket)
                            #print(indexOfExpressionClosedBracket)
                            vtl_layer_list = expression[indexOfExpressionOpenBracket:indexOfExpressionClosedBracket].split(',')
                            
                            indexOfSchemeStart = expression.find('G_')
                            indexOfSchemeEnd= expression.find('UNFLDD_FINREP_1')
                            output_layer = scheme[indexOfSchemeStart:indexOfSchemeEnd]
                            context.outputLayerToVTLLayerMap[output_layer, vtl_layer_list]
    
    def buildListOfIntermediateFinrepLayers(self,context):
        for scheme in context.vtlModule.VTLSchemes:
            if scheme.scheme_id.startswith("G_") and not(scheme.scheme_id.startswith("G_F_")) and scheme.scheme_id.endswith("_FINREP_1"):  
                intermediateLayer = VTLGeneratedIntermediateLayer(name=scheme.scheme_id)
                intermediateLayer.transformations = scheme
                intermediateLayer.dependant_enriched_cubes = ImportFinrepVTL.findEnrichedCubeFor(self, context, scheme.scheme_id)
                if ImportFinrepVTL.findIntermediateLayer(self,context,scheme.scheme_id) is None:
                    context.vtlModule.VTLGeneratedIntermediateLayers.append(intermediateLayer)
    
    def findEnrichedCubeFor(self,context,scheme_id):
        indexOfSchemeStart = scheme_id.find('G_')
        indexOfSchemeEnd= scheme_id.find('_FINREP_1')
        enrichedCube = "P_" + scheme_id[2:indexOfSchemeEnd] + "_E_1"
        print("enrichedCube")
        print(enrichedCube)

        returnTransformations = None
        for scheme in context.vtlModule.VTLSchemes:

            if scheme.scheme_id == enrichedCube:
                returnTransformations = scheme
                
        return returnTransformations
                
    def buildListOfVTLLayersForFinrep(self,context):
        fileLocation = context.fileDirectory + os.sep + "transformations.csv"

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
                    expression = row[2]
                    scheme = row[7] 
                    if scheme.startsWith("G_") and not(scheme.startsWith("G_F_")) and scheme.endsWith("_FINREP_1"):   
                        indexOfSchemeStart = expression.find('G_')
                        indexOfSchemeEnd= expression.find('_FINREP_1')
                        output_layer = scheme.substring(indexOfSchemeStart,indexOfSchemeEnd)
                        if not(context.vtl_layers.contains(output_layer)):
                            context.vtl_layers.append(output_layer)
                                                                      
    def VTLLayerToVTLLayerLogicMap(self,context):
        
        for vtl_layer in context.vtl_layers:
            expressionlist = []
            fileLocation = context.fileDirectory + os.sep + "transformations.csv"
    
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
                        expression = row[2]
                        scheme = row[7]
                        if ("G_F_" + vtl_layer + "_FINREP_1") == scheme:
                            expressionlist.add()
                        
                    
                       
        
    def addReports(self,context):
        fileLocation = context.fileDirectory + os.sep + "in_scope_reports.csv"
        
        
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
                    new = row[1];
                    view =  View(name = "view_" + reportTemplate)
                    context.viewModule.views.append(view)
                    
                    task = ScriptTask(name = "task_" + reportTemplate)
                    context.workflowModule.subProcess[0].flowElements.append(task)
                    ImportFinrepVTL.addLayers(self, context, view, task)
                    
                    
    def addLayers(self,context,view, task):
        
        
        #print("view.name")
        #print(view.name[5:len(view.name)] )
        rolVTL = ImportFinrepVTL.findOutputLayerVTL(self,context,view.name[5:len(view.name)] + "_REF_OutputItem" )
        #print("rolVTL")
        #print(rolVTL)
        if not (rolVTL is None):
            view.outputLayer = rolVTL.outputLayer
            viewVTL = VTLForView(name="vtl_" + view.name)
            viewVTL.view = view
            viewVTL.vtl = rolVTL
            context.vtlModule.VTLForViews.append(viewVTL)
            
            for intermediateLayer in rolVTL.dependant_intermediate_layers:
                
                #vtlLayer = intermediateLayer.transformations
                link = ImportFinrepVTL.findEntityIntermediateLayerLink(self,context,intermediateLayer) 
                if not( link is None):
                    input_layer = link.entity
                    sqlfilter = link.filter
                    selectionLayer = SelectionLayer()
                    if input_layer is not None:
                        selectionLayer.name=view.outputLayer.name + "_" + input_layer.name + "_" + link.filterName
                    selectionLayer.generatedEntity = rolVTL.outputLayer
                    task.selectionLayers.extend([selectionLayer])
                    layerSQL = LayerSQL()
                    layerSQL.selectionLayer = selectionLayer
                    vtlForSelectionLayer = VTLForSelectionLayer()
                    vtlForSelectionLayer.selectionLayer = layerSQL
                    vtlForSelectionLayer.outputLayer = rolVTL
                    vtlForSelectionLayer.intermediateLayer = intermediateLayer
                    context.vtlModule.VTLForSelectionLayers.append(vtlForSelectionLayer)
                    
                    
                    view.selectionLayerSQL.extend([layerSQL])
                    ImportFinrepVTL.addColumnsToLayer(self,context,layerSQL)
           
    def findOutputLayerVTL(self,context, outputLayerName):
        #print("outputLayerName")
        #print(outputLayerName)
        for rol in context.vtlModule.VTLGeneratedOutputLayers:
            if not(rol.outputLayer is None):
                #print("rol.outputLayer.name")
                #print(rol.outputLayer.name)
                a=0
            if not(rol.outputLayer is None) and (rol.outputLayer.name == outputLayerName):
                return rol
        

    def findEntityIntermediateLayerLink(self,context,intermediateLayer):   
        #print("intermediateLayer")
        #print(intermediateLayer)  
        for link in context.vtlModule.entityToVTLIntermediateLayerLinks:
            if link.VTLIntermediateLayer == intermediateLayer:
                return link
    
                       
    def addColumnsToLayer(self,context,layerSQL):

        outputLayer = layerSQL.selectionLayer.generatedEntity
        if not (outputLayer is None):
            for member in outputLayer.members:
                if isinstance(member, XOperation):     
                    selectColumn  = SelectColumnAttributeAs ( )
                    selectColumn.asAttribute = member
                    layerSQL.columns.extend([selectColumn])
                    
    def buildIntermediateLayerToInputLayer(self,context):
        fileLocation = context.fileDirectory + os.sep + "VTL_layer_to_IL.csv"
        #il_name = layerSQL.name
        #amended_il_name = "FINREP_REF_" + il_name + "_REF_FINREP 3.0"

        
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
                    vtlLayer = row[0]
                    inputLayer = row[2]
                    filter = row[3]
                    filterName = row[5]
                    link = EntityToVTLIntermediateLayerLink()
                    link.VTLIntermediateLayer = ImportFinrepVTL.findIntermediateLayer(self,context,"G_" + vtlLayer.strip() + "_FINREP_1")
                    link.entity = ImportFinrepVTL.findEntity(self,context,inputLayer)
                    link.filter = filter 
                    link.filterName = filterName
                    context.vtlModule.entityToVTLIntermediateLayerLinks.append(link)
                    
     