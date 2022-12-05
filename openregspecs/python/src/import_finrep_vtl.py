'''
Created on 22 Jan 2022

@author: Neil
'''
import csv
from open_reg_specs import *
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
        
        ImportFinrepVTL.buildOutputLayerToVTLLayerMap(self,context)
        subProcess = SubProcess(name = "finrepReports")
        context.workflowModule.subProcess.extend([subProcess])
        ImportFinrepVTL.addReports(self,context)
    
    def buildOutputLayerToVTLLayerMap(self,context):
        fileLocation = context.fileDirectory + "\\TRANSFORMATIONS_50.csv"

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
                            print("expression")
                            print(expression)
                            indexOfExpressionOpenBracket = expression.find('(')
                            indexOfExpressionClosedBracket = expression.find(')')
                            print(indexOfExpressionOpenBracket)
                            print(indexOfExpressionClosedBracket)
                            vtl_layer_list = expression[indexOfExpressionOpenBracket:indexOfExpressionClosedBracket].split(',')
                            
                            indexOfSchemeStart = expression.find('G_')
                            indexOfSchemeEnd= expression.find('UNFLDD_FINREP_1')
                            output_layer = scheme[indexOfSchemeStart:indexOfSchemeEnd]
                            context.outputLayerToVTLLayerMap[output_layer, vtl_layer_list]
    
    def buildListOfVTLLayersForFinrep(self,context):
        fileLocation = context.fileDirectory + "\\TRANSFORMATIONS_50.csv"

        headerSkipped = False
        # Load all the entities from the csv file, make an XClass per entity,
        # and add the XClass to the package
        with open(fileLocation) as csvfile:
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
            fileLocation = context.fileDirectory + "\\TRANSFORMATIONS_50.csv"
    
            headerSkipped = False
            # Load all the entities from the csv file, make an XClass per entity,
            # and add the XClass to the package
            with open(fileLocation) as csvfile:
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
        fileLocation = context.fileDirectory + "\\in_scope_reports.csv"
        
        
        headerSkipped = False
        # Load all the entities from the csv file, make an XClass per entity,
        # and add the XClass to the package
        with open(fileLocation) as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not headerSkipped):
                    headerSkipped = True
                else:
                    reportTemplate = row[0];
                    new = row[1];
                    view =  View(name = reportTemplate)
                    task = ScriptTask(name = reportTemplate)
                    context.viewModule.views.extend([view])
                    context.workflowModule.subProcess[0].extend([task])
                    ImportFinrepVTL.addLayers(self, view, task)
                    
                    
    def addLayers(self,context,view, task):
        fileLocation = context.fileDirectory + "\\VTL_layer_to_IL.csv"
        
        vtlLayers = context.outputLayerToVTLLayerMap[view.name]
        
        for layer in vtlLayers:
            
            headerSkipped = False
            # Load all the entities from the csv file, make an XClass per entity,
            # and add the XClass to the package
            with open(fileLocation) as csvfile:
                filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
                for row in filereader:
                    # skip the first line which is the header.
                    if (not headerSkipped):
                        headerSkipped = True
                    else:
                        vtlLayer = row[0];
                        input_layer = row[2]
                        sqlfilter = row[3]
                        if vtlLayer == layer:
                            selectionLayer = SelectionLayer(name = input_layer)
                            task.selectionLayers.extend([selectionLayer])
                            layerSQL = LayerSQL(name = input_layer)
                            layerSQL.selectionLayer = selectionLayer
                            layerSQL.filter_comment = sqlfilter
                            layerSQL.original_vtl_layer_comment = vtlLayer
                            view.selectionLayerSQL.extend([layerSQL])
                            ImportFinrepVTL.addColumnsToLayer(self,layerSQL)
                            
                        
    def addColumnsToLayer(self,context,layerSQL):
        fileLocation = context.fileDirectory + "\\cube_structure_item.csv"
        il_name = layerSQL.name
        amended_il_name = "FINREP_REF_" + il_name + "_REF_FINREP 3.0"

        
        headerSkipped = False
        # Load all the entities from the csv file, make an XClass per entity,
        # and add the XClass to the package
        with open(fileLocation) as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not headerSkipped):
                    headerSkipped = True
                else:
                    cubeName = row[0]
                    columnName = row[1]
                    if cubeName == amended_il_name:
                        attribute = XAttribute(name = columnName)
                        selectColumn  = SelectColumnAttributeAs ( name = columnName )
                        selectColumn.attribute = attribute
                        layerSQL.columns.extend([selectColumn])
                    

            