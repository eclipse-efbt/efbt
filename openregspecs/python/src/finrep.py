'''
Created on 22 Jan 2022

@author: Neil
'''
import csv
from open_reg_specs import *
class Finrep(object):
    '''
    1.) Look throug in scope reports
    2.) Fro each report:
    3.) get the list of column items
    4.) get the union command
    5.) get the list of layers
    6.) for each layer: 
    7.) get the Input Layers.
    8.) for each input layer:
    9.) write the rpmn:
    10.) write the relevent VTL commands fro that VTL layer.
    Thats all.
    Document this in a powerpoint.
    '''
    
    # the directory where we get our input files
    fileDirectory = ""
    # the directory where we save our outputs.
    outputDirectory = ""
     # This is the root Module for generation rpmn files
    viewModule = ViewModule(name='generations')
    
    workflowModule = WorkflowModule(name='finrep')
    
    outputLayerToVTLLayerMap = {}
    
    
    def convert(self,theFileDirectory,theOutputDirectory):
        
        Finrep.buildOutputLayerToVTLLayerMap(self)
        self.fileDirectory = theFileDirectory
        self.outputDirectory = theOutputDirectory
        subProcess = SubProcess(name = "finrepReports")
        self.workflowModule.subProcess.extend([subProcess])
        Finrep.addReports(self)
    
    def buildOutputLayerToVTLLayerMap(self):
        fileLocation = self.fileDirectory + "\\TRANSFORMATIONS_50.csv"

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
                    if scheme.startsWith("G_F") and scheme.endsWith("_FINREP_1"):
                        if expression.contains("union"):
                            indexOfExpressionOpenBracket = expression.find('(')
                            indexOfExpressionClosedBracket = expression.find(')')
                            vtl_layer_list = expression.substring(indexOfExpressionOpenBracket,indexOfExpressionClosedBracket).split(',')
                            
                            indexOfSchemeStart = expression.find('G_')
                            indexOfSchemeEnd= expression.find('UNFLDD_FINREP_1')
                            output_layer = scheme.substring(indexOfSchemeStart,indexOfSchemeEnd)
                            self.outputLayerToVTLLayerMap[output_layer, vtl_layer_list]
                            
                            
                       
        
    def addReports(self):
        fileLocation = self.fileDirectory + "\\in_scope_reports.csv"
        
        
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
                    self.viewModule.views.extend([view])
                    self.workflowModule.subProcess[0].extend([task])
                    Finrep.addLayers(self, view, task)
                    
                    
    def addLayers(self,view, task):
        fileLocation = self.fileDirectory + "\\VTL_layer_to_IL.csv"
        
        
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
                    if(input_layer == view.name):
                        selectionLayer = SelectionLayer(name = input_layer)
                        task.selectionLayers.extend([selectionLayer])
                        layerSQL = LayerSQL(name = input_layer)
                        layerSQL.selectionLayer = selectionLayer
                        layerSQL.comment = sqlfilter
                        view.selectionLayerSQL.extend([layerSQL])
                        Finrep.addColumnsToLayer(self,layerSQL)
                        
                        
    def addColumnsToLayer(self,layerSQL):
        fileLocation = self.fileDirectory + "\\cube_structure_item.csv"
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
                    

if __name__ == '__main__':
    Finrep().convert('C:\\Users\\LENOVO\\freebirdtools-develop-nov\\git\\efbt\\openregspecs\\python\\resources\\vtl_breakdown','C:\\Users\\LENOVO\\freebirdtools-develop-nov\\git\\efbt\\openregspecs\\python\\results\\')
    
            