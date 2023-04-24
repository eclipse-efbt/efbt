# coding=UTF-8#
# Copyright (c) 2020 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDE-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#
'''
Created on 22 Jan 2022

@author: Neil
'''
import csv


import os

from ecore4reg import SubProcess,ELClass, ELOperation, EntityToVTLIntermediateLayerLink, EntityToVTLIntermediateLayerLinkModule, LayerSQL, ScriptTask, SelectColumnAttributeAs, SelectionLayer, VTLForOutputLayerAndIntermediateLayerCombination, VTLForSelectionLayer, VTLForSelectionLayerModule, VTLForView, VTLForViewModule, VTLGeneratedIntermediateLayer, VTLGeneratedIntermediateLayerModule, VTLGeneratedOutputlayer, VTLGeneratedOutputlayerModule, VTLScheme, VTLSchemesModule, VTLTransformation, View


class ImportFinrepVTL(object):
    '''
    Documentation for ImportFinrepVTL
    '''

    def doImport(self, context):
        '''
        Documentation for doImport
        '''
        # ImportFinrepVTL.buildOutputLayerToVTLLayerMap(self,context)
        subProcess = SubProcess(name="finrepReports")
        context.workflowModule.subProcess.extend([subProcess])
        # ImportFinrepVTL.addReports(self,context)
        ImportFinrepVTL.initialiseVTLModule(self, context)
        ImportFinrepVTL.importFinrepEIL_to_ROLTransformationSchemes(
            self, context)
        ImportFinrepVTL.importTransformations(self, context)
        ImportFinrepVTL.buildListOfEnrichedLayers(self, context)
        ImportFinrepVTL.buildListOfIntermediateFinrepLayers(self, context)
        ImportFinrepVTL.buildROLToIntermediateLayerLink(self, context)
        ImportFinrepVTL.buildIntermediateLayerToInputLayer(self, context)
        ImportFinrepVTL.addReports(self, context)

    def initialiseVTLModule(self, context):
        '''
        Doc for initialiseVTLModule
        '''
        vtlModule = context.vtlModule
        vtlModule.VTLSchemes = VTLSchemesModule(name="VTLSchemes")
        vtlModule.VTLGeneratedOutputLayers = VTLGeneratedOutputlayerModule(
            name="VTLGeneratedOutputLayers")
        vtlModule.VTLGeneratedIntermediateLayers = VTLGeneratedIntermediateLayerModule(
            name="VTLGeneratedIntermediateLayers")
        vtlModule.VTLEnrichedLayers = VTLGeneratedIntermediateLayer(
            name="VTLEnrichedLayers")
        vtlModule.VTLForSelectionLayers = VTLForSelectionLayerModule(
            name="VTLForSelectionLayers")
        vtlModule.entityToVTLIntermediateLayerLinks = EntityToVTLIntermediateLayerLinkModule(
            name="entityToVTLIntermediateLayerLinks")
        vtlModule.VTLForViews = VTLForViewModule(name="VTLForViews")

    def importFinrepEIL_to_ROLTransformationSchemes(self, context):
        '''
        Doc for importFinrepEIL_to_ROLTransformationSchemes
        '''
        fileLocation = context.fileDirectory + os.sep + "transformation_scheme.csv"

        headerSkipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(fileLocation, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not headerSkipped):
                    headerSkipped = True
                else:
                    transformation_scheme_id = row[6]
                    phase = row[5]

                    if (transformation_scheme_id.find('_FINREP') > 0 and phase == 'EIL_ROL') or (transformation_scheme_id.startswith('P_') and transformation_scheme_id.endswith('_E_1')):
                        vtlScheme = VTLScheme()
                        vtlScheme.scheme_id = transformation_scheme_id
                        context.vtlModule.VTLSchemes.vTLSchemes.append(
                            vtlScheme)

    def importTransformations(self, context):
        '''
        Doc for importTransformations
        '''
        fileLocation = context.fileDirectory + os.sep + "transformations.csv"

        headerSkipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(fileLocation, encoding='utf-8') as csvfile:
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
                    vtlScheme = ImportFinrepVTL.lookupScheme(
                        self, context, scheme)

                    if not (vtlScheme is None):

                        transformation = VTLTransformation()
                        transformation.transformation_id = transformationID
                        transformation.description = description
                        transformation.order_in_scheme = order
                        transformation.expression = expression
                        vtlScheme.expressions.append(transformation)

                    # if scheme.startswith("G_F") and scheme.endswith("_FINREP_1"):
                    #    if "union" in expressio

    def schemesContains(self, context, scheme_id):
        '''
        Doc for schemesContains
        '''
        for scheme in context.vtlModule.VTLSchemes.vTLSchemes:
            if scheme.scheme_id == scheme_id:
                return True
        return False

    def lookupScheme(self, context, scheme_id):
        '''
        Doc for lookupScheme
        '''
        for scheme in context.vtlModule.VTLSchemes.vTLSchemes:
            if scheme.scheme_id == scheme_id:
                return scheme
        return None

    def buildROLToIntermediateLayerLink(self, context):
        '''
        Doc for buildROLToIntermediateLayerLink
        '''
        for scheme in context.vtlModule.VTLSchemes.vTLSchemes:

            if scheme.scheme_id.startswith("G_F") and scheme.scheme_id.endswith("_REF_UNFLDD_FINREP_1"):

                indexOfSchemeEnd = scheme.scheme_id.find(
                    '_REF_UNFLDD_FINREP_1')
                output_layer_name = scheme.scheme_id[2:indexOfSchemeEnd]

                outputLayer = VTLGeneratedOutputlayer(name=output_layer_name)

                outputLayer.outputLayer = ImportFinrepVTL.findEntity(
                    self, context, output_layer_name + "_REF_OutputItem")

                context.vtlModule.VTLGeneratedOutputLayers.vTLGeneratedOutputlayerModules.append(
                    outputLayer)
                for transformation in scheme.expressions:
                    expression = transformation.expression
                    if "union" in expression:
                        indexOfExpressionOpenBracket = expression.find('(')
                        indexOfExpressionClosedBracket = expression.find(')')
                        print(indexOfExpressionOpenBracket)
                        print(indexOfExpressionClosedBracket)
                        vtl_layer_list = expression[indexOfExpressionOpenBracket +
                                                    1:indexOfExpressionClosedBracket].split(',')
                        for vtl_layer in vtl_layer_list:


                            intermediateLayer = ImportFinrepVTL.findIntermediateLayer(
                                self, context, vtl_layer.strip())

                            if not (intermediateLayer is None):
                                outputLayer.dependant_intermediate_layers.append(
                                    intermediateLayer)
                                combo = VTLForOutputLayerAndIntermediateLayerCombination()
                                combo.outputLayer = outputLayer
                                combo.intermediateLayer = intermediateLayer
                                combo.name = outputLayer.name + '_' + intermediateLayer.name
                                # get the special commands specific to the output layer and intermediate layer
                                for trans in scheme.expressions:
                                    indexOfColon = trans.expression.find(':')
                                    lhs = trans.expression[0:indexOfColon].strip(
                                    )
                                    if (lhs) == intermediateLayer.name:
                                        combo.transformations.append(trans)
                                outputLayer.VTLForOutputLayerAndIntemedateLayerCombinations.append(
                                    combo)

    def findEntity(self, context, outputLayerName):
        '''
        Doc for findEntity
        '''
        for theEntity in context.inputLayerEntitiesPackage.eClassifiers:
            if isinstance(theEntity, ELClass):
                if theEntity.name == outputLayerName:
                    return theEntity

        for theEntity in context.outputLayerEntitiesPackage.eClassifiers:
            if isinstance(theEntity, ELClass):
                if theEntity.name == outputLayerName:
                    return theEntity

        return None

    def findIntermediateLayer(self, context, layerName):
        '''
        Doc for findIntermediateLayer
        '''

        # print("layerName")
        # print(layerName)
        for layer in context.vtlModule.VTLGeneratedIntermediateLayers.vTLGeneratedIntermediateLayers:
            # print(layer.transformations.scheme_id)
            # print("layer.transformations.scheme_id")
            if layer.name == layerName:
                return layer

        return None

    def buildOutputLayerToVTLLayerMap(self, context):
        '''
        Doc for buildOutputLayerToVTLLayerMap
        '''
        fileLocation = context.fileDirectory + os.sep + "transformations.csv"

        headerSkipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(fileLocation, encoding='utf-8') as csvfile:
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
                            # print("expression")
                            # print(expression)
                            indexOfExpressionOpenBracket = expression.find('(')
                            indexOfExpressionClosedBracket = expression.find(
                                ')')
                            # print(indexOfExpressionOpenBracket)
                            # print(indexOfExpressionClosedBracket)
                            vtl_layer_list = expression[indexOfExpressionOpenBracket:indexOfExpressionClosedBracket].split(
                                ',')

                            indexOfSchemeStart = expression.find('G_')
                            indexOfSchemeEnd = expression.find(
                                'UNFLDD_FINREP_1')
                            output_layer = scheme[indexOfSchemeStart:indexOfSchemeEnd]
                            context.outputLayerToVTLLayerMap[output_layer,
                                                             vtl_layer_list]

    def buildListOfEnrichedLayers(self, context):
        '''
        Doc for buildListOfEnrichedLayers
        '''
        for scheme in context.vtlModule.VTLSchemes.vTLSchemes:

            if (scheme.scheme_id.startswith("P_") and scheme.scheme_id.endswith("_E_1")):
                ImportFinrepVTL.buildListOfEnrichedLayersForScheme(
                    self, scheme, context)

    def buildListOfIntermediateFinrepLayers(self, context):
        '''
        Doc for buildListOfIntermediateFinrepLayers
        '''
        for scheme in context.vtlModule.VTLSchemes.vTLSchemes:

            if not (scheme.scheme_id.startswith("G_F_")) and (not ((scheme.scheme_id.startswith("P_") and scheme.scheme_id.endswith("_E_1")))):
                ImportFinrepVTL.buildListOfIntermediateFinrepLayersForScheme(
                    self, scheme, context)

    def buildListOfEnrichedLayersForScheme(self, scheme, context):
        '''
        Doc for buildListOfEnrichedLayersForScheme
        '''
        for transformation in scheme.expressions:

            theIntermediateLayer = None
            for enrichedLayer in context.vtlModule.VTLEnrichedLayers.vTLGeneratedIntermediateLayers:
                intermediateLayerName = enrichedLayer.name
                if intermediateLayerName == scheme.scheme_id:
                    theIntermediateLayer = enrichedLayer

            if theIntermediateLayer is None:
                theIntermediateLayer = VTLGeneratedIntermediateLayer(
                    name=scheme.scheme_id)
                theIntermediateLayer.isEnrichment = True
                context.vtlModule.VTLEnrichedLayers.vTLGeneratedIntermediateLayers.append(
                    theIntermediateLayer)

            theIntermediateLayer.transformations.append(transformation)

    def buildListOfIntermediateFinrepLayersForScheme(self, scheme, context):
        '''
        Doc for buildListOfIntermediateFinrepLayersForScheme
        '''
        for transformation in scheme.expressions:
            transformationText = transformation.expression
            indexOfLHSEnd = transformationText.find(' :=')
            expressionLHS = ImportFinrepVTL.stripComment(
                self, transformationText[0:indexOfLHSEnd])
            theIntermediateLayer = None
            for intermediateLayer in context.vtlModule.VTLGeneratedIntermediateLayers.vTLGeneratedIntermediateLayers:
                intermediateLayerName = intermediateLayer.name
                if intermediateLayerName == expressionLHS:
                    theIntermediateLayer = intermediateLayer

            if theIntermediateLayer is None:
                theIntermediateLayer = VTLGeneratedIntermediateLayer(
                    name=expressionLHS)
                theIntermediateLayer.isEnrichment = False
                theIntermediateLayer.dependant_enriched_cubes = ImportFinrepVTL.findEnrichedCubeFor(
                    self, context, expressionLHS)
                context.vtlModule.VTLGeneratedIntermediateLayers.vTLGeneratedIntermediateLayers.append(
                    theIntermediateLayer)

            theIntermediateLayer.transformations.append(transformation)

    def stripComment(self, text):
        '''
        Doc for stripComment
        '''
        endCommentIndex = text.find('*/')
        if endCommentIndex > 0:
            return text[endCommentIndex + 2:len(text)]
        else:
            return text

    def findEnrichedCubeFor(self, context, scheme_id):
        '''
        Doc for findEnrichedCubeFor
        '''
        indexOfSchemeEnd = scheme_id.find('_FINREP')
        enrichedCube = "P_" + scheme_id[0:indexOfSchemeEnd] + "_E_1"
        print("enrichedCube")
        print(enrichedCube)

        returnLayer = None
        for enrichedLayer in context.vtlModule.VTLEnrichedLayers.vTLGeneratedIntermediateLayers:

            if enrichedLayer.name == enrichedCube:
                returnLayer = enrichedLayer

        return returnLayer

    def addReports(self, context):
        '''
        Doc for addReports
        '''
        fileLocation = context.fileDirectory + os.sep + "in_scope_reports.csv"

        headerSkipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not headerSkipped):
                    headerSkipped = True
                else:
                    reportTemplate = row[0]
                    new = row[1]
                    view = View(name="view_" + reportTemplate)
                    context.viewModule.views.append(view)

                    task = ScriptTask(name="task_" + reportTemplate)
                    context.workflowModule.subProcess[0].flowElements.append(
                        task)
                    ImportFinrepVTL.addLayers(self, context, view, task)

    def addLayers(self, context, view, task):
        '''
        Doc for addLayers
        '''

        # print("view.name")
        # print(view.name[5:len(view.name)] )
        rolVTL = ImportFinrepVTL.findOutputLayerVTL(
            self, context, view.name[5:len(view.name)] + "_REF_OutputItem")
        # print("rolVTL")
        # print(rolVTL)
        if not (rolVTL is None):
            view.outputLayer = rolVTL.outputLayer
            viewVTL = VTLForView(name="vtl_" + view.name)
            viewVTL.view = view
            viewVTL.vtl = rolVTL
            context.vtlModule.VTLForViews.vTLForViews.append(viewVTL)

            for intermediateLayer in rolVTL.dependant_intermediate_layers:

                # vtlLayer = intermediateLayer.transformations
                link = ImportFinrepVTL.findEntityIntermediateLayerLink(
                    self, context, intermediateLayer)
                if not (link is None):
                    input_layer = link.entity
                    selectionLayer = SelectionLayer()
                    if input_layer is not None:
                        selectionLayer.name = view.outputLayer.name + \
                            "_" + link.VTLIntermediateLayer.name
                    selectionLayer.generatedEntity = rolVTL.outputLayer
                    task.selectionLayers.extend([selectionLayer])
                    layerSQL = LayerSQL()
                    layerSQL.selectionLayer = selectionLayer
                    vtlForSelectionLayer = VTLForSelectionLayer()
                    vtlForSelectionLayer.selectionLayer = layerSQL
                    vtlForSelectionLayer.outputLayer = rolVTL
                    vtlForSelectionLayer.intermediateLayer = intermediateLayer
                    context.vtlModule.VTLForSelectionLayers.vTLForSelectionLayers.append(
                        vtlForSelectionLayer)

                    view.selectionLayerSQL.extend([layerSQL])
                    ImportFinrepVTL.addColumnsToLayer(self, context, layerSQL)

    def findOutputLayerVTL(self, context, outputLayerName):
        '''
        Doc for findOutputLayerVTL
        '''
        # print("outputLayerName")
        # print(outputLayerName)
        for rol in context.vtlModule.VTLGeneratedOutputLayers.vTLGeneratedOutputlayerModules:
            if not (rol.outputLayer is None):
                # print("rol.outputLayer.name")
                # print(rol.outputLayer.name)
                a = 0
            if not (rol.outputLayer is None) and (rol.outputLayer.name == outputLayerName):
                return rol

    def findEntityIntermediateLayerLink(self, context, intermediateLayer):
        '''
        Doc for findEntityIntermediateLayerLink
        '''
        # print("intermediateLayer")
        # print(intermediateLayer)
        for link in context.vtlModule.entityToVTLIntermediateLayerLinks.entityToVTLIntermediateLayerLinks:
            if link.VTLIntermediateLayer == intermediateLayer:
                return link

    def addColumnsToLayer(self, context, layerSQL):
        '''
        Doc for addColumnsToLayer
        '''

        outputLayer = layerSQL.selectionLayer.generatedEntity
        if not (outputLayer is None):
            for member in outputLayer.eOperations:
                if isinstance(member, ELOperation):
                    selectColumn = SelectColumnAttributeAs()
                    selectColumn.asAttribute = member
                    layerSQL.columns.extend([selectColumn])

    def buildIntermediateLayerToInputLayer(self, context):
        '''
        Doc for buildIntermediateLayerToInputLayer
        '''
        fileLocation = context.fileDirectory + os.sep + "VTL_layer_to_IL.csv"
        # il_name = layerSQL.name
        # amended_il_name = "FINREP_REF_" + il_name + "_REF_FINREP 3.0"

        headerSkipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
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
                    link.VTLIntermediateLayer = ImportFinrepVTL.findIntermediateLayer(
                        self, context, vtlLayer.strip() + "_FINREP")
                    link.entity = ImportFinrepVTL.findEntity(
                        self, context, inputLayer)
                    link.filter = filter
                    link.filterName = filterName
                    context.vtlModule.entityToVTLIntermediateLayerLinks.entityToVTLIntermediateLayerLinks.append(
                        link)
