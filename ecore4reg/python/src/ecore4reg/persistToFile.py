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
import os
from pyecore.resources import ResourceSet, URI
from pyecore.resources.json import JsonResource
from Utils import Utils


from ecore4reg import ELAttribute, ELClass, ELEnum, ELOperation, ELReference, ScriptTask


class PersistToFile:
    '''
    Documentation for PersistToFile
    '''

    def saveModelAsEcore4RegFile(self, context):
        '''
        Documentation for saveModelAsEcore4RegFile
        '''

        PersistToFile.persistEntityModel(
            self, context, context.inputLayerEntitiesPackage,
            "ecore4reg", context.inputLayerEnumsPackage)
        PersistToFile.persistEntityModel(
            self, context, context.outputLayerEntitiesPackage,
            "ecore4reg", context.outputLayerEnumsPackage)
        PersistToFile.persistEnumModel(
            self, context, context.inputLayerEnumsPackage, "ecore4reg")
        PersistToFile.persistEnumModel(
            self, context, context.outputLayerEnumsPackage, "ecore4reg")
        PersistToFile.persistTypesModel(
            self, context, context.typesPackage, "ecore4reg")
        PersistToFile.persistWorkflow(self, context)
        PersistToFile.persistGenerationTransformations(self, context)
        for package in context.logicPackages:
            PersistToFile.persistEntityModel(
                self, context, package, "ecore4reg", context.outputLayerEnumsPackage)

    def persistEntityModel(self, context, thePackage, extension, importedPackage):
        '''
        Documentation for persistEntityModel
        '''

        f = open(context.outputDirectory + os.sep + thePackage.name +
                 '.' + extension, "a",  encoding='utf-8')
        f.write("\t\t package " + thePackage.name + "\r")
        f.write("\t\t import " + importedPackage.name + ".*\r")
        if thePackage == context.outputLayerEntitiesPackage:
            for importString in context.importLogicStrings:
                f.write("\t\t import " + importString + ".*\r")
        if extension == "ecore4reg":
            f.write("\t\t import types.*\r")
        for classifier in thePackage.eClassifiers:
            if isinstance(classifier, ELClass):
                f.write("\t\t\t")
                if classifier.abstract:
                    f.write("abstract ")
                f.write("class " + classifier.name)
                if (hasattr(classifier, "superTypes") and len(classifier.superTypes) > 0):
                    f.write(" extends " + classifier.superTypes[0].name)
                f.write(" {\r")
                for member in classifier.eStructuralFeatures:

                    if isinstance(member, ELReference):
                        if member.containment:
                            f.write("\t\t\t\tcontains ")
                        else:
                            f.write("\t\t\t\trefers ")

                        f.write(member.eType.name + " ")
                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            f.write("[" + str(member.lowerBound) +
                                    ".." + str(member.upperBound) + "] ")

                        f.write(member.name)
                        f.write(" \r")
                    elif isinstance(member, ELAttribute):
                        f.write("\t\t\t\t")
                        if member.iD:
                            f.write("id ")

                        if member.eType.name == "String":
                            f.write("String  ")
                        elif member.eType.name == "Double":
                            f.write("double  ")
                        elif member.eType.name == "Integer":
                            f.write("int  ")
                        elif member.eType.name == "Date":
                            f.write("Date  ")
                        else:
                            f.write(member.eType.name + " ")

                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            f.write("[" + str(member.lowerBound) +
                                    ".." + str(member.upperBound) + "] ")

                        f.write(member.name)
                        f.write(" \r")

                for member in classifier.eOperations:
                    if isinstance(member, ELOperation):
                        f.write("\t\t\t\top ")

                        f.write(member.eType.name + " ")
                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            f.write("[" + str(member.lowerBound) +
                                    ".." + str(member.upperBound) + "] ")

                        f.write(member.name)
                        if extension == "ecore4reg" and context.addExecutableStubs:
                            if hasattr(member, "ecore4regText"):
                                f.write(
                                    "() {\n\t\t\t\t\t\"" + member.ecore4regText + "\"\n\t\t\t\t\t}")
                            else:
                                f.write("() {}")
                        else:
                            f.write("() {}")

                        f.write(" \r")

                f.write("\t\t\t}\r")

        f.close()

    def persistTypesModel(self, context, thePackage, extension):
        '''
        Documentation for persistTypesModel
        '''
        f = open(context.outputDirectory + os.sep + thePackage.name +
                 '.' + extension, "a",  encoding='utf-8')
        f.write("\t\t package " + thePackage.name + "\r")
        f.write("\t\t\ttype double wraps double\r")
        f.write("\t\t\ttype String wraps String\r")
        f.write("\t\t\ttype int wraps int\r")
        if extension == "ecore4reg":
            f.write("\t\t\ttype Date wraps Date\r")
        else:
            f.write("\t\t\ttype Date wraps java.util.Date\r")
        f.write("\t\t\ttype boolean wraps boolean\r")
        f.close()

    def persistEnumModel(self, context, thePackage, extension):
        '''
        Documentation for persistEnumModel
        '''

        f = open(context.outputDirectory + os.sep + thePackage.name +
                 '.' + extension, "a",  encoding='utf-8')
        f.write("\t\t package " + thePackage.name + "\r")
        for classifier in thePackage.eClassifiers:

            if isinstance(classifier, ELEnum):
                f.write("\t\t\tenum " + classifier.name)

                f.write(" { ")
                counter = 0
                splitcount = 1
                for theLiteral in classifier.eLiterals:
                    counter = counter+1

                    if counter < 100:
                        f.write(" " + theLiteral.name + " as \"" +
                                theLiteral.literal + "\" = " + str(theLiteral.value))
                    else:
                        counter = 0
                        splitcount = splitcount+1
                        f.write(" }\r")
                        f.write("\t\t\tenum " + classifier.name +
                                "_" + str(splitcount))
                        f.write(" { ")

                f.write(" }\r")

        f.close()

    def getVTLTextForLayer(self, context, layer):
        '''
        Documentation for getVTLTextForLayer
        '''

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
                                output = output + \
                                    PersistToFile.removeCommentChars(
                                        self, trans.expression) + "\r"
            output = output + "*/\r\r"

            output = output + "/** VTL intermediate layer specific VTL \r"
            intermediateLayer = None
            for vtl in context.vtlModule.VTLForSelectionLayers.vTLForSelectionLayers:
                if vtl.selectionLayer == layer:
                    intermediateLayer = vtl.intermediateLayer
                    for trans in intermediateLayer.transformations:
                        output = output + \
                            PersistToFile.removeCommentChars(
                                self, trans.expression) + "\r"
            output = output + "*/\r\r"

            output = output + "/** assocated enriched layer in VTL \r"
            enrichedLayer = intermediateLayer.dependant_enriched_cubes
            if not enrichedLayer is None:
                output = output + "enriched Layer :" + enrichedLayer.name + "\r"
                output = output + "enriched Layer transformations:\r"
                for exp in enrichedLayer.transformations:
                    output = output + \
                        PersistToFile.removeCommentChars(
                            self, exp.expression) + "\r"
            output = output + "*/\r\r"

            output = output + "/** associated input layer table and filter \r"
            for link in context.vtlModule.entityToVTLIntermediateLayerLinks.entityToVTLIntermediateLayerLinks:
                if link.VTLIntermediateLayer == intermediateLayer:
                    output = output + "input layer entity: " + link.entity.name + "\r"
                    output = output + "filter: " + link.filter + "\r"

            output = output + "*/\r\r"
        return output

    def getVTLTextForView(self, context, view):
        '''
        Documentation for getVTLTextForView
        '''
        output = ""
        if context.persistVTLComments:
            output = output + "/** View specific VTL \r"
            for vtl in context.vtlModule.VTLForViews.vTLForViews:
                if vtl.view == view:
                    output = output + "output layer = " + vtl.vtl.outputLayer.name + "\r"
            output = output + "*/\r\r"

        return output

    def countNonNoneLayers(self, element):
        '''
        Documentation for countNonNoneLayers
        '''
        counter = 0
        for layer in element.selectionLayers:
            if not layer.name is None:
                counter = counter + 1
        return counter

    def saveModelAsXMIFile(self, context):
        '''
         save model as an xmi file representing an object tree.
        '''
        rset = ResourceSet()

        # This will create an XMI resource
        resource = rset.create_resource(
            URI(context.outputDirectory + os.sep + 'VTL.xmi'))
        resource.append(context.moduleList)
        resource.save()

    def saveModelAsJSONFiles(self, context):
        '''
         save model as an json file representing an object tree.
        '''

        rset2 = ResourceSet()  # We have a resource set
        # we register the factory for '.json' extensions
        rset2.resource_factory['json'] = JsonResource

        inputLayerEnums_resource2 = rset2.create_resource(URI(
            context.outputDirectory + os.sep + "types.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        inputLayerEnums_resource2.append(context.typesPackage)
        inputLayerEnums_resource2.save()
        inputLayerEnums_resource2 = rset2.create_resource(URI(
            context.outputDirectory + os.sep + "input_layer_enums.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        inputLayerEnums_resource2.append(context.inputLayerEnumsPackage)
        inputLayerEnums_resource2.save()
        outputLayerEnums_resource2 = rset2.create_resource(URI(
            context.outputDirectory + os.sep + "output_layer_enums.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        outputLayerEnums_resource2.append(context.outputLayerEnumsPackage)
        outputLayerEnums_resource2.save()
        inputLayerEntities_resource2 = rset2.create_resource(URI(
            context.outputDirectory + os.sep + "input_layer_entities.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        inputLayerEntities_resource2.append(context.inputLayerEntitiesPackage)
        inputLayerEntities_resource2.save()
        outputLayerEntities_resource2 = rset2.create_resource(URI(
            context.outputDirectory + os.sep + "output_layer_entities.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        outputLayerEntities_resource2.append(
            context.outputLayerEntitiesPackage)
        outputLayerEntities_resource2.save()

    def persistWorkflow(self, context):
        '''
        Documentation for persistWorkflow
        '''
        f = open(context.outputDirectory + os.sep +
                 'workflow.ecore4reg', "a",  encoding='utf-8')
        f.write("WorkflowModule " + context.workflowModule.name + "\r{\r")
        f.write("\t\tsubProcess {\r")
        for process in context.workflowModule.subProcess:
            f.write("\t\t\t")
            f.write("SubProcess " + process.name + " flowElements {\r")
            f.write("\t\t\t\tParallelGateway gw1 outgoing (")
            counter = 0
            for element in process.flowElements:
                if counter != 0:
                    f.write(",")
                counter = counter + 1
                f.write("sf" + str(counter))
            f.write("),\r")
            counter = 0
            for element in process.flowElements:
                if isinstance(element, ScriptTask):
                    counter = counter + 1
                    f.write("\t\t\t\t")
                    f.write("ScriptTask " + element.name +
                            " incoming (sf" + str(counter) + ")")

                    counter2 = 0
                    if PersistToFile.countNonNoneLayers(self, element) > 0:
                        f.write("\r\t\t\t\t\tselectionLayers {\r")
                        for layer in element.selectionLayers:
                            if not (layer.name is None):
                                if counter2 != 0:
                                    f.write(",")
                                f.write("\r\t\t\t\t\t\tSelectionLayer " + Utils.makeValidID(str(layer.name)) +
                                        "{generatedEntity output_layer_entities." + layer.generatedEntity.name + " }")
                                counter2 = counter2+1
                        f.write("\r\t\t\t\t\t}")
                    f.write(",\r")

            counter = 0

            for element in process.flowElements:
                if not (counter == 0):
                    f.write(",\r")
                counter = counter + 1
                f.write("SequenceFlow sf" + str(counter) +
                        " from gw1 to " + element.name)
            f.write("\r}\r")
            f.write("}\r")
            f.write("}\r")
            f.close()

    def persistGenerationTransformations(self, context):
        '''
        Documentation for persistGenerationTransformations
        '''
        views = context.viewModule.views

        for view in views:
            f = open(context.outputDirectory + os.sep + view.name +
                     '_view.ecore4reg', "a",  encoding='utf-8')
            f.write("ViewModule " + view.name + "_viewModule\r{\r")
            f.write("\tviews " + "{\r")
            f.write("\t\tView " + view.name + "_view {\r")
            for layer in view.selectionLayerSQL:
                if not layer.selectionLayer.name is None:
                    f.write("\t\t\tLayerSQL {\r")
                    f.write("selectionLayer finrepWorkflow.finrepReports.task_" + view.name[5:len(
                        view.name)] + "." + Utils.makeValidID(str(layer.selectionLayer.name)) + "\r")
                    for column in layer.columns:
                        f.write("\t\t\t\tSelectMember   input_layer_enums.Group_type_domain.Internal_group as output_layer_entities." +
                                view.name[5:len(view.name)] + "_REF_OutputItem." + column.asAttribute.name + "\r")
                    f.write("\t\t\t}\r")
                    f.write(PersistToFile.getVTLTextForLayer(
                        self, context, layer))
            f.write("\t\t}\r")
            f.write("\t}\r")
            f.write("}\r")
            f.write(PersistToFile.getVTLTextForView(self, context, view))
            f.close()

    def saveModelAsEcoreFile(self, context):
        '''
         save model as an xmi file representing an object tree.
        '''
        rset2 = ResourceSet()

        inputLayerEnums_resource2 = rset2.create_resource(URI(
            context.outputDirectory + os.sep + "input_layer_enums.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        inputLayerEnums_resource2.append(context.inputLayerEnumsEcorePackage)
        inputLayerEnums_resource2.save()
        outputLayerEnums_resource2 = rset2.create_resource(URI(
            context.outputDirectory + os.sep + "output_layer_enums.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        outputLayerEnums_resource2.append(context.outputLayerEnumsEcorePackage)
        outputLayerEnums_resource2.save()
        inputLayerEntities_resource2 = rset2.create_resource(URI(
            context.outputDirectory + os.sep + "input_layer_entities.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        inputLayerEntities_resource2.append(
            context.inputLayerEntitiesEcorePackage)
        inputLayerEntities_resource2.save()
        outputLayerEntities_resource2 = rset2.create_resource(URI(
            context.outputDirectory + os.sep + "output_layer_entities.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        outputLayerEntities_resource2.append(
            context.outputLayerEntitiesEcorePackage)
        outputLayerEntities_resource2.save()

        PersistToFile.hotFix(self, context.outputDirectory +
                             os.sep + "input_layer_enums.ecore")
        PersistToFile.hotFix(self, context.outputDirectory +
                             os.sep + "output_layer_enums.ecore")
        PersistToFile.hotFix(self, context.outputDirectory +
                             os.sep + "input_layer_entities.ecore")
        PersistToFile.hotFix(self, context.outputDirectory +
                             os.sep + "output_layer_entities.ecore")

    def hotFix(self, fileName):
        '''
        Documentation for hotFix
        '''
        f = open(fileName, "r",encoding='utf-8')
        text = f.read()
        amendedText = text.replace('ecore4reg:EEnum', 'ecore:EEnum')
        f.close()
        f1 = open(fileName, "w",encoding='utf-8')
        f1.write(amendedText)
        f.close()

    def removeCommentChars(self, theString):
        '''
        Documentation for removeCommentChars
        '''
        return theString.replace("/**", "").replace("/*", "").replace("*/", "")
