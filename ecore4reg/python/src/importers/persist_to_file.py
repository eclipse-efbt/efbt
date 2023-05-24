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
from importers.utils import Utils


from ecore4reg import ELAttribute, ELClass, ELEnum, ELOperation, ELReference


class PersistToFile:
    '''
    Documentation for PersistToFile
    '''

    def save_model_as_ecore4reg_file(self, context):
        '''
        Save resources as Ecore4Reg files
        '''

        PersistToFile.persist_entity_model(
            self, context, context.input_layer_entities_package,
            "ecore4reg", context.input_layer_enums_package)
        PersistToFile.persist_entity_model(
            self, context, context.output_layer_entities_package,
            "ecore4reg", context.output_layer_enums_package)
        PersistToFile.persist_enum_model(
            self, context, context.input_layer_enums_package, "ecore4reg")
        PersistToFile.persist_enum_model(
            self, context, context.output_layer_enums_package, "ecore4reg")
        PersistToFile.persist_types_model(
            self, context, context.types_package, "ecore4reg")
        PersistToFile.persist_generation_transformations(self, context)
        
    def save_model_as_xcore_file(self, context):
        '''
        Save resources as XCore files
        '''

        PersistToFile.persist_entity_model(
            self, context, context.input_layer_entities_package,
            "xcore", context.input_layer_enums_package)
        PersistToFile.persist_entity_model(
            self, context, context.output_layer_entities_package,
            "xcore", context.output_layer_enums_package)
        PersistToFile.persist_enum_model(
            self, context, context.input_layer_enums_package, "xcore")
        PersistToFile.persist_enum_model(
            self, context, context.output_layer_enums_package, "xcore")
        

    def persist_entity_model(self, context, the_package, extension, imported_package):
        '''
        Documentation for persist_entity_model
        '''

        f = open(context.output_directory + os.sep + extension +
                 os.sep + the_package.name + '.' + extension,
                 "a",  encoding='utf-8')
        f.write("\t\t package " + the_package.name + "\r")
        f.write("\t\t import " + imported_package.name + ".*\r")
        if the_package == context.output_layer_entities_package:
            for import_string in context.importLogicStrings:
                f.write("\t\t import " + import_string + ".*\r")
        if extension == "ecore4reg":
            f.write("\t\t import types.*\r")
        for classifier in the_package.eClassifiers:
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
                            upperBoundNumber = member.upperBound
                            upperBoundString = str(member.upperBound)
                            if (extension == "xcore") and (upperBoundNumber == -1):
                                upperBoundString = "*"
                            
                            f.write("[" + str(member.lowerBound) +
                                    ".." + upperBoundString + "] ")

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
                            upperBoundNumber = member.upperBound
                            upperBoundString = str(member.upperBound)
                            if (extension == "xcore") and (upperBoundNumber == -1):
                                upperBoundString = "*"
                            
                            f.write("[" + str(member.lowerBound) +
                                    ".." + upperBoundString + "] ")

                        f.write(member.name)
                        f.write(" \r")

                for member in classifier.eOperations:
                    if isinstance(member, ELOperation):
                        f.write("\t\t\t\top ")

                        f.write(member.eType.name + " ")
                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            upperBoundNumber = member.upperBound
                            upperBoundString = str(member.upperBound)
                            if (extension == "xcore") and (upperBoundNumber == -1):
                                upperBoundString = "*"
                            
                            f.write("[" + str(member.lowerBound) +
                                    ".." + upperBoundString + "] ")
                            
                        f.write(member.name)
                        if (extension == "xcore") and (member.eType.name == "int"):
                            f.write("() { return 0}")
                        else:
                            f.write("() {}")
                            
                        f.write(" \r")

                f.write("\t\t\t}\r")

        f.close()

    def persist_types_model(self, context, the_package, extension):
        '''
        Documentation for persist_types_model
        '''
        f = open(context.output_directory + os.sep + extension +
                 os.sep + the_package.name +
                 '.' + extension, "a",  encoding='utf-8')
        f.write("\t\t package " + the_package.name + "\r")
        f.write("\t\t\ttype double wraps double\r")
        f.write("\t\t\ttype String wraps String\r")
        f.write("\t\t\ttype int wraps int\r")
        if extension == "ecore4reg":
            f.write("\t\t\ttype Date wraps Date\r")
        else:
            f.write("\t\t\ttype Date wraps java.util.Date\r")
        f.write("\t\t\ttype boolean wraps boolean\r")
        f.close()

    def persist_enum_model(self, context, the_package, extension):
        '''
        Documentation for persist_enum_model
        '''

        f = open(context.output_directory + os.sep + extension +
                 os.sep + the_package.name +
                 '.' + extension, "a",  encoding='utf-8')
        f.write("\t\t package " + the_package.name + "\r")
        for classifier in the_package.eClassifiers:

            if isinstance(classifier, ELEnum):
                f.write("\t\t\tenum " + classifier.name)

                f.write(" { ")
                counter = 0
                splitcount = 1
                for the_literal in classifier.eLiterals:
                    counter = counter+1

                    if counter < 100:
                        f.write(" " + the_literal.name + " as \"" +
                                the_literal.literal + "\" = " + str(the_literal.value))
                    else:
                        counter = 0
                        splitcount = splitcount+1
                        f.write(" }\r")
                        f.write("\t\t\tenum " + classifier.name +
                                "_" + str(splitcount))
                        f.write(" { ")

                f.write(" }\r")

        f.close()

    def get_vtl_text_for_layer(self, context, layer):
        '''
        Documentation for get_vtl_text_for_layer
        '''

        output = ""
        if context.persist_vtl_comments:
            output = output + "/** VTL intermediate layer and report combination specific VTL \r"
            intermediate_layer = None
            for vtl in context.vtl_module.VTLForSelectionLayers.vTLForSelectionLayers:
                if vtl.selectionLayer == layer:
                    intermediate_layer = vtl.intermediateLayer
                    for combo in vtl.outputLayer.VTLForOutputLayerAndIntemedateLayerCombinations:
                        if (combo.intermediateLayer == vtl.intermediateLayer) and (combo.outputLayer.outputLayer == layer.selectionLayer.generatedEntity):
                            for trans in combo.transformations:
                                output = output + \
                                    PersistToFile.remove_comment_chars(
                                        self, trans.expression) + "\r"
            output = output + "*/\r\r"

            output = output + "/** VTL intermediate layer specific VTL \r"
            intermediate_layer = None
            for vtl in context.vtl_module.VTLForSelectionLayers.vTLForSelectionLayers:
                if vtl.selectionLayer == layer:
                    intermediate_layer = vtl.intermediateLayer
                    for trans in intermediate_layer.transformations:
                        output = output + \
                            PersistToFile.remove_comment_chars(
                                self, trans.expression) + "\r"
            output = output + "*/\r\r"

            output = output + "/** assocated enriched layer in VTL \r"
            enriched_layer = intermediate_layer.dependant_enriched_cubes
            if not enriched_layer is None:
                output = output + "enriched Layer :" + enriched_layer.name + "\r"
                output = output + "enriched Layer transformations:\r"
                for exp in enriched_layer.transformations:
                    output = output + \
                        PersistToFile.remove_comment_chars(
                            self, exp.expression) + "\r"
            output = output + "*/\r\r"

            output = output + "/** associated input layer table and filter \r"
            for link in context.vtl_module.entityToVTLIntermediateLayerLinks.entityToVTLIntermediateLayerLinks:
                if link.VTLIntermediateLayer == intermediate_layer:
                    output = output + "input layer entity: " + link.entity.name + "\r"
                    output = output + "filter: " + link.filter + "\r"

            output = output + "*/\r\r"
        return output

    def get_vtl_text_for_view(self, context, view):
        '''
        Documentation for get_vtl_text_for_view
        '''
        output = ""
        if context.persist_vtl_comments:
            output = output + "/** View specific VTL \r"
            for vtl in context.vtl_module.VTLForViews.vTLForViews:
                if vtl.view == view:
                    output = output + "output layer = " + vtl.vtl.outputLayer.name + "\r"
            output = output + "*/\r\r"

        return output

    def count_non_none_layers(self, element):
        '''
        Documentation for count_non_none_layers
        '''
        counter = 0
        for layer in element.selectionLayers:
            if not layer.name is None:
                counter = counter + 1
        return counter

    def save_model_as_xmi_file(self, context):
        '''
         save model as an xmi file representing an object tree.
        '''
        rset = ResourceSet()

        # This will create an XMI resource
        resource = rset.create_resource(
            URI(context.output_directory + os.sep + 'xmi' + os.sep + 'VTL.xmi'))
        resource.append(context.module_list)
        resource.save()

    def save_model_as_json_files(self, context):
        '''
         save model as an json file representing an object tree.
        '''
        extension = 'json'
        rset2 = ResourceSet()  # We have a resource set
        # we register the factory for '.json' extensions
        rset2.resource_factory['json'] = JsonResource

        input_layer_enums_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "types.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        input_layer_enums_resource2.append(context.types_package)
        input_layer_enums_resource2.save()
        input_layer_enums_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "input_layer_enums.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        input_layer_enums_resource2.append(context.input_layer_enums_package)
        input_layer_enums_resource2.save()
        output_layer_enums_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "output_layer_enums.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        output_layer_enums_resource2.append(context.output_layer_enums_package)
        output_layer_enums_resource2.save()
        input_layer_entities_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "input_layer_entities.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        input_layer_entities_resource2.append(
            context.input_layer_entities_package)
        input_layer_entities_resource2.save()
        output_layer_entities_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "output_layer_entities.json"))
        # This will create an JSON resource
        # we add the EPackage instance in the resource
        output_layer_entities_resource2.append(
            context.output_layer_entities_package)
        output_layer_entities_resource2.save()

   

    def persist_generation_transformations(self, context):
        '''
        Documentation for persist_generation_transformations
        '''
        views = context.view_module.views

        for view in views:
            f = open(context.output_directory + os.sep + 'ecore4reg' +
                     os.sep + view.name +
                     '_view.ecore4reg', "a",  encoding='utf-8')
            f.write("ViewModule " + view.name + "_viewModule\r{\r")
            f.write("\tviews " + "{\r")
            f.write("\t\tView " + view.name + "_view {\r")
            for layer in view.selectionLayerSQL:
                if not layer.selectionLayer.name is None:
                    f.write("\t\t\tLayerSQL {\r")
                    f.write("selectionLayer SelectionLayer " + Utils.make_valid_id(str(layer.selectionLayer.name)) + "\r")
                    for column in layer.columns:
                        f.write("\t\t\t\tSelectValue \"TODO\" as output_layer_entities." +
                                view.name[5:len(view.name)] + "_REF_OutputItem." + column.asAttribute.name + "\r")
                    f.write("\t\t\t}\r")
                    f.write(PersistToFile.get_vtl_text_for_layer(
                        self, context, layer))
            f.write("\t\t}\r")
            f.write("\t}\r")
            f.write("}\r")
            f.write(PersistToFile.get_vtl_text_for_view(self, context, view))
            f.close()

    def save_model_as_ecore_file(self, context):
        '''
         save model as an xmi file representing an object tree.
        '''
        rset2 = ResourceSet()
        extension = 'ecore'
        input_layer_enums_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "input_layer_enums.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        input_layer_enums_resource2.append(context.input_layer_enums_ecore_package)
        input_layer_enums_resource2.save()
        output_layer_enums_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "output_layer_enums.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        output_layer_enums_resource2.append(context.output_layer_enums_ecore_package)
        output_layer_enums_resource2.save()
        input_layer_entities_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "input_layer_entities.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        input_layer_entities_resource2.append(
            context.input_layer_entities_ecore_package)
        input_layer_entities_resource2.save()
        output_layer_entities_resource2 = rset2.create_resource(URI(
            context.output_directory + os.sep + extension +
            os.sep + "output_layer_entities.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        output_layer_entities_resource2.append(
            context.output_layer_entities_ecore_package)
        output_layer_entities_resource2.save()

        PersistToFile.hot_fix(self, context.output_directory +
                              os.sep + extension +
                              os.sep + "input_layer_enums.ecore")
        PersistToFile.hot_fix(self, context.output_directory +
                              os.sep + extension +
                              os.sep + "output_layer_enums.ecore")
        PersistToFile.hot_fix(self, context.output_directory +
                              os.sep + extension +
                              os.sep + "input_layer_entities.ecore")
        PersistToFile.hot_fix(self, context.output_directory +
                              os.sep + extension +
                              os.sep + "output_layer_entities.ecore")

    def hot_fix(self, file_name):
        '''
        Documentation for hot_fix
        '''
        f = open(file_name, "r", encoding='utf-8')
        text = f.read()
        amended_text = text.replace('ecore4reg:EEnum', 'ecore:EEnum')
        f.close()
        f1 = open(file_name, "w", encoding='utf-8')
        f1.write(amended_text)
        f.close()

    def remove_comment_chars(self, the_string):
        '''
        Documentation for remove_comment_chars
        '''
        return the_string.replace("/**", "").replace("/*", "").replace("*/", "")
