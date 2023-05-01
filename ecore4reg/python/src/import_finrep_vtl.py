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

from ecore4reg import SubProcess, ELClass, ELOperation
from ecore4reg import VTLSchemesModule, VTLTransformation, View
from ecore4reg import EntityToVTLIntermediateLayerLink
from ecore4reg import EntityToVTLIntermediateLayerLinkModule
from ecore4reg import LayerSQL, ScriptTask, SelectColumnAttributeAs
from ecore4reg import SelectionLayer
from ecore4reg import VTLForOutputLayerAndIntermediateLayerCombination
from ecore4reg import VTLForSelectionLayer, VTLForSelectionLayerModule
from ecore4reg import VTLForView, VTLForViewModule
from ecore4reg import VTLGeneratedIntermediateLayer
from ecore4reg import VTLGeneratedIntermediateLayerModule
from ecore4reg import VTLGeneratedOutputlayer
from ecore4reg import VTLGeneratedOutputlayerModule, VTLScheme


class ImportFinrepVTL(object):
    '''
    Documentation for ImportFinrepVTL
    '''

    def do_import(self, context):
        '''
        Documentation for doImport
        '''
        # ImportFinrepVTL.buildOutputLayerToVTLLayerMap(self,context)
        sub_process = SubProcess(name="finrepReports")
        context.workflowModule.subProcess.extend([sub_process])
        # ImportFinrepVTL.addReports(self,context)
        ImportFinrepVTL.initialise_vtl_Module(self, context)
        ImportFinrepVTL.import_finrep_eil_to_rol_transformation_schemes(
            self, context)
        ImportFinrepVTL.import_transformations(self, context)
        ImportFinrepVTL.build_list_of_enriched_layers(self, context)
        ImportFinrepVTL.build_list_of_intermediate_finrep_layers(self, context)
        ImportFinrepVTL.build_rol_to_intermediate_layer_link(self, context)
        ImportFinrepVTL.build_intermediate_layer_to_input_layer(self, context)
        ImportFinrepVTL.add_reports(self, context)

    def initialise_vtl_Module(self, context):
        '''
        Doc for initialiseVTLModule
        '''
        vtl_module = context.vtl_module
        vtl_module.VTLSchemes = VTLSchemesModule(name="VTLSchemes")
        vtl_module.VTLGeneratedOutputLayers = VTLGeneratedOutputlayerModule(
            name="VTLGeneratedOutputLayers")
        vtl_module.VTLGeneratedIntermediateLayers = VTLGeneratedIntermediateLayerModule(
            name="VTLGeneratedIntermediateLayers")
        vtl_module.VTLEnrichedLayers = VTLGeneratedIntermediateLayerModule(
            name="VTLEnrichedLayers")
        vtl_module.VTLForSelectionLayers = VTLForSelectionLayerModule(
            name="VTLForSelectionLayers")
        vtl_module.entityToVTLIntermediateLayerLinks = EntityToVTLIntermediateLayerLinkModule(
            name="entityToVTLIntermediateLayerLinks")
        vtl_module.VTLForViews = VTLForViewModule(name="VTLForViews")

    def import_finrep_eil_to_rol_transformation_schemes(self, context):
        '''
        Doc for importFinrepEIL_to_ROLTransformationSchemes
        '''
        file_location = context.file_directory + os.sep + "transformation_scheme.csv"

        header_skipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not header_skipped):
                    header_skipped = True
                else:
                    transformation_scheme_id = row[6]
                    phase = row[5]

                    if (transformation_scheme_id.find('_FINREP') > 0 and phase == 'EIL_ROL') or (transformation_scheme_id.startswith('P_') and transformation_scheme_id.endswith('_E_1')):
                        vtl_scheme = VTLScheme()
                        vtl_scheme.scheme_id = transformation_scheme_id
                        context.vtl_module.VTLSchemes.vTLSchemes.append(
                            vtl_scheme)

    def import_transformations(self, context):
        '''
        Doc for importTransformations
        '''
        file_location = context.file_directory + os.sep + "transformations.csv"

        header_skipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not header_skipped):
                    header_skipped = True
                else:
                    expression = row[2]
                    scheme = row[7]
                    description = row[1]
                    transformation_id = row[6]
                    order = row[5]
                    vtl_scheme = None
                    vtl_scheme = ImportFinrepVTL.lookup_scheme(
                        self, context, scheme)

                    if not (vtl_scheme is None):

                        transformation = VTLTransformation()
                        transformation.transformation_id = transformation_id
                        transformation.description = description
                        transformation.order_in_scheme = order
                        transformation.expression = expression
                        vtl_scheme.expressions.append(transformation)

                    # if scheme.startswith("G_F") and scheme.endswith("_FINREP_1"):
                    #    if "union" in expressio

    def schemes_contains(self, context, scheme_id):
        '''
        Doc for schemesContains
        '''
        for scheme in context.vtl_module.VTLSchemes.vTLSchemes:
            if scheme.scheme_id == scheme_id:
                return True
        return False

    def lookup_scheme(self, context, scheme_id):
        '''
        Doc for lookupScheme
        '''
        for scheme in context.vtl_module.VTLSchemes.vTLSchemes:
            if scheme.scheme_id == scheme_id:
                return scheme
        return None

    def build_rol_to_intermediate_layer_link(self, context):
        '''
        Doc for buildROLToIntermediateLayerLink
        '''
        for scheme in context.vtl_module.VTLSchemes.vTLSchemes:

            if scheme.scheme_id.startswith("G_F") and scheme.scheme_id.endswith("_REF_UNFLDD_FINREP_1"):

                index_of_scheme_end = scheme.scheme_id.find(
                    '_REF_UNFLDD_FINREP_1')
                output_layer_name = scheme.scheme_id[2:index_of_scheme_end]

                output_layer = VTLGeneratedOutputlayer(name=output_layer_name)

                output_layer.outputLayer = ImportFinrepVTL.find_entity(
                    self, context, output_layer_name + "_REF_OutputItem")

                context.vtl_module.VTLGeneratedOutputLayers.vTLGeneratedOutputlayerModules.append(
                    output_layer)
                for transformation in scheme.expressions:
                    expression = transformation.expression
                    if "union" in expression:
                        index_of_expression_open_bracket = expression.find('(')
                        index_of_expression_closed_bracket = expression.find(
                            ')')

                        vtl_layer_list = expression[index_of_expression_open_bracket +
                                                    1:index_of_expression_closed_bracket].split(',')
                        for vtl_layer in vtl_layer_list:

                            intermediate_layer = ImportFinrepVTL.find_intermediate_layer(
                                self, context, vtl_layer.strip())

                            if not (intermediate_layer is None):
                                output_layer.dependant_intermediate_layers.append(
                                    intermediate_layer)
                                combo = VTLForOutputLayerAndIntermediateLayerCombination()
                                combo.outputLayer = output_layer
                                combo.intermediateLayer = intermediate_layer
                                combo.name = output_layer.name + '_' + intermediate_layer.name
                                # get the special commands specific to the output layer and intermediate layer
                                for trans in scheme.expressions:
                                    index_of_colon = trans.expression.find(':')
                                    lhs = trans.expression[0:index_of_colon].strip(
                                    )
                                    if (lhs) == intermediate_layer.name:
                                        combo.transformations.append(trans)
                                output_layer.VTLForOutputLayerAndIntemedateLayerCombinations.append(
                                    combo)

    def find_entity(self, context, output_layer_name):
        '''
        Doc for findEntity
        '''
        for the_entity in context.input_layer_entities_package.eClassifiers:
            if isinstance(the_entity, ELClass):
                if the_entity.name == output_layer_name:
                    return the_entity

        for the_entity in context.output_layer_entities_package.eClassifiers:
            if isinstance(the_entity, ELClass):
                if the_entity.name == output_layer_name:
                    return the_entity

        return None

    def find_intermediate_layer(self, context, layer_name):
        '''
        Doc for findIntermediateLayer
        '''

        for layer in context.vtl_module.VTLGeneratedIntermediateLayers.vTLGeneratedIntermediateLayers:

            if layer.name == layer_name:
                return layer

        return None

    def build_output_layer_to_vtl_layer_map(self, context):
        '''
        Doc for buildOutputLayerToVTLLayerMap
        '''
        file_location = context.file_directory + os.sep + "transformations.csv"

        header_skipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    expression = row[2]
                    scheme = row[7]
                    if scheme.startswith("G_F") and scheme.endswith("_FINREP_1"):
                        if "union" in expression:

                            index_of_expression_open_bracket = expression.find(
                                '(')
                            index_of_expression_closed_bracket = expression.find(
                                ')')

                            vtl_layer_list = expression[index_of_expression_open_bracket:index_of_expression_closed_bracket].split(
                                ',')

                            index_of_scheme_start = expression.find('G_')
                            index_of_scheme_end = expression.find(
                                'UNFLDD_FINREP_1')
                            output_layer = scheme[index_of_scheme_start:index_of_scheme_end]
                            context.outputLayerToVTLLayerMap[output_layer,
                                                             vtl_layer_list]

    def build_list_of_enriched_layers(self, context):
        '''
        Doc for buildListOfEnrichedLayers
        '''
        for scheme in context.vtl_module.VTLSchemes.vTLSchemes:

            if (scheme.scheme_id.startswith("P_") and scheme.scheme_id.endswith("_E_1")):
                ImportFinrepVTL.build_list_of_enriched_layers_for_scheme(
                    self, scheme, context)

    def build_list_of_intermediate_finrep_layers(self, context):
        '''
        Doc for buildListOfIntermediateFinrepLayers
        '''
        for scheme in context.vtl_module.VTLSchemes.vTLSchemes:

            if not (scheme.scheme_id.startswith("G_F_")) and (not ((scheme.scheme_id.startswith("P_") and scheme.scheme_id.endswith("_E_1")))):
                ImportFinrepVTL.build_list_of_intermediate_finrep_layers_for_scheme(
                    self, scheme, context)

    def build_list_of_enriched_layers_for_scheme(self, scheme, context):
        '''
        Doc for buildListOfEnrichedLayersForScheme
        '''
        for transformation in scheme.expressions:

            the_intermediate_layer = None
            for enriched_layer in context.vtl_module.VTLEnrichedLayers.vTLGeneratedIntermediateLayers:
                intermediate_layer_name = enriched_layer.name
                if intermediate_layer_name == scheme.scheme_id:
                    the_intermediate_layer = enriched_layer

            if the_intermediate_layer is None:
                the_intermediate_layer = VTLGeneratedIntermediateLayer(
                    name=scheme.scheme_id)
                the_intermediate_layer.isEnrichment = True
                context.vtl_module.VTLEnrichedLayers.vTLGeneratedIntermediateLayers.append(
                    the_intermediate_layer)

            the_intermediate_layer.transformations.append(transformation)

    def build_list_of_intermediate_finrep_layers_for_scheme(self, scheme, context):
        '''
        Doc for buildListOfIntermediateFinrepLayersForScheme
        '''
        for transformation in scheme.expressions:
            transformation_text = transformation.expression
            index_of_lhs_end = transformation_text.find(' :=')
            expression_lhs = ImportFinrepVTL.strip_comment(
                self, transformation_text[0:index_of_lhs_end])
            the_intermediate_layer = None
            for intermediate_layer in context.vtl_module.VTLGeneratedIntermediateLayers.vTLGeneratedIntermediateLayers:
                intermediate_layer_name = intermediate_layer.name
                if intermediate_layer_name == expression_lhs:
                    the_intermediate_layer = intermediate_layer

            if the_intermediate_layer is None:
                the_intermediate_layer = VTLGeneratedIntermediateLayer(
                    name=expression_lhs)
                the_intermediate_layer.isEnrichment = False
                the_intermediate_layer.dependant_enriched_cubes = ImportFinrepVTL.find_enriched_cube_for(
                    self, context, expression_lhs)
                context.vtl_module.VTLGeneratedIntermediateLayers.vTLGeneratedIntermediateLayers.append(
                    the_intermediate_layer)

            the_intermediate_layer.transformations.append(transformation)

    def strip_comment(self, text):
        '''
        Doc for stripComment
        '''
        end_comment_index = text.find('*/')
        if end_comment_index > 0:
            return text[end_comment_index + 2:len(text)]
        else:
            return text

    def find_enriched_cube_for(self, context, scheme_id):
        '''
        Doc for findEnrichedCubeFor
        '''
        index_of_scheme_end = scheme_id.find('_FINREP')
        enriched_cube = "P_" + scheme_id[0:index_of_scheme_end] + "_E_1"

        return_layer = None
        for enriched_layer in context.vtl_module.VTLEnrichedLayers.vTLGeneratedIntermediateLayers:

            if enriched_layer.name == enriched_cube:
                return_layer = enriched_layer

        return return_layer

    def add_reports(self, context):
        '''
        Doc for addReports
        '''
        file_location = context.file_directory + os.sep + "in_scope_reports.csv"

        header_skipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not header_skipped):
                    header_skipped = True
                else:
                    report_template = row[0]
                    view = View(name="view_" + report_template)
                    context.view_module.views.append(view)

                    task = ScriptTask(name="task_" + report_template)
                    context.workflowModule.subProcess[0].flowElements.append(
                        task)
                    ImportFinrepVTL.add_layers(self, context, view, task)

    def add_layers(self, context, view, task):
        '''
        Doc for addLayers
        '''

        rol_vtl = ImportFinrepVTL.find_output_layer_vtl(
            self, context, view.name[5:len(view.name)] + "_REF_OutputItem")

        if not rol_vtl is None:
            view.outputLayer = rol_vtl.outputLayer
            view_vtl = VTLForView(name="vtl_" + view.name)
            view_vtl.view = view
            view_vtl.vtl = rol_vtl
            context.vtl_module.VTLForViews.vTLForViews.append(view_vtl)

            for intermediate_layer in rol_vtl.dependant_intermediate_layers:

                # vtlLayer = intermediateLayer.transformations
                link = ImportFinrepVTL.find_entity_intermediate_layer_link(
                    self, context, intermediate_layer)
                if not link is None:
                    input_layer = link.entity
                    selection_layer = SelectionLayer()
                    if input_layer is not None:
                        selection_layer.name = view.outputLayer.name + \
                            "_" + link.VTLIntermediateLayer.name
                    selection_layer.generatedEntity = rol_vtl.outputLayer
                    task.selectionLayers.extend([selection_layer])
                    layer_sql = LayerSQL()
                    layer_sql.selectionLayer = selection_layer
                    vtl_for_selection_layer = VTLForSelectionLayer()
                    vtl_for_selection_layer.selectionLayer = layer_sql
                    vtl_for_selection_layer.outputLayer = rol_vtl
                    vtl_for_selection_layer.intermediateLayer = intermediate_layer
                    context.vtl_module.VTLForSelectionLayers.vTLForSelectionLayers.append(
                        vtl_for_selection_layer)

                    view.selectionLayerSQL.extend([layer_sql])
                    ImportFinrepVTL.add_columns_to_layer(self, layer_sql)

    def find_output_layer_vtl(self, context, output_layer_name):
        '''
        Doc for findOutputLayerVTL
        '''

        for rol in context.vtl_module.VTLGeneratedOutputLayers.vTLGeneratedOutputlayerModules:
            if not (rol.outputLayer is None) and (rol.outputLayer.name == output_layer_name):
                return rol

    def find_entity_intermediate_layer_link(self, context, intermediate_layer):
        '''
        Doc for findEntityIntermediateLayerLink
        '''

        for link in context.vtl_module.entityToVTLIntermediateLayerLinks.entityToVTLIntermediateLayerLinks:
            if link.VTLIntermediateLayer == intermediate_layer:
                return link

    def add_columns_to_layer(self, layer_sql):
        '''
        Doc for addColumnsToLayer
        '''

        output_layer = layer_sql.selectionLayer.generatedEntity
        if not output_layer is None:
            for member in output_layer.eOperations:
                if isinstance(member, ELOperation):
                    select_column = SelectColumnAttributeAs()
                    select_column.asAttribute = member
                    layer_sql.columns.extend([select_column])

    def build_intermediate_layer_to_input_layer(self, context):
        '''
        Doc for buildIntermediateLayerToInputLayer
        '''
        file_location = context.file_directory + os.sep + "VTL_layer_to_IL.csv"
        # il_name = layerSQL.name
        # amended_il_name = "FINREP_REF_" + il_name + "_REF_FINREP 3.0"

        header_skipped = False
        # Load all the entities from the csv file, make an ELClass per entity,
        # and add the ELClass to the package
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if not header_skipped:
                    header_skipped = True
                else:
                    vtl_layer = row[0]
                    input_layer = row[2]
                    filter1 = row[3]
                    filter_name = row[5]
                    link = EntityToVTLIntermediateLayerLink()
                    link.VTLIntermediateLayer = ImportFinrepVTL.find_intermediate_layer(
                        self, context, vtl_layer.strip() + "_FINREP")
                    link.entity = ImportFinrepVTL.find_entity(
                        self, context, input_layer)
                    link.filter = filter1
                    link.filterName = filter_name
                    context.vtl_module.entityToVTLIntermediateLayerLinks.entityToVTLIntermediateLayerLinks.append(
                        link)
