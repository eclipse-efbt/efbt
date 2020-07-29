/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *  Neil Mackenzie - initial API and implementation
 *  */
package org.eclipse.efbt.component.birdartefactscreator.importer;

import org.eclipse.efbt.bird.efbt_data_definition.CombinationModule;
import org.eclipse.efbt.bird.efbt_data_definition.CubeModule;
import org.eclipse.efbt.bird.efbt_data_definition.DomainModule;
import org.eclipse.efbt.bird.efbt_data_definition.Efbt_data_definitionFactory;
import org.eclipse.efbt.bird.efbt_data_definition.MemberModule;
import org.eclipse.efbt.bird.efbt_data_definition.VariableModule;
import org.eclipse.efbt.bird.efbt_mapping.CubeMappingModule;
import org.eclipse.efbt.bird.efbt_mapping.Efbt_mappingFactory;
import org.eclipse.efbt.bird.efbt_mapping.MappingDefinitionModule;
import org.eclipse.efbt.bird.efbt_mapping.MemberMappingModule;
import org.eclipse.efbt.bird.efbt_mapping.VariableMappingModule;
import org.eclipse.efbt.bird.efbt_vtl_transformation.Efbt_vtl_transformationFactory;
import org.eclipse.efbt.bird.efbt_vtl_transformation.TransformationSchemeModule;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.eclipse.efbt.component.birdartefactscreator.importer.FreeBirdToolsResourceFactory;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.efbt.bird.bird_model.BIRDModel;
import org.eclipse.efbt.bird.bird_model.Bird_modelFactory;

/**
 * Import SMCubes Artefacts into EMF Model instances
 * 
 * @author Neil Mackenzie
 */
public abstract class Importer {

	/**
	 * The file location to output the SMCubes artefacts
	 */
	public static String outputFilepath;

	/**
	 * The transformationSchemes
	 */
	public static TransformationSchemeModule transformationSchemes;

	/**
	 * The domains
	 */
	public static DomainModule domains;

	/**
	 * The variables
	 */
	public static VariableModule variables;
	/**
	 * The members
	 */
	public static MemberModule members;
	/**
	 * The combinationsModules
	 */
	public static List<CombinationModule> combinationsModules;
	/**
	 * The cubesModule
	 */
	public static CubeModule cubesModule;
	/**
	 * The cubeStructuresModule
	 */
	public static CubeModule cubeStructuresModule;
	/**
	 * The cubeStructureItemsModule
	 */
	public static CubeModule cubeStructureItemsModule;
	/**
	 * The cubeMappingModule
	 */
	public static CubeMappingModule cubeMappingModule;
	/**
	 * The mappingDefinitionModule
	 */
	public static MappingDefinitionModule mappingDefinitionModule;
	/**
	 * The memberMappingModule
	 */
	public static MemberMappingModule memberMappingModule;
	/**
	 * The variableMappingModule
	 */
	public static VariableMappingModule variableMappingModule;

	/**
	 * The birdModel
	 */
	public static BIRDModel birdModel;

	/**
	 * Constructor
	 */
	public Importer() {
		transformationSchemes = Efbt_vtl_transformationFactory.eINSTANCE.createTransformationSchemeModule();
		domains = Efbt_data_definitionFactory.eINSTANCE.createDomainModule();
		variables = Efbt_data_definitionFactory.eINSTANCE.createVariableModule();
		members = Efbt_data_definitionFactory.eINSTANCE.createMemberModule();

		cubesModule = Efbt_data_definitionFactory.eINSTANCE.createCubeModule();
		cubeStructuresModule = Efbt_data_definitionFactory.eINSTANCE.createCubeModule();
		cubeStructureItemsModule = Efbt_data_definitionFactory.eINSTANCE.createCubeModule();
		combinationsModules = new BasicEList<CombinationModule>();
		cubeMappingModule = Efbt_mappingFactory.eINSTANCE.createCubeMappingModule();
		mappingDefinitionModule = Efbt_mappingFactory.eINSTANCE.createMappingDefinitionModule();
		memberMappingModule = Efbt_mappingFactory.eINSTANCE.createMemberMappingModule();
		variableMappingModule = Efbt_mappingFactory.eINSTANCE.createVariableMappingModule();

		birdModel = Bird_modelFactory.eINSTANCE.createBIRDModel();
		// birdModel.setCombinations(combinationModule);
		birdModel.getCubes().add(cubesModule);
		birdModel.getDomains().add(domains);
		birdModel.getMappings().add(mappingDefinitionModule);
		birdModel.getMembers().add(members);
		// must rename this to getTranformationSchemes
		birdModel.getFunctionalModules().add(transformationSchemes);
		birdModel.getVariables().add(variables);

	}

	/**
	 * Create all the Combination EObjects
	 */
	public abstract void createAllCombinations();

	/**
	 * Create all the Cube EObjects
	 */
	public abstract void createAllCubes();

	/**
	 * Create all the Domain EObjects
	 */
	public abstract void createAllDomains();

	/**
	 * Create all the Mapping EObjects
	 */
	public abstract void createAllMappings();

	/**
	 * Create all the Members EObjects
	 */
	public abstract void createAllMembers();

	/**
	 * Create all the Transformation Scheme EObjects
	 */
	public abstract void createAllTransformationSchemes();

	/**
	 * Create all the Variable EObjects
	 */
	public abstract void createAllVariables();

	/**
	 * Import all the SMCubes information and store is as EObjects
	 */
	public void doImport() {
		createAllDomains();
		createAllMembers();
		createAllVariables();
		createAllCubes();
		createAllCombinations();
		createAllTransformationSchemes();
		// createAllMappings();
		saveArtifacts();
	}

	/**
	 * Serialize all the EObjects to files.
	 */
	public void saveArtifacts() {
		FreeBirdToolsResourceFactory factory = new FreeBirdToolsResourceFactory();
		URI domainsURI = URI.createFileURI(outputFilepath + "domains.efbt_data_definition");
		URI membersURI = URI.createFileURI(outputFilepath + "members.efbt_data_definition");
		URI variablesURI = URI.createFileURI(outputFilepath + "variables.efbt_data_definition");
		URI cubesURI = URI.createFileURI(outputFilepath + "cubes.efbt_data_definition");
		URI cubestructuresURI = URI.createFileURI(outputFilepath + "cube_structures.efbt_data_definition");
		URI cubestructureitemsURI = URI.createFileURI(outputFilepath + "cube_structure_items.efbt_data_definition");
		// URI transformationsURI = URI.createFileURI(outputFilepath +
		// "transformations.efbt_vtl_transformation");

		// URI cubeMappingURI = URI.createFileURI(outputFilepath +
		// "cube_mapping.efbt_mapping");
		// URI mappingDefinitionURI = URI.createFileURI(outputFilepath +
		// "mapping_definition.efbt_mapping");
		// URI memberMappingURI = URI.createFileURI(outputFilepath +
		// "member_mapping.efbt_mapping");
		// URI variableMappingURI = URI.createFileURI(outputFilepath +
		// "variable_mapping.efbt_mapping");

		URI birdURI = URI.createFileURI(outputFilepath + "bird.bird_model");

		Resource domainsResource = factory.createResource(domainsURI);
		Resource membersResource = factory.createResource(membersURI);
		Resource variablesResource = factory.createResource(variablesURI);
		Resource cubesResource = factory.createResource(cubesURI);
		Resource cubestructuresResource = factory.createResource(cubestructuresURI);
		Resource cubestructureitemsResource = factory.createResource(cubestructureitemsURI);
		// Resource transformationsResource =
		// factory.createResource(transformationsURI);

		// Resource cubeMappingResource = factory.createResource(cubeMappingURI);
		// Resource mappingDefinitionResource =
		// factory.createResource(mappingDefinitionURI);
		// Resource memberMappingResource = factory.createResource(memberMappingURI);
		// Resource variableMappingResource =
		// factory.createResource(variableMappingURI);

		// Resource birdResource = factory.createResource(birdURI);

		domainsResource.getContents().add(domains);
		membersResource.getContents().add(members);
		variablesResource.getContents().add(variables);
		cubesResource.getContents().add(cubesModule);
		cubestructuresResource.getContents().add(cubeStructuresModule);
		cubestructureitemsResource.getContents().add(cubeStructureItemsModule);
		// transformationsResource.getContents().add(transformationSchemes);

		// cubeMappingResource.getContents().add(cubeMappingModule);
		// mappingDefinitionResource.getContents().add(mappingDefinitionModule);
		// memberMappingResource.getContents().add(memberMappingModule);
		// variableMappingResource.getContents().add(variableMappingModule);

		// birdResource.getContents().add(birdModel);

		try {
			domainsResource.save(Collections.EMPTY_MAP);
			membersResource.save(Collections.EMPTY_MAP);
			variablesResource.save(Collections.EMPTY_MAP);
			cubesResource.save(Collections.EMPTY_MAP);
			cubestructuresResource.save(Collections.EMPTY_MAP);
			cubestructureitemsResource.save(Collections.EMPTY_MAP);
			// transformationsResource.save(Collections.EMPTY_MAP);
			// cubeMappingResource.save(Collections.EMPTY_MAP);
			// mappingDefinitionResource.save(Collections.EMPTY_MAP);
			// memberMappingResource.save(Collections.EMPTY_MAP);
			// variableMappingResource.save(Collections.EMPTY_MAP);
			// birdResource.save(Collections.EMPTY_MAP);

		} catch (IOException e) {

			e.printStackTrace();
		}
		int counter = 0;

		for (Iterator iterator = combinationsModules.iterator(); iterator.hasNext();) {
			CombinationModule combinationModule = (CombinationModule) iterator.next();
			URI combinationsURI = URI
					.createFileURI(outputFilepath + "\\combinations\\combinations" + counter + ".efbt_data_definition");
			counter++;
			Resource combinationsResource = factory.createResource(combinationsURI);
			combinationsResource.getContents().add(combinationModule);
			try {
				combinationsResource.save(Collections.EMPTY_MAP);
			} catch (IOException e) {

				e.printStackTrace();
			}

		}

	}

}
