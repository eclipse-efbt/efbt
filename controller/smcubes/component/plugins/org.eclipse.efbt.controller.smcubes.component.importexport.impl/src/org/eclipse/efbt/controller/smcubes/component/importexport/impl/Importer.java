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
 *    Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.controller.smcubes.component.importexport.impl;


import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.eclipse.efbt.openregspecs.model.open_reg_specs.ModuleDependencies;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ModuleDependency;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsFactory;

import org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsFactory;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.DomainModule;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.MemberModule;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.SMCubesCoreModel;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.VariableModule;

import org.eclipse.efbt.openregspecs.model.open_reg_specs.SubDomainModule;
import org.eclipse.efbt.controller.smcubes.component.importexport.api.BirdImporter;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;





/**
 * Import SMCubes Artefacts into EMF Model instances
 * 
 * @author Neil Mackenzie
 */
public abstract class Importer implements BirdImporter {


	
	/**
	 * The file location to output the SMCubes artefacts
	 */
	protected String outputFilepath;
	

	/**
	 * The domains
	 */
	protected DomainModule domains;

	/**
	 * The variables
	 */
	protected VariableModule variables;
	/**
	 * The members
	 */
	protected MemberModule members;
	/**
	 * The subdomains
	 */
	protected SubDomainModule subdomains;
	/**
	 * The subdomain enumerations
	 */
	//protected List<CombinationModule> combinationsModules;
	/**
	 * The cubesModule
	 */
	//protected CubeModule cubesModule;
	/**
	 * The cubeStructuresModule
	 */
	//protected CubeModule cubeStructuresModule;
	/**
	 * The cubeStructureItemsModule
	 */
	//protected CubeModule cubeStructureItemsModule;
	/**
	 * The cubeMappingModule
	 */
	//protected CubeMappingModule cubeMappingModule;
	/**
	 * The mappingDefinitionModule
	 */
	//protected MappingDefinitionModule mappingDefinitionModule;
	/**
	 * The memberMappingModule
	 */
	//protected MemberMappingModule memberMappingModule;
	/**
	 * The variableMappingModule
	 */
	//protected VariableMappingModule variableMappingModule;
	
	public String logMessage = "";
	
	

	/**
	 * The birdModel
	 */
	protected SMCubesCoreModel birdModel;

	public SMCubesCoreModel getBirdModel() {
		return birdModel;
	}




	/**
	 * The file path of the access database
	 */
	public  String filepath;
	

	/**
	 * Constructor
	 */
	public Importer() {
		domains = Open_reg_specsFactory.eINSTANCE.createDomainModule();
		domains.setName("domainsModule");
		variables = Open_reg_specsFactory.eINSTANCE.createVariableModule();
		variables.setName("variablesModule");
		members = Open_reg_specsFactory.eINSTANCE.createMemberModule();
		members.setName("membersModule");
		subdomains = Open_reg_specsFactory.eINSTANCE.createSubDomainModule();
		subdomains.setName("subdomainsModule");

		//cubesModule = openregspecs_smcubes_extra_extensionFactory.eINSTANCE.createCubeModule();
		//cubesModule.setName("cubesModule");
		//cubeStructuresModule = openregspecs_smcubes_extra_extensionFactory.eINSTANCE.createCubeModule();
		//cubeStructuresModule.setName("cubeStructuresModule");
		////cubeStructureItemsModule = openregspecs_smcubes_extra_extensionFactory.eINSTANCE.createCubeModule();
		//cubeStructureItemsModule.setName("cubeStructureItemsModule");
		//combinationsModules = new BasicEList<CombinationModule>();
		//cubeMappingModule = openregspecs_smcubes_extra_extensionFactory.eINSTANCE.createCubeMappingModule();
		//mappingDefinitionModule = openregspecs_smcubes_extra_extensionFactory.eINSTANCE.createMappingDefinitionModule();
		//memberMappingModule = openregspecs_smcubes_extra_extensionFactory.eINSTANCE.createMemberMappingModule();
		//variableMappingModule = openregspecs_smcubes_extra_extensionFactory.eINSTANCE.createVariableMappingModule();

		birdModel = Open_reg_specsFactory.eINSTANCE.createSMCubesCoreModel();
		// birdModel.setCombinations(combinationModule);
		//birdModel.getCubes().add(cubesModule);
		//birdModel.getCubes().add(cubeStructuresModule);
		//birdModel.getCubes().add(cubeStructureItemsModule);
		
		//birdModel.getDomains().add(domains);
		//birdModel.getMappings().add(mappingDefinitionModule);
		//birdModel.getMembers().add(members);
		//birdModel.getVariables().add(variables);
		
		ModuleDependencies dependencies = Open_reg_specsFactory.eINSTANCE.createModuleDependencies();
		
		domains.setDependencies(dependencies);
		
		ModuleDependency dependency = Open_reg_specsFactory.eINSTANCE.createModuleDependency();
		
		dependencies.getTheModules().add(dependency);
		dependency.setTheModule(members);
		

	}

	/**
	 * Create all the Combination EObjects
	 */
	//public abstract void createAllCombinations();

	/**
	 * Create all the Cube EObjects
	 */
	//public abstract void createAllCubes();

	/**
	 * Create all the Domain EObjects
	 */
	public abstract void createAllDomains();

	/**
	 * Create all the Mapping EObjects
	 */
	//public abstract void createAllMappings();

	/**
	 * Create all the Members EObjects
	 */
	public abstract void createAllMembers();
	
	/**
	 * Create all the Subdomain EObjects
	 */
	public abstract void createAllSubdomains();
	
	/**
	 * Create all the Subdomain Enumeration EObjects
	 */
	public abstract void createAllSubdomainEnumerarions();


	/**
	 * Create all the Variable EObjects
	 */
	public abstract void createAllVariables();

	/**
	 * Import all the SMCubes information and store is as EObjects
	 * @param outputFilepath2 
	 * @param filepath 
	 */
	
	public void doImport(String theFilepath, String theOutputFilepath)
	{
		
		filepath = theFilepath;
		outputFilepath = theOutputFilepath;
		
		createAllDomains();  
		createAllMembers();
		createAllVariables();
		createAllSubdomains();
		createAllSubdomainEnumerarions();
		//createAllCubes();	
		//createAllCombinations();		
	
		
		
	}
	
	
	/**
	 * Serialize all the EObjects to files.
	 */
	public void saveArtifactsAsJSON() {
		//JsonResourceFactory factory = new JsonResourceFactory();
		//URI domainsURI = URI.createFileURI(outputFilepath + "domains.openregspecs_smcubes_extension");
		//URI membersURI = URI.createFileURI(outputFilepath + "members.openregspecs_smcubes_extension");
		//URI variablesURI = URI.createFileURI(outputFilepath + "variables.openregspecs_smcubes_extension");
		//URI subdomainsURI = URI.createFileURI(outputFilepath + "subdomains.openregspecs_smcubes_extension");
		
		//URI cubesURI = URI.createFileURI(outputFilepath + "cubes.openregspecs_smcubes_extension");
		//URI cubestructuresURI = URI.createFileURI(outputFilepath + "cube_structures.openregspecs_smcubes_extension");
		//URI cubestructureitemsURI = URI.createFileURI(outputFilepath + "cube_structure_items.openregspecs_smcubes_extension");
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

		//URI birdURI = URI.createFileURI(outputFilepath + "bird.bird_model");

		//Resource domainsResource = factory.createResource(domainsURI);
		//Resource membersResource = factory.createResource(membersURI);
		//Resource variablesResource = factory.createResource(variablesURI);
		//Resource subdomainsResource = factory.createResource(subdomainsURI);
		//Resource cubesResource = factory.createResource(cubesURI);
		//Resource cubestructuresResource = factory.createResource(cubestructuresURI);
		//Resource cubestructureitemsResource = factory.createResource(cubestructureitemsURI);
		// Resource transformationsResource =
		// factory.createResource(transformationsURI);

		// Resource cubeMappingResource = factory.createResource(cubeMappingURI);
		// Resource mappingDefinitionResource =
		// factory.createResource(mappingDefinitionURI);
		// Resource memberMappingResource = factory.createResource(memberMappingURI);
		// Resource variableMappingResource =
		// factory.createResource(variableMappingURI);

		// Resource birdResource = factory.createResource(birdURI);

		//domainsResource.getContents().add(domains);
		//membersResource.getContents().add(members);
		//variablesResource.getContents().add(variables);
		//subdomainsResource.getContents().add(subdomains);
		
		//cubesResource.getContents().add(cubesModule);
		//cubestructuresResource.getContents().add(cubeStructuresModule);
		//cubestructureitemsResource.getContents().add(cubeStructureItemsModule);
		// cubeMappingResource.getContents().add(cubeMappingModule);
		// mappingDefinitionResource.getContents().add(mappingDefinitionModule);
		// memberMappingResource.getContents().add(memberMappingModule);
		// variableMappingResource.getContents().add(variableMappingModule);

		// birdResource.getContents().add(birdModel);

		try {
			//domainsResource.save(Collections.EMPTY_MAP);
			//membersResource.save(Collections.EMPTY_MAP);
			//variablesResource.save(Collections.EMPTY_MAP);
			//subdomainsResource.save(Collections.EMPTY_MAP);
			//cubesResource.save(Collections.EMPTY_MAP);
			//cubestructuresResource.save(Collections.EMPTY_MAP);
			//cubestructureitemsResource.save(Collections.EMPTY_MAP);			
			// cubeMappingResource.save(Collections.EMPTY_MAP);
			// mappingDefinitionResource.save(Collections.EMPTY_MAP);
			// memberMappingResource.save(Collections.EMPTY_MAP);
			// variableMappingResource.save(Collections.EMPTY_MAP);
			// birdResource.save(Collections.EMPTY_MAP);
			
			FileWriter fw = new FileWriter(outputFilepath + "logfile");
			fw.write(logMessage);
			fw.flush();

		} catch (IOException e) {

			e.printStackTrace();
		}
		int counter = 0;

		/**for (Iterator iterator = combinationsModules.iterator(); iterator.hasNext();) {
			CombinationModule combinationModule = (CombinationModule) iterator.next();
			URI combinationsURI = URI
					.createFileURI(outputFilepath + "\\combinations\\combinations" + counter + ".openregspecs_smcubes_extension");
			counter++;
			Resource combinationsResource = factory.createResource(combinationsURI);
			combinationsResource.getContents().add(combinationModule);
			try {
				combinationsResource.save(Collections.EMPTY_MAP);
			} catch (IOException e) {

				e.printStackTrace();
			}

		}*/

	}
	
}
