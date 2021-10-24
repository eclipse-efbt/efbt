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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.eclipse.efbt.cocalimo.core.model.task.Task;
import org.eclipse.efbt.cocalimo.core.model.task.TaskModule;
import org.eclipse.efbt.cocalimo.core.model.task.TaskFactory;
import org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependencies;
import org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency;
import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementFactory;
import org.eclipse.efbt.controller.smcubes.component.importexport.api.BirdImporter;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinitionModule;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CombinationModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.DomainModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.Efbt_data_definitionFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.VariableModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.CubeMappingModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.Efbt_mappingFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.MappingDefinitionModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.MemberMappingModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.VariableMappingModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.Efbt_vtl_transformationFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.TransformationSchemeModule;
import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel;
import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelFactory;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;
import org.emfjson.jackson.resource.JsonResourceFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesFactory;




/**
 * Import SMCubes Artefacts into EMF Model instances
 * 
 * @author Neil Mackenzie
 */
public abstract class Importer implements BirdImporter {


	
//	@Inject
//	private ISerializer serializer;


	//@Inject public EFactoryServiceProvider serviceProvider;
	
	/**
	 * The file location to output the SMCubes artefacts
	 */
	protected String outputFilepath;
	
	
	/**
	 * A SMCubesStaticModel instance which contains just TestTemplates,
	 * note that the testTemplates can refer to TestDefintions 
	 * in testDefinitionProgram
	 */
	public static AortaSMCubesModel testTemplateProgram;
	/**
	 * A SMCubesStaticModel instance which contains just TestConstriants
	 */
	public static AortaSMCubesModel testConstraintsProgram;
	
	
	/**
	 * A SMCubesStaticModel instance which contains just functionality Modules
	 */
	public static AortaSMCubesModel functionalityModulesProgram;
	

	public static TestModule testModule;
	
	public static TestDefinitionModule testDefinitionModule;
	/**
	 * The transformationSchemes
	 */
	protected TransformationSchemeModule transformationSchemes;

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
	 * The combinationsModules
	 */
	protected List<CombinationModule> combinationsModules;
	/**
	 * The cubesModule
	 */
	protected CubeModule cubesModule;
	/**
	 * The cubeStructuresModule
	 */
	protected CubeModule cubeStructuresModule;
	/**
	 * The cubeStructureItemsModule
	 */
	protected CubeModule cubeStructureItemsModule;
	/**
	 * The cubeMappingModule
	 */
	protected CubeMappingModule cubeMappingModule;
	/**
	 * The mappingDefinitionModule
	 */
	protected MappingDefinitionModule mappingDefinitionModule;
	/**
	 * The memberMappingModule
	 */
	protected MemberMappingModule memberMappingModule;
	/**
	 * The variableMappingModule
	 */
	protected VariableMappingModule variableMappingModule;
	
	public String logMessage = "";
	
	

	/**
	 * The birdModel
	 */
	protected SmcubesModel birdModel;

	public SmcubesModel getBirdModel() {
		return birdModel;
	}




	/**
	 * The file path of the access database
	 */
	public  String filepath;
	/**
	 * The file path of the input test data
	 */
	public  String testdatafilepath;

	/**
	 * Constructor
	 */
	public Importer() {
		transformationSchemes = Efbt_vtl_transformationFactory.eINSTANCE.createTransformationSchemeModule();
		transformationSchemes.setName("TransformationSchemesModule");
		domains = Efbt_data_definitionFactory.eINSTANCE.createDomainModule();
		domains.setName("domainsModule");
		variables = Efbt_data_definitionFactory.eINSTANCE.createVariableModule();
		variables.setName("variablesModule");
		members = Efbt_data_definitionFactory.eINSTANCE.createMemberModule();
		members.setName("membersModule");

		cubesModule = Efbt_data_definitionFactory.eINSTANCE.createCubeModule();
		cubesModule.setName("cubesModule");
		cubeStructuresModule = Efbt_data_definitionFactory.eINSTANCE.createCubeModule();
		cubeStructuresModule.setName("cubeStructuresModule");
		cubeStructureItemsModule = Efbt_data_definitionFactory.eINSTANCE.createCubeModule();
		cubeStructureItemsModule.setName("cubeStructureItemsModule");
		combinationsModules = new BasicEList<CombinationModule>();
		cubeMappingModule = Efbt_mappingFactory.eINSTANCE.createCubeMappingModule();
		mappingDefinitionModule = Efbt_mappingFactory.eINSTANCE.createMappingDefinitionModule();
		memberMappingModule = Efbt_mappingFactory.eINSTANCE.createMemberMappingModule();
		variableMappingModule = Efbt_mappingFactory.eINSTANCE.createVariableMappingModule();

		birdModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		// birdModel.setCombinations(combinationModule);
		birdModel.getCubes().add(cubesModule);
		birdModel.getCubes().add(cubeStructuresModule);
		birdModel.getCubes().add(cubeStructureItemsModule);
		
		birdModel.getDomains().add(domains);
		birdModel.getMappings().add(mappingDefinitionModule);
		birdModel.getMembers().add(members);
		// must rename this to getTranformationSchemes
		birdModel.getFunctionalModules().add(transformationSchemes);
		birdModel.getVariables().add(variables);
		
		ModuleDependencies dependencies = Module_managementFactory.eINSTANCE.createModuleDependencies();
		
		domains.setDependencies(dependencies);
		
		ModuleDependency dependency = Module_managementFactory.eINSTANCE.createModuleDependency();
		
		dependencies.getTheModules().add(dependency);
		dependency.setTheModule(members);
		
		functionalityModulesProgram = Aorta_smcubesFactory.eINSTANCE.createAortaSMCubesModel();
		TaskModule fmm = TaskFactory.eINSTANCE.createTaskModule();
		fmm.setName("functionalityModuleModule");
		Task fm = TaskFactory.eINSTANCE.createDataProcessingTask();
		fm.setName("functionalityModule");
		fmm.getTasks().add(fm);
		functionalityModulesProgram.setTaskModules(fmm);
		
		
		
		
	
		 

		
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
	 * @param testdatafilepath 
	 * @param outputFilepath2 
	 * @param filepath 
	 */
	
	public void doImport(String theFilepath, String theOutputFilepath, String theTestdatafilepath)
	{
		
		filepath = theFilepath;
		outputFilepath = theOutputFilepath;
		testdatafilepath = theTestdatafilepath;
		
		createAllDomains();  
		createAllMembers();
		createAllVariables();
		createAllCubes();	
		createAllCombinations();		
		createAllTransformationSchemes();
	
		
		
	}
	
	
	/**
	 * Serialize all the EObjects to files.
	 */
	public void saveArtifactsAsJSON() {
		JsonResourceFactory factory = new JsonResourceFactory();
		URI domainsURI = URI.createFileURI(outputFilepath + "domains.efbt_data_definition");
		URI membersURI = URI.createFileURI(outputFilepath + "members.efbt_data_definition");
		URI variablesURI = URI.createFileURI(outputFilepath + "variables.efbt_data_definition");
		URI cubesURI = URI.createFileURI(outputFilepath + "cubes.efbt_data_definition");
		URI cubestructuresURI = URI.createFileURI(outputFilepath + "cube_structures.efbt_data_definition");
		URI cubestructureitemsURI = URI.createFileURI(outputFilepath + "cube_structure_items.efbt_data_definition");
		URI testURI = URI.createFileURI(outputFilepath + "test.program");
		URI testDefinitionURI = URI.createFileURI(outputFilepath + "test_def.test_definition");
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

		Resource domainsResource = factory.createResource(domainsURI);
		Resource membersResource = factory.createResource(membersURI);
		Resource variablesResource = factory.createResource(variablesURI);
		Resource cubesResource = factory.createResource(cubesURI);
		Resource cubestructuresResource = factory.createResource(cubestructuresURI);
		Resource cubestructureitemsResource = factory.createResource(cubestructureitemsURI);
		Resource testResource = factory.createResource(testURI);
		Resource testDefinitionResource = factory.createResource(testDefinitionURI);
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
		testResource.getContents().add(testModule);
		testDefinitionResource.getContents().add(testDefinitionModule);
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
			testDefinitionResource.save(Collections.EMPTY_MAP);
			testResource.save(Collections.EMPTY_MAP);
			// transformationsResource.save(Collections.EMPTY_MAP);
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
	

	/**
	 * from Test data sored in CSV format , create all the model instances of
	 * Test and Store them in the list of  testPrograms. 
	 */
	public void importTestDataWithOldTestFormat(String fileLocation) {
		// TODO Auto-generated method stub
		
	}

}
