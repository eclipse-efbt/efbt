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
package org.eclipse.efbt.cocamo.smcubes.component.importexport.impl;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import org.eclipse.efbt.cocamo.model.cocamo.cocamoPackage;
import org.eclipse.efbt.cocamo.smcubes.component.importexport.api.BirdImporter;
import org.eclipse.efbt.cocamo.core.model.module_management.Module_managementPackage;
import org.eclipse.efbt.cocamo.core.model.requirements_text.Requirements_textPackage;
import org.eclipse.efbt.cocamo.smcubes.model.core.CorePackage;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.Data_definitionPackage;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.CombinationModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.CubeModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.DomainModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.Efbt_data_definitionFactory;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.MemberModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.VariableModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.CubeMappingModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.Efbt_mappingFactory;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.Efbt_mappingPackage;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.MappingDefinitionModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.MemberMappingModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.VariableMappingModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_vtl_transformation.Efbt_vtl_transformationFactory;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_vtl_transformation.Efbt_vtl_transformationPackage;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_vtl_transformation.TransformationSchemeModule;
import org.eclipse.efbt.cocamo.smcubes.model.mapping.MappingPackage;
import org.eclipse.efbt.cocamo.smcubes.model.rendering.RenderingPackage;
import org.eclipse.efbt.cocamo.smcubes.model.reports.ReportsPackage;
import org.eclipse.efbt.cocamo.smcubes.model.smcubes_model.SmcubesModel;
import org.eclipse.efbt.cocamo.smcubes.model.smcubes_model.Smcubes_modelFactory;
import org.eclipse.efbt.cocamo.smcubes.model.smcubes_model.Smcubes_modelPackage;
import org.eclipse.efbt.cocamo.smcubes.model.vtl_transformation.Vtl_transformationPackage;
import org.eclipse.efbt.cocamo.core.model.functionality_module.Functionality_modulePackage;
import org.eclipse.efbt.functional_design.core.model.scenarios.ScenariosPackage;
import org.eclipse.efbt.input_data.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;
import org.eclipse.efbt.testing.common.model.test.TestPackage;
import org.eclipse.efbt.testing.common.model.test_definition.Test_definitionPackage;
import org.eclipse.efbt.testing.common.model.test_input_data.Test_input_dataPackage;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.eson.EFactoryServiceProvider;
import org.eclipse.emf.eson.eFactory.EFactoryFactory;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.PackageImport;
import org.eclipse.emf.eson.serialization.FactoryBuilder;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.resource.IResourceDescription;
import org.eclipse.xtext.resource.XtextResourceSet;

import com.google.inject.Injector;



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
	public String outputFilepath;

	/**
	 * The transformationSchemes
	 */
	public TransformationSchemeModule transformationSchemes;

	/**
	 * The domains
	 */
	public DomainModule domains;

	/**
	 * The variables
	 */
	public VariableModule variables;
	/**
	 * The members
	 */
	public MemberModule members;
	/**
	 * The combinationsModules
	 */
	public List<CombinationModule> combinationsModules;
	/**
	 * The cubesModule
	 */
	public CubeModule cubesModule;
	/**
	 * The cubeStructuresModule
	 */
	public CubeModule cubeStructuresModule;
	/**
	 * The cubeStructureItemsModule
	 */
	public CubeModule cubeStructureItemsModule;
	/**
	 * The cubeMappingModule
	 */
	public CubeMappingModule cubeMappingModule;
	/**
	 * The mappingDefinitionModule
	 */
	public MappingDefinitionModule mappingDefinitionModule;
	/**
	 * The memberMappingModule
	 */
	public MemberMappingModule memberMappingModule;
	/**
	 * The variableMappingModule
	 */
	public VariableMappingModule variableMappingModule;

	/**
	 * The birdModel
	 */
	public SmcubesModel birdModel;

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
	
	public void doImport(String filepath6, String outputFilepath26, String testdatafilepath6)
	{
		
		filepath = filepath6;
		outputFilepath = outputFilepath26;
		testdatafilepath = testdatafilepath6;
		
		createAllDomains();  
		createAllMembers();
		createAllVariables();
		createAllCubes();
	
		createAllCombinations();
		
		createAllTransformationSchemes();
	
		// createAllMappings();
		//saveArtifactsAsXML();
		saveArtifactsAsEson(birdModel);
		
	}
	
	public void saveArtifactsAsEson(EObject testModel) {
		//get the birdModel , walk through it and add the features to a text file.
		
		//Factory expected = ((EFactoryResource) testModel.eResource()).getEFactoryFactory();
		
		
		
		//cocamoStandaloneSetup.doSetup();  
		//EFactoryStandaloneSetup.doSetup();
		ESONWithcocamomodelInjectorProvider eip = new ESONWithcocamomodelInjectorProvider();
		Injector injector = eip.getInjector();
		
		EFactoryServiceProvider serviceProvider = injector.getInstance(EFactoryServiceProvider.class);
		
	
		FactoryBuilder builder = new FactoryBuilder(serviceProvider);
	
		Factory actual = builder.buildFactory(testModel);
		// it looks like we only add one package in the builder, the package of the 
		// rootbuilder, we are going to add all teh packages here to work around this.
		PackageImport cocamo_programPackagePI = EFactoryFactory.eINSTANCE.createPackageImport();
		cocamo_programPackagePI.setEPackage(cocamoPackage.eINSTANCE);		
		actual.getEPackages().add(cocamo_programPackagePI);
		
		PackageImport corePI = EFactoryFactory.eINSTANCE.createPackageImport();
		corePI.setEPackage(CorePackage.eINSTANCE);		
		actual.getEPackages().add(corePI);
		
		PackageImport Efbt_vtl_transformationPackagePI = EFactoryFactory.eINSTANCE.createPackageImport();
		Efbt_vtl_transformationPackagePI.setEPackage(Efbt_vtl_transformationPackage.eINSTANCE);		
		actual.getEPackages().add(Efbt_vtl_transformationPackagePI);
		
		PackageImport Efbt_mappingPackagePI = EFactoryFactory.eINSTANCE.createPackageImport();
		Efbt_mappingPackagePI.setEPackage(Efbt_mappingPackage.eINSTANCE);		
		actual.getEPackages().add(Efbt_mappingPackagePI);
		
		PackageImport Data_definitionPackagePI = EFactoryFactory.eINSTANCE.createPackageImport();
		Data_definitionPackagePI.setEPackage(Data_definitionPackage.eINSTANCE);		
		actual.getEPackages().add(Data_definitionPackagePI);
		
		PackageImport Module_managementPackagePI = EFactoryFactory.eINSTANCE.createPackageImport();
		Module_managementPackagePI.setEPackage(Module_managementPackage.eINSTANCE);		
		actual.getEPackages().add(Module_managementPackagePI);
		
		PackageImport Efbt_data_definitionPackagePI = EFactoryFactory.eINSTANCE.createPackageImport();
		Efbt_data_definitionPackagePI.setEPackage(Efbt_data_definitionPackage.eINSTANCE);		
		actual.getEPackages().add(Efbt_data_definitionPackagePI);
		
		PackageImport MappingPackagePI = EFactoryFactory.eINSTANCE.createPackageImport();
		MappingPackagePI.setEPackage(MappingPackage.eINSTANCE);		
		actual.getEPackages().add(MappingPackagePI);
		
		PackageImport Vtl_transformationPackagePI = EFactoryFactory.eINSTANCE.createPackageImport();
		Vtl_transformationPackagePI.setEPackage(Vtl_transformationPackage.eINSTANCE);		
		actual.getEPackages().add(Vtl_transformationPackagePI);
		
		
/**		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/base_column_structured_data")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/base_column_structured_data",
					Base_column_structured_dataPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/smcubes_model")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/smcubes_model",
					Smcubes_modelPackage.eINSTANCE);
		}

		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/functionality_module")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/functionality_module",
					Functionality_modulePackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/reports")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/reports",
					ReportsPackage.eINSTANCE);
		}
		
		

		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/requirements_text")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/requirements_text",
					Requirements_textPackage.eINSTANCE);
		}
	
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/scenarios")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/scenarios", ScenariosPackage.eINSTANCE);
		}

		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/test_definition")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/test_definition", Test_definitionPackage.eINSTANCE);
		}
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/test_input_data")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/test_input_data", Test_input_dataPackage.eINSTANCE);
		}

		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/test")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/test", TestPackage.eINSTANCE);
		}
		
	
	
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/rendering")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/rendering", RenderingPackage.eINSTANCE);
		}
		
		*/
		

		
		//XtextResourceSet xrs = injector.getInstance(XtextResourceSet.class);
		XtextResourceSet xrs = new XtextResourceSet();
		

		URI smcubesURI = URI.createFileURI(outputFilepath + "//smcubesURI.eson");
		Resource smcubesResource = xrs.createResource(smcubesURI);
		smcubesResource.getContents().add(actual);
		smcubesResource.getContents().add(testModel);
		
		//URI modelURI = URI.createFileURI(outputFilepath + "modelURI.smcubes_model");
		//Resource modelResource = xrs.createResource(modelURI);
		//modelResource.getContents().add(testModel);
		
		//smcubesResource.getContents().add(testModel);
		//smcubesResource.getContents().get(1);
		//EcoreUtil.resolveAll(xrs);
		try {
			smcubesResource.save(Collections.EMPTY_MAP);
			/**IResourceDescription.Manager resourceDescriptionManager = injector.getInstance(IResourceDescription.Manager.class);
			Iterable<IEObjectDescription> eods = resourceDescriptionManager.getResourceDescription(smcubesResource).getExportedObjects();
			for (IEObjectDescription eod : eods) {
				System.out.println("\t" + eod.getQualifiedName() + " - " + eod.getClass().getName() );
			}
			OutputStream outputStream = new ByteArrayOutputStream();
			smcubesResource.save(outputStream, null);
			System.out.println("outputStream.toString()" + outputStream.toString());*/
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	

	/**private void saveArtifactsAsEson2() {
		// persist the files as a set of xtext files

				EPackage.Registry.INSTANCE.put("http://www.eclipse.org/emf/eson/EFactory", EFactoryPackage.eINSTANCE);
				FreeBirdToolsResourceFactory factory = new FreeBirdToolsResourceFactory();

				Resource.Factory.Registry reg = Resource.Factory.Registry.INSTANCE;
				Map<String, Object> m = reg.getExtensionToFactoryMap();
				m.put("cocamo_program", factory);

				EFactoryStandaloneSetup setup = new EFactoryStandaloneSetup();
				Injector i = setup.createInjectorAndDoEMFRegistration();
				
				//load the xml files into a resource, using the FreeBirdToolsResourceFactory
				XtextResourceSet xrs = i.getInstance(XtextResourceSet.class);
				
				
		
				URI domainsURI = URI.createFileURI(outputFilepath + "domains.eson");
				URI membersURI = URI.createFileURI(outputFilepath + "members.eson");
				URI variablesURI = URI.createFileURI(outputFilepath + "variables.eson");
				URI cubesURI = URI.createFileURI(outputFilepath + "cubes.eson");
				URI cubestructuresURI = URI.createFileURI(outputFilepath + "cube_structures.eson");
				URI cubestructureitemsURI = URI.createFileURI(outputFilepath + "cube_structure_items.eson");
			
				URI birdURI = URI.createFileURI(outputFilepath + "bird.eson");

				Resource domainsResource = xrs.createResource(domainsURI);
				Resource membersResource = xrs.createResource(membersURI);
				Resource variablesResource = xrs.createResource(variablesURI);
				Resource cubesResource = xrs.createResource(cubesURI);
				Resource cubestructuresResource = xrs.createResource(cubestructuresURI);
				Resource cubestructureitemsResource = xrs.createResource(cubestructureitemsURI);
		

				domainsResource.getContents().add(domains);
				membersResource.getContents().add(members);
				variablesResource.getContents().add(variables);
				cubesResource.getContents().add(cubesModule);
				cubestructuresResource.getContents().add(cubeStructuresModule);
				cubestructureitemsResource.getContents().add(cubeStructureItemsModule);
		
				EcoreUtil.resolveAll(xrs);
				
				//save the xtext representation of the model
				try {
					domainsResource.save(Collections.EMPTY_MAP);
					variablesResource.save(Collections.EMPTY_MAP);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				

		
	}*/

	/**
	 * Serialize all the EObjects to files.
	 */
	public void saveArtifactsAsXML() {
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
