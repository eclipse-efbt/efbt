/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2020 Bird Software Solutions
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.efbt.cocason.component.export_smcubes;

import java.util.Collections;
import java.util.List;

import org.eclipse.efbt.cocamo.smcubes.component.importexport.impl.BIRDImporterImpl;
import org.eclipse.efbt.cocamo.smcubes.component.importexport.impl.Importer;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.CocamoFactory;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.Program;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.CubeModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.DomainModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.MemberModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.VariableModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_vtl_transformation.TransformationSchemeModule;
import org.eclipse.efbt.cocamo.smcubes.model.smcubes_model.SmcubesModel;
import org.eclipse.efbt.cocamo.smcubes.model.smcubes_model.Smcubes_modelFactory;
import org.eclipse.efbt.cocason.dsl.cocason.CoCaSonStandaloneSetup;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.xtext.resource.XtextResourceSet;

import com.google.inject.Injector;

public class ImportAndExportSMCubesToNewCocason {

	
	public void importAndExportSMCubes(String filepath, String outputFilepath, String testdatafilepath)
	{
		Importer  importer = new BIRDImporterImpl(); 
		importer.doImport(filepath,outputFilepath,testdatafilepath);
		SmcubesModel birdModel = importer.getBirdModel();
		//CoCaSonSMCubesPersister persister = new CoCaSonSMCubesPersister();
		XtextResourceSet xrs = new XtextResourceSet();
		//persister.saveArtifactsAsCoCaSon(birdModel, outputFilepath,xrs);
	}
	
	public static void translateBIRDWithNewTestFormatToCocason(String filepath, String outputFilepath, String testdatafilepath)
	{
		Importer  importer = new BIRDImporterImpl(); 
		importer.doImport(filepath,outputFilepath,testdatafilepath);
		importer.importTestDataWithNewTestFormat();
		persistModel(importer,  outputFilepath);
	}
	
	
	public static void translateBIRDWithOldTestFormatToCocason(String filepath, String outputFilepath, String testdatafilepath)
	{
		
		Importer  importer = new BIRDImporterImpl(); 
		importer.doImport(filepath,outputFilepath,testdatafilepath);
		importer.importTestDataWithOldTestFormat(testdatafilepath);
		persistModel(importer,  outputFilepath);
	}
	
	public static void persistModel(Importer  importer, String outputFilepath)
	{
		
	
		SmcubesModel birdModel = importer.getBirdModel();
		
		
	
		 EList<TransformationSchemeModule> tsRoot = birdModel.getFunctionalModules();
		 
		 SmcubesModel tsRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 tsRootModel.setName("TransformationSchemesModel");
		 tsRootModel.getFunctionalModules().add(tsRoot.get(0));
		 Program tsRootProgram = CocamoFactory.eINSTANCE.createProgram();
		 tsRootProgram.setSmCubesModel(tsRootModel);
		 
		 EList<DomainModule> domainsRoot = birdModel.getDomains();
		
		 SmcubesModel domainsRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 domainsRootModel.setName("domainsModel");
		 domainsRootModel.getDomains().add(domainsRoot.get(0));
		 Program domainsRootProgram = CocamoFactory.eINSTANCE.createProgram();
		 domainsRootProgram.setSmCubesModel(domainsRootModel);
		
		 EList<MemberModule> membersRoot = birdModel.getMembers();
		SmcubesModel membersRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		membersRootModel.setName("membersModel");
		membersRootModel.getMembers().add(membersRoot.get(0));
		Program membersRootProgram = CocamoFactory.eINSTANCE.createProgram();
		membersRootProgram.setSmCubesModel(membersRootModel);
		
		 EList<VariableModule> variablesRoot = birdModel.getVariables();
			SmcubesModel variablesRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
			variablesRootModel.setName("variablesModel");
			variablesRootModel.getVariables().add(variablesRoot.get(0));
			Program variablesRootProgram = CocamoFactory.eINSTANCE.createProgram();
			variablesRootProgram.setSmCubesModel(variablesRootModel);
			
		//EObject combinationsRoot = (EObject) birdModel.getCombinations();
		 EList<CubeModule> cubesRoot = birdModel.getCubes();
		 
		 
		 
		 
		 SmcubesModel cubesRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 cubesRootModel.setName("cubesModel");
		 cubesRootModel.getCubes().add(cubesRoot.get(0));
		 Program cubesRootProgram = CocamoFactory.eINSTANCE.createProgram();
		 cubesRootProgram.setSmCubesModel(cubesRootModel);
		 
		 SmcubesModel cube_structuresRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 cube_structuresRootModel.setName("cubeStructuresModel");
		 cube_structuresRootModel.getCubes().add(cubesRoot.get(0));
		 Program cube_structuresRootProgram = CocamoFactory.eINSTANCE.createProgram();
		 cube_structuresRootProgram.setSmCubesModel(cube_structuresRootModel);
		 
		 SmcubesModel cube_structure_itemsRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 cube_structure_itemsRootModel.setName("cubeStructureItemsModel");
		 cube_structure_itemsRootModel.getCubes().add(cubesRoot.get(0));
		 Program cube_structure_itemsRootProgram = CocamoFactory.eINSTANCE.createProgram();
		 cube_structure_itemsRootProgram.setSmCubesModel(cube_structure_itemsRootModel);
		 
		 
		 
		
		
		
		CoCaSonStandaloneSetup setup = new CoCaSonStandaloneSetup();
		Injector i = setup.createInjectorAndDoEMFRegistration();
		ResourceSet xrs = i.getInstance(ResourceSet.class);
		
		saveArtifactsAsCoCaSon(tsRootProgram, outputFilepath + "tsRoot.coca",xrs);
		saveArtifactsAsCoCaSon(domainsRootProgram, outputFilepath + "domainsRoot.coca",xrs);
		saveArtifactsAsCoCaSon(membersRootProgram, outputFilepath + "membersRoot.coca",xrs);
		saveArtifactsAsCoCaSon(variablesRootProgram, outputFilepath + "variablesRoot.coca",xrs);
		saveArtifactsAsCoCaSon(cube_structuresRootProgram, outputFilepath + "cube_structuresRoot.coca",xrs);
		//saveArtifactsAsCoCaSon(combinationsRoot, outputFilepath + "combinationsRoot.coca",xrs);
		saveArtifactsAsCoCaSon(cubesRootProgram, outputFilepath + "cubesRoot.coca",xrs);		
		saveArtifactsAsCoCaSon(cube_structure_itemsRootProgram, outputFilepath + "cube_structure_itemsRoot.coca",xrs);
		saveArtifactsAsCoCaSon(importer.functionalityModulesProgram, outputFilepath + "functionalityModulesProgram.coca",xrs);
		
		saveArtifactsAsCoCaSon(importer.testTemplateProgram, outputFilepath + "testTemplateProgram.coca",xrs);
		saveArtifactsAsCoCaSon(importer.testConstraintsProgram, outputFilepath + "testConstraintsProgram.coca",xrs);
		saveArtifactsAsCoCaSon(importer.testDefinitionProgram, outputFilepath + "testDefinitionProgram.coca",xrs);
		
		
		List<Program> tests = importer.testPrograms;
		int counter=0;
		for (Program program : tests) {
			counter++;
			saveArtifactsAsCoCaSon(program, outputFilepath + "testProgram" + counter + ".coca",xrs);
		}
		
	}
	
	private static void saveArtifactsAsCoCaSon(EObject o, String outputFilepath, ResourceSet xrs) {
		URI smcubesURI = URI.createFileURI(outputFilepath );
		Resource smcubesResource = xrs.createResource(smcubesURI);
		smcubesResource.getContents().add(o);
		
		
		
		try {
			smcubesResource.save(Collections.EMPTY_MAP);			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
