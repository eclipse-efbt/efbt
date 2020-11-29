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
package org.eclipse.efbt.cocason.component.export_smcubes;

import java.util.Collections;
import java.util.List;

import org.eclipse.efbt.cocamo.smcubes.component.importexport.impl.BIRDImporterImpl;
import org.eclipse.efbt.cocamo.smcubes.component.importexport.impl.Importer;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.CocamoFactory;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.SMCubesStaticModel;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.CubeModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.DomainModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.MemberModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.VariableModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_vtl_transformation.TransformationSchemeModule;
import org.eclipse.efbt.cocamo.smcubes.model.smcubes_model.SmcubesModel;
import org.eclipse.efbt.cocamo.smcubes.model.smcubes_model.Smcubes_modelFactory;
import org.eclipse.efbt.cocason.dsl.cocason.CoCaSonStandaloneSetup;
import org.eclipse.efbt.controller.core.model.platform_call.Platform_callFactory;
import org.eclipse.efbt.controller.core.model.platform_call.SMCubesProgram;
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
		 SMCubesStaticModel tsRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
		 tsRootProgram.setSmCubesModel(tsRootModel);
		 tsRootProgram.setName("TransformationSchemesStaticModel");		 
		 SMCubesProgram tsProgram =  Platform_callFactory.eINSTANCE.createSMCubesProgram();	
		 tsProgram.setSmcubesStaticModel(tsRootProgram);
		 
		 EList<DomainModule> domainsRoot = birdModel.getDomains();
		
		 SmcubesModel domainsRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 domainsRootModel.setName("domainsModel");
		 domainsRootModel.getDomains().add(domainsRoot.get(0));
		 SMCubesStaticModel domainsRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
		 domainsRootProgram.setSmCubesModel(domainsRootModel);
		 domainsRootProgram.setName("domainsStaticModel");		 
		 SMCubesProgram domainsProgram =  Platform_callFactory.eINSTANCE.createSMCubesProgram();	
		 domainsProgram.setSmcubesStaticModel(domainsRootProgram);
		
		 EList<MemberModule> membersRoot = birdModel.getMembers();
		SmcubesModel membersRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		membersRootModel.setName("membersModel");
		membersRootModel.getMembers().add(membersRoot.get(0));
		SMCubesStaticModel membersRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
		membersRootProgram.setSmCubesModel(membersRootModel);
		membersRootProgram.setName("membersStaticModel");		 
		 SMCubesProgram membersProgram =  Platform_callFactory.eINSTANCE.createSMCubesProgram();	
		 membersProgram.setSmcubesStaticModel(membersRootProgram);
		
		 EList<VariableModule> variablesRoot = birdModel.getVariables();
			SmcubesModel variablesRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
			variablesRootModel.setName("variablesModel");
			variablesRootModel.getVariables().add(variablesRoot.get(0));
			SMCubesStaticModel variablesRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
			variablesRootProgram.setSmCubesModel(variablesRootModel);
			variablesRootProgram.setName("variablesStaticModel");		 
			 SMCubesProgram variablesProgram =  Platform_callFactory.eINSTANCE.createSMCubesProgram();	
			 variablesProgram.setSmcubesStaticModel(variablesRootProgram);
		//EObject combinationsRoot = (EObject) birdModel.getCombinations();
		 EList<CubeModule> cubesRoot = birdModel.getCubes();
		 
		 
		 
		 
		 SmcubesModel cubesRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 cubesRootModel.setName("cubesModel");
		 cubesRootModel.getCubes().add(cubesRoot.get(0));
		 SMCubesStaticModel cubesRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
		 cubesRootProgram.setSmCubesModel(cubesRootModel);
		 cubesRootProgram.setName("cubesStaticModel");		 
		 SMCubesProgram cubesProgram =  Platform_callFactory.eINSTANCE.createSMCubesProgram();	
		 cubesProgram.setSmcubesStaticModel(cubesRootProgram);
		 
		 SmcubesModel cube_structuresRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 cube_structuresRootModel.setName("cubeStructuresModel");
		 cube_structuresRootModel.getCubes().add(cubesRoot.get(0));
		 SMCubesStaticModel cube_structuresRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
		 cube_structuresRootProgram.setSmCubesModel(cube_structuresRootModel);
		 cube_structuresRootProgram.setName("cube_structuresStaticModel");		 
		 SMCubesProgram cube_structuresProgram =  Platform_callFactory.eINSTANCE.createSMCubesProgram();	
		 cube_structuresProgram.setSmcubesStaticModel(cube_structuresRootProgram);
		 
		 SmcubesModel cube_structure_itemsRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 cube_structure_itemsRootModel.setName("cubeStructureItemsModel");
		 cube_structure_itemsRootModel.getCubes().add(cubesRoot.get(0));
		 SMCubesStaticModel cube_structure_itemsRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
		 cube_structure_itemsRootProgram.setSmCubesModel(cube_structure_itemsRootModel);
		 cube_structure_itemsRootProgram.setName("cube_structure_itemsStaticModel");		 
		 SMCubesProgram cube_structure_itemsProgram =  Platform_callFactory.eINSTANCE.createSMCubesProgram();	
		 cube_structure_itemsProgram.setSmcubesStaticModel(cube_structure_itemsRootProgram);
		 
		 
		 importer.functionalityModulesProgram.setName("functionalityStaticModel");	 
		 SMCubesProgram functionalityProgram =  Platform_callFactory.eINSTANCE.createSMCubesProgram();	
		 functionalityProgram.setSmcubesStaticModel(importer.functionalityModulesProgram);
		
		 importer.testTemplateProgram.setName("testTemplateStaticModel");	
		 SMCubesProgram ttProgram =  Platform_callFactory.eINSTANCE.createSMCubesProgram();	
		 ttProgram.setSmcubesStaticModel(importer.testTemplateProgram);
		 
		 importer.testConstraintsProgram.setName("testConstraintsStaticModel");
		 SMCubesProgram tcProgram =  Platform_callFactory.eINSTANCE.createSMCubesProgram();	
		 tcProgram.setSmcubesStaticModel(importer.testConstraintsProgram);
		 
		 importer.testDefinitionProgram.setName("testDefinitionsStaticModel");
		 SMCubesProgram tdProgram =  Platform_callFactory.eINSTANCE.createSMCubesProgram();	
		 tdProgram.setSmcubesStaticModel(importer.testDefinitionProgram);
		 
		 
		CoCaSonStandaloneSetup setup = new CoCaSonStandaloneSetup();
		Injector i = setup.createInjectorAndDoEMFRegistration();
		ResourceSet xrs = i.getInstance(ResourceSet.class);
		
		saveArtifactsAsCoCaSon(tsProgram, outputFilepath + "tsRoot.coca",xrs);
		saveArtifactsAsCoCaSon(domainsProgram, outputFilepath + "domainsRoot.coca",xrs);
		saveArtifactsAsCoCaSon(membersProgram, outputFilepath + "membersRoot.coca",xrs);
		saveArtifactsAsCoCaSon(variablesProgram, outputFilepath + "variablesRoot.coca",xrs);
		saveArtifactsAsCoCaSon(cube_structuresProgram, outputFilepath + "cube_structuresRoot.coca",xrs);
		//saveArtifactsAsCoCaSon(combinationsRoot, outputFilepath + "combinationsRoot.coca",xrs);
		saveArtifactsAsCoCaSon(cubesProgram, outputFilepath + "cubesRoot.coca",xrs);		
		saveArtifactsAsCoCaSon(cube_structure_itemsProgram, outputFilepath + "cube_structure_itemsRoot.coca",xrs);
		
		
		
		saveArtifactsAsCoCaSon(functionalityProgram, outputFilepath + "functionalityModulesProgram.coca",xrs);		
		saveArtifactsAsCoCaSon(ttProgram, outputFilepath + "testTemplateProgram.coca",xrs);
		saveArtifactsAsCoCaSon(tcProgram, outputFilepath + "testConstraintsProgram.coca",xrs);
		saveArtifactsAsCoCaSon(tdProgram, outputFilepath + "testDefinitionProgram.coca",xrs);
		
		
		List<SMCubesStaticModel> tests = importer.testPrograms;
		int counter=0;
		for (SMCubesStaticModel program : tests) {
			counter++;
			
			program.setName("TestStaticModel"+ counter);
			 SMCubesProgram testProgram =  Platform_callFactory.eINSTANCE.createSMCubesProgram();	
			 testProgram.setSmcubesStaticModel(program);
			saveArtifactsAsCoCaSon(testProgram, outputFilepath + "testProgram" + counter + ".coca",xrs);
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
