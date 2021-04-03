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
package org.eclipse.efbt.controller.smcubes.component.export_smcubes;

import java.util.Collections;
import java.util.List;

import org.eclipse.efbt.controller.smcubes.component.importexport.impl.BIRDImporterImpl;
import org.eclipse.efbt.controller.smcubes.component.importexport.impl.Importer;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.DomainModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.MemberModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.VariableModule;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.TransformationSchemeModule;
import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel;
import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.Smcubes_modelFactory;
import org.eclipse.efbt.cocason.dsl.cocason.CoCaSonStandaloneSetup;
import org.eclipse.efbt.controller.core.model.platform_call.Platform_callFactory;
import org.eclipse.efbt.controller.core.model.platform_call.ProgramAndCalls;
import org.eclipse.efbt.language.dtm.model.transformation.dtmProgram;
import org.eclipse.efbt.language.dtm.model.transformation.TransformationFactory;
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
		 
		 dtmProgram tsProgram =  TransformationFactory.eINSTANCE.createdtmProgram();	
		 tsProgram.setSmcubesStaticModel(tsRootProgram);
		 ProgramAndCalls tsProgramAndCalls = Platform_callFactory.eINSTANCE.createProgramAndCalls();
		 tsProgramAndCalls.setProgram(tsProgram);
		 EList<DomainModule> domainsRoot = birdModel.getDomains();
		
		 SmcubesModel domainsRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 domainsRootModel.setName("domainsModel");
		 domainsRootModel.getDomains().add(domainsRoot.get(0));
		 SMCubesStaticModel domainsRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
		 domainsRootProgram.setSmCubesModel(domainsRootModel);
		 domainsRootProgram.setName("domainsStaticModel");		 
		 dtmProgram domainsProgram =  TransformationFactory.eINSTANCE.createdtmProgram();	
		 domainsProgram.setSmcubesStaticModel(domainsRootProgram);
		 ProgramAndCalls domainsProgramAndCalls = Platform_callFactory.eINSTANCE.createProgramAndCalls();
		 domainsProgramAndCalls.setProgram(domainsProgram);
		
		 EList<MemberModule> membersRoot = birdModel.getMembers();
		SmcubesModel membersRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		membersRootModel.setName("membersModel");
		membersRootModel.getMembers().add(membersRoot.get(0));
		SMCubesStaticModel membersRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
		membersRootProgram.setSmCubesModel(membersRootModel);
		membersRootProgram.setName("membersStaticModel");		 
		 dtmProgram membersProgram =  TransformationFactory.eINSTANCE.createdtmProgram();	
		 membersProgram.setSmcubesStaticModel(membersRootProgram);
		 ProgramAndCalls membersProgramAndCalls = Platform_callFactory.eINSTANCE.createProgramAndCalls();
		 membersProgramAndCalls.setProgram(membersProgram);
		
		 EList<VariableModule> variablesRoot = birdModel.getVariables();
			SmcubesModel variablesRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
			variablesRootModel.setName("variablesModel");
			variablesRootModel.getVariables().add(variablesRoot.get(0));
			SMCubesStaticModel variablesRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
			variablesRootProgram.setSmCubesModel(variablesRootModel);
			variablesRootProgram.setName("variablesStaticModel");		 
			 dtmProgram variablesProgram =  TransformationFactory.eINSTANCE.createdtmProgram();	
			 variablesProgram.setSmcubesStaticModel(variablesRootProgram);
			 ProgramAndCalls variablesProgramAndCalls = Platform_callFactory.eINSTANCE.createProgramAndCalls();
			 variablesProgramAndCalls.setProgram(variablesProgram);
		//EObject combinationsRoot = (EObject) birdModel.getCombinations();
		 EList<CubeModule> cubesRoot = birdModel.getCubes();
		 
		 
		 
		 
		 SmcubesModel cubesRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 cubesRootModel.setName("cubesModel");
		 cubesRootModel.getCubes().add(cubesRoot.get(0));
		 SMCubesStaticModel cubesRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
		 cubesRootProgram.setSmCubesModel(cubesRootModel);
		 cubesRootProgram.setName("cubesStaticModel");		 
		 dtmProgram cubesProgram =  TransformationFactory.eINSTANCE.createdtmProgram();	
		 cubesProgram.setSmcubesStaticModel(cubesRootProgram);
		 ProgramAndCalls cubesProgramAndCalls = Platform_callFactory.eINSTANCE.createProgramAndCalls();
		 cubesProgramAndCalls.setProgram(cubesProgram);
		 
		 SmcubesModel cube_structuresRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 cube_structuresRootModel.setName("cubeStructuresModel");
		 cube_structuresRootModel.getCubes().add(cubesRoot.get(0));
		 SMCubesStaticModel cube_structuresRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
		 cube_structuresRootProgram.setSmCubesModel(cube_structuresRootModel);
		 cube_structuresRootProgram.setName("cube_structuresStaticModel");		 
		 dtmProgram cube_structuresProgram =  TransformationFactory.eINSTANCE.createdtmProgram();	
		 cube_structuresProgram.setSmcubesStaticModel(cube_structuresRootProgram);
		 ProgramAndCalls cube_structuresProgramAndCalls = Platform_callFactory.eINSTANCE.createProgramAndCalls();
		 cube_structuresProgramAndCalls.setProgram(cube_structuresProgram);
		 
		 SmcubesModel cube_structure_itemsRootModel = Smcubes_modelFactory.eINSTANCE.createSmcubesModel();
		 cube_structure_itemsRootModel.setName("cubeStructureItemsModel");
		 cube_structure_itemsRootModel.getCubes().add(cubesRoot.get(0));
		 SMCubesStaticModel cube_structure_itemsRootProgram = CocamoFactory.eINSTANCE.createSMCubesStaticModel();
		 cube_structure_itemsRootProgram.setSmCubesModel(cube_structure_itemsRootModel);
		 cube_structure_itemsRootProgram.setName("cube_structure_itemsStaticModel");		 
		 dtmProgram cube_structure_itemsProgram =  TransformationFactory.eINSTANCE.createdtmProgram();	
		 cube_structure_itemsProgram.setSmcubesStaticModel(cube_structure_itemsRootProgram);
		 ProgramAndCalls cube_structure_itemsProgramAndCalls = Platform_callFactory.eINSTANCE.createProgramAndCalls();
		 cube_structure_itemsProgramAndCalls.setProgram(cube_structure_itemsProgram);
		 
		 importer.functionalityModulesProgram.setName("functionalityStaticModel");	 
		 dtmProgram functionalityProgram =  TransformationFactory.eINSTANCE.createdtmProgram();	
		 functionalityProgram.setSmcubesStaticModel(importer.functionalityModulesProgram);
		
		 ProgramAndCalls functionalityProgramAndCalls = Platform_callFactory.eINSTANCE.createProgramAndCalls();
		 functionalityProgramAndCalls.setProgram(functionalityProgram);
		 
		 importer.testTemplateProgram.setName("testTemplateStaticModel");	
		 dtmProgram ttProgram =  TransformationFactory.eINSTANCE.createdtmProgram();	
		 ttProgram.setSmcubesStaticModel(importer.testTemplateProgram);
		 ProgramAndCalls ttProgramAndCalls = Platform_callFactory.eINSTANCE.createProgramAndCalls();
		 ttProgramAndCalls.setProgram(ttProgram);
			
		 importer.testConstraintsProgram.setName("testConstraintsStaticModel");
		 dtmProgram tcProgram =  TransformationFactory.eINSTANCE.createdtmProgram();	
		 tcProgram.setSmcubesStaticModel(importer.testConstraintsProgram);
		 ProgramAndCalls tcProgramAndCalls = Platform_callFactory.eINSTANCE.createProgramAndCalls();
		 tcProgramAndCalls.setProgram(tcProgram);
		 
		 importer.testDefinitionProgram.setName("testDefinitionsStaticModel");
		 dtmProgram tdProgram =  TransformationFactory.eINSTANCE.createdtmProgram();	
		 tdProgram.setSmcubesStaticModel(importer.testDefinitionProgram);
		 ProgramAndCalls tdProgramAndCalls = Platform_callFactory.eINSTANCE.createProgramAndCalls();
		 tdProgramAndCalls.setProgram(tdProgram);
		 
		CoCaSonStandaloneSetup setup = new CoCaSonStandaloneSetup();
		Injector i = setup.createInjectorAndDoEMFRegistration();
		ResourceSet xrs = i.getInstance(ResourceSet.class);
		
		saveArtifactsAsCoCaSon(tsProgramAndCalls, outputFilepath + "tsRoot.coca",xrs);
		saveArtifactsAsCoCaSon(domainsProgramAndCalls, outputFilepath + "domainsRoot.coca",xrs);
		saveArtifactsAsCoCaSon(membersProgramAndCalls, outputFilepath + "membersRoot.coca",xrs);
		saveArtifactsAsCoCaSon(variablesProgramAndCalls, outputFilepath + "variablesRoot.coca",xrs);
		saveArtifactsAsCoCaSon(cube_structuresProgramAndCalls, outputFilepath + "cube_structuresRoot.coca",xrs);
		//saveArtifactsAsCoCaSon(combinationsRoot, outputFilepath + "combinationsRoot.coca",xrs);
		saveArtifactsAsCoCaSon(cubesProgramAndCalls, outputFilepath + "cubesRoot.coca",xrs);		
		saveArtifactsAsCoCaSon(cube_structure_itemsProgramAndCalls, outputFilepath + "cube_structure_itemsRoot.coca",xrs);
		
		
		
		saveArtifactsAsCoCaSon(functionalityProgramAndCalls, outputFilepath + "functionalityModulesProgram.coca",xrs);		
		saveArtifactsAsCoCaSon(ttProgramAndCalls, outputFilepath + "testTemplateProgram.coca",xrs);
		saveArtifactsAsCoCaSon(tcProgramAndCalls, outputFilepath + "testConstraintsProgram.coca",xrs);
		saveArtifactsAsCoCaSon(tdProgramAndCalls, outputFilepath + "testDefinitionProgram.coca",xrs);
		
		
		List<SMCubesStaticModel> tests = importer.testPrograms;
		int counter=0;
		for (SMCubesStaticModel program : tests) {
			counter++;
			
			program.setName("TestStaticModel"+ counter);
			 dtmProgram  testProgram = TransformationFactory.eINSTANCE.createdtmProgram();	
			 testProgram.setSmcubesStaticModel(program);
			 
			 ProgramAndCalls testProgramAndCalls = Platform_callFactory.eINSTANCE.createProgramAndCalls();
			 testProgramAndCalls.setProgram(testProgram);
			saveArtifactsAsCoCaSon(testProgramAndCalls, outputFilepath + "testProgram" + counter + ".coca",xrs);
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
