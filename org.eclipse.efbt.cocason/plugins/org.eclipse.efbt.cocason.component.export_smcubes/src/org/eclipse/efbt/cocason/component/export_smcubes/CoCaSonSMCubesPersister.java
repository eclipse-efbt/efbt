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

import org.eclipse.efbt.cocamo.core.model.module_management.Module_managementPackage;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.CocamoPackage;
import org.eclipse.efbt.cocamo.smcubes.model.core.CorePackage;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.Data_definitionPackage;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.Efbt_mappingPackage;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_vtl_transformation.Efbt_vtl_transformationPackage;
import org.eclipse.efbt.cocamo.smcubes.model.mapping.MappingPackage;
import org.eclipse.efbt.cocamo.smcubes.model.vtl_transformation.Vtl_transformationPackage;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.serialization.FactoryBuilder;
import org.eclipse.xtext.resource.XtextResourceSet;
import org.eclipse.emf.eson.EFactoryServiceProvider;
import org.eclipse.emf.eson.eFactory.EFactoryFactory;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.PackageImport;

import com.google.inject.Injector;

public class CoCaSonSMCubesPersister {

	public void saveArtifactsAsCoCaSon(EObject testModel, String outputFilepath) {
		
		
		
	
		ESONWithcocamomodelInjectorProvider eip = new ESONWithcocamomodelInjectorProvider();
		Injector injector = eip.getInjector();
		
		EFactoryServiceProvider serviceProvider = injector.getInstance(EFactoryServiceProvider.class);
		
	
		FactoryBuilder builder = new FactoryBuilder(serviceProvider);
	
		Factory actual = builder.buildFactory(testModel);
		// it looks like we only add one package in the builder, the package of the 
		// rootbuilder, we are going to add all teh packages here to work around this.
		PackageImport cocamo_programPackagePI = EFactoryFactory.eINSTANCE.createPackageImport();
		cocamo_programPackagePI.setEPackage(CocamoPackage.eINSTANCE);		
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
		
		

		XtextResourceSet xrs = new XtextResourceSet();
		

		URI smcubesURI = URI.createFileURI(outputFilepath + "//smcubesURI.coca");
		Resource smcubesResource = xrs.createResource(smcubesURI);
		smcubesResource.getContents().add(actual);
		smcubesResource.getContents().add(testModel);
		
		
		try {
			smcubesResource.save(Collections.EMPTY_MAP);			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
