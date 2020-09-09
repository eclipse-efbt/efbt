/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.efbt.cocamo.smcubes.component.importexport.impl;

import org.eclipse.efbt.cocamo.model.cocamo.cocamoPackage;
import org.eclipse.efbt.cocamo.core.model.module_management.Module_managementPackage;
import org.eclipse.efbt.cocamo.core.model.requirements_text.Requirements_textPackage;
import org.eclipse.efbt.cocamo.smcubes.model.core.CorePackage;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.Data_definitionPackage;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.Efbt_data_definitionPackage;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.Efbt_mappingPackage;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_vtl_transformation.Efbt_vtl_transformationPackage;
import org.eclipse.efbt.cocamo.smcubes.model.mapping.MappingPackage;
import org.eclipse.efbt.cocamo.smcubes.model.rendering.RenderingPackage;
import org.eclipse.efbt.cocamo.smcubes.model.reports.ReportsPackage;
import org.eclipse.efbt.cocamo.smcubes.model.smcubes_model.Smcubes_modelPackage;
import org.eclipse.efbt.cocamo.smcubes.model.vtl_transformation.Vtl_transformationPackage;
import org.eclipse.efbt.cocamo.core.model.functionality_module.Functionality_modulePackage;
import org.eclipse.efbt.functional_design.core.model.scenarios.ScenariosPackage;
import org.eclipse.efbt.input_data.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;
import org.eclipse.efbt.testing.common.model.test.TestPackage;
import org.eclipse.efbt.testing.common.model.test_definition.Test_definitionPackage;
import org.eclipse.efbt.testing.common.model.test_input_data.Test_input_dataPackage;
import org.eclipse.emf.ecore.EPackage;


import com.google.inject.Injector;

/**
 * IInjectorProvider for ESON + TestmodelPackage.
 * 
 * This is needed because the approach of just putting
 * TestmodelPackageImpl.init() into the @Before of a *Test doesn't quite work
 * reliably, it works for one @Test but not several, because "*PackageImpl
 * init() will not always put itself into the registry. It does that only on
 * first call. IRegistryConfigurator . That will create a copy of the registries
 * before each test and set that state back after the test has run." Using @BeforeClass
 * works as well, but completely defeats the purpose.
 * 
 * @see ESONWithTestmodelInjectorProviderTest
 * 
 * @author Michael Vorburger, based on advise from Sven Efftinge & Anton Kosyakov
 */
public class ESONWithcocamomodelInjectorProvider extends EFactoryInjectorProvider {

	@Override
	protected Injector internalCreateInjector() {
	    // Needed because.. XtextRunner & *InjectorProvider & *StandaloneSetup
	    // stuff due to GlobalStateMemento in restoreGlobalState() will cause a
	    // confusing mess with hard-to-reproduce test failures if we don't do this.  
	    // To reproduce the original problem, create a JUnit Suite of 1. XcoreTest,
	    // 2. EcoreUtil3Test, 3. SimplestTest (in this exact order!), and you'll see.
        if (!EPackage.Registry.INSTANCE.containsKey(Smcubes_modelPackage.eNS_URI))
            EPackage.Registry.INSTANCE.put(Smcubes_modelPackage.eNS_URI, Smcubes_modelPackage.eINSTANCE);
        
        if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/cocamo")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/cocamo", cocamoPackage.eINSTANCE);
		}

		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/base_column_structured_data")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/base_column_structured_data",
					Base_column_structured_dataPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/smcubes_model")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/smcubes_model",
					Smcubes_modelPackage.eINSTANCE);
		}

	
	//	if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/column_structured_data")) {
	//		EPackage.Registry.INSTANCE.put("http://www.eclipse.org/column_structured_data",
	//				Column_structured_dataPackage.eINSTANCE);
	//	}
	//	if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/column_structures")) {
	//		EPackage.Registry.INSTANCE.put("http://www.eclipse.org/column_structures",
	//				Column_structuresPackage.eINSTANCE);
	//	}

		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt_vtl_transformation")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt_vtl_transformation",
					Efbt_vtl_transformationPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/functionality_module")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/functionality_module",
					Functionality_modulePackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt_mapping")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt_mapping",
					Efbt_mappingPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/data_definition")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/data_definition",
					Data_definitionPackage.eINSTANCE);
		}
		
		
		
	//	if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/input_data_structures")) {
	//		EPackage.Registry.INSTANCE.put("http://www.eclipse.org/input_data_structures",
	//				Input_data_structuresPackage.eINSTANCE);
	//	}
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/module_management")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/module_management",
					Module_managementPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/reports")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/reports",
					ReportsPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt_data_definition")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt_data_definition",
					Efbt_data_definitionPackage.eINSTANCE);
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
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/mapping")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/mapping", MappingPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey(CorePackage.eNS_URI)) {
			EPackage.Registry.INSTANCE.put(CorePackage.eNS_URI, CorePackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/vtl_transformation")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/vtl_transformation", Vtl_transformationPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/rendering")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/rendering", RenderingPackage.eINSTANCE);
		}
		
		

		// NORMALLY TestmodelPackage.eINSTANCE.getTestModel(); // NOT TestmodelPackageImpl.init();
	    //  ^^^^^ but this doesn't really work reliably, so needs above

		return super.internalCreateInjector();
	}

}
