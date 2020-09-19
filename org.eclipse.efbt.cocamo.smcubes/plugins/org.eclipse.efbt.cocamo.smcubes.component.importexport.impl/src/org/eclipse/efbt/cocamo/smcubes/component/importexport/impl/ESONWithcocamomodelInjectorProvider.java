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
public class ESONWithcocamomodelInjectorProvider {// extends EFactoryInjectorProvider {

	//@Override
	protected Injector internalCreateInjector() {
		/**
		
	    // Needed because.. XtextRunner & *InjectorProvider & *StandaloneSetup
	    // stuff due to GlobalStateMemento in restoreGlobalState() will cause a
	    // confusing mess with hard-to-reproduce test failures if we don't do this.  
	    // To reproduce the original problem, create a JUnit Suite of 1. XcoreTest,
	    // 2. EcoreUtil3Test, 3. SimplestTest (in this exact order!), and you'll see.
        if (!EPackage.Registry.INSTANCE.containsKey(Smcubes_modelPackage.eNS_URI))
            EPackage.Registry.INSTANCE.put(Smcubes_modelPackage.eNS_URI, Smcubes_modelPackage.eINSTANCE);
        
        if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/cocamo")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/cocamo", cocamoPackage.eINSTANCE);
		}

		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/base_column_structured_data")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/base_column_structured_data",
					Base_column_structured_dataPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/smcubes_model")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/smcubes_model",
					Smcubes_modelPackage.eINSTANCE);
		}

	
	//	if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/column_structured_data")) {
	//		EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/column_structured_data",
	//				Column_structured_dataPackage.eINSTANCE);
	//	}
	//	if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/column_structures")) {
	//		EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/column_structures",
	//				Column_structuresPackage.eINSTANCE);
	//	}

		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/efbt_vtl_transformation")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/efbt_vtl_transformation",
					Efbt_vtl_transformationPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/functionality_module")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/functionality_module",
					Functionality_modulePackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/efbt_mapping")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/efbt_mapping",
					Efbt_mappingPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/data_definition")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/data_definition",
					Data_definitionPackage.eINSTANCE);
		}
		
		
		
	//	if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/input_data_structures")) {
	//		EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/input_data_structures",
	//				Input_data_structuresPackage.eINSTANCE);
	//	}
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/module_management")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/module_management",
					Module_managementPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/reports")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/reports",
					ReportsPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/efbt_data_definition")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/efbt_data_definition",
					Efbt_data_definitionPackage.eINSTANCE);
		}
		
		
		

		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/requirements_text")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/requirements_text",
					Requirements_textPackage.eINSTANCE);
		}
	
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/scenarios")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/scenarios", ScenariosPackage.eINSTANCE);
		}

		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/test_definition")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/test_definition", Test_definitionPackage.eINSTANCE);
		}
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/test_input_data")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/test_input_data", Test_input_dataPackage.eINSTANCE);
		}

		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/test")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/test", TestPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/mapping")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/mapping", MappingPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey(CorePackage.eNS_URI)) {
			EPackage.Registry.INSTANCE.put(CorePackage.eNS_URI, CorePackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/vtl_transformation")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/vtl_transformation", Vtl_transformationPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/rendering")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/rendering", RenderingPackage.eINSTANCE);
		}
		
		

		// NORMALLY TestmodelPackage.eINSTANCE.getTestModel(); // NOT TestmodelPackageImpl.init();
	    //  ^^^^^ but this doesn't really work reliably, so needs above

		return super.internalCreateInjector();*/
		return null;
	}

}
