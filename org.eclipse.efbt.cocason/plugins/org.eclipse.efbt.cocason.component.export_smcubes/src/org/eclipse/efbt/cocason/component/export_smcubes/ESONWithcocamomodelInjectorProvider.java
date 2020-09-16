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
package org.eclipse.efbt.cocason.component.export_smcubes;


import org.eclipse.efbt.cocamo.core.model.functionality_module.Functionality_modulePackage;
import org.eclipse.efbt.cocamo.core.model.module_management.Module_managementPackage;
import org.eclipse.efbt.cocamo.core.model.requirements_text.Requirements_textPackage;
import org.eclipse.efbt.cocamo.core.model.scenarios.ScenariosPackage;
import org.eclipse.efbt.cocamo.core.model.test.TestPackage;
import org.eclipse.efbt.cocamo.core.model.test_definition.Test_definitionPackage;
import org.eclipse.efbt.cocamo.core.model.test_input_data.Test_input_dataPackage;
import org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage;
import org.eclipse.efbt.cocamo.smcubes.model.cocamo.CocamoPackage;
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
import org.eclipse.emf.ecore.EPackage;

import com.google.inject.Injector;

/**
 * 
 * @author Neil Mackenzie
 */
public class ESONWithcocamomodelInjectorProvider  extends EFactoryInjectorProvider {

	//@Override
	protected Injector internalCreateInjector() {
	

        if (!EPackage.Registry.INSTANCE.containsKey(Smcubes_modelPackage.eNS_URI))
            EPackage.Registry.INSTANCE.put(Smcubes_modelPackage.eNS_URI, Smcubes_modelPackage.eINSTANCE);
        
        if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/cocamo")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/cocamo", CocamoPackage.eINSTANCE);
		}

		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/base_column_structured_data")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/base_column_structured_data",
					Base_column_structured_dataPackage.eINSTANCE);
		}
		
		if (!EPackage.Registry.INSTANCE.containsKey("http://www.eclipse.org/efbt/smcubes_model")) {
			EPackage.Registry.INSTANCE.put("http://www.eclipse.org/efbt/smcubes_model",
					Smcubes_modelPackage.eINSTANCE);
		}

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
		
		return super.internalCreateInjector();
		
	}

}
