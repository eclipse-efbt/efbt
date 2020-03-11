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
 *******************************************************************************/
package org.eclipse.efbt.generator.scala.engine;


import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EPackage.Registry;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.mwe.core.ConfigurationException;
import org.eclipse.emf.mwe.core.resources.ResourceLoaderFactory;
import org.eclipse.xtext.ISetup;
import org.eclipse.efbt.generator.scala.engine.GeneratorConfig;
import org.eclipse.efbt.generator.scala.engine.FreeBirdToolsGeneratorModule;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

/**
 * @author Neil Mackenzie
 *
 */
public class GeneratorStandaloneSetup  implements ISetup {

	
	/**
	 * the injector
	 */
	private Injector injector;
	/**
	 * the config
	 */
	private GeneratorConfig config;
	
	/**
	 * Set the config
	 * @param config
	 */
	public void setConfig(GeneratorConfig config) {
		this.config = config;
	}
	
	/**
	 * setDoInit
	 * @param init
	 */
	public void setDoInit (boolean init) {
		createInjectorAndDoEMFRegistration();
	}
	

	/**
	 * Consrructoe
	 */
	public GeneratorStandaloneSetup () {
	}
	
	/**
	 * Get Dynamic Module
	 * @return
	 */
	private Module getDynamicModule () {
		return new AbstractModule() {
			@Override
			protected void configure() {
				bind(GeneratorConfig.class).toInstance(config);
			}
		};
	}

	/**
	 * create the injector and do EMF registration
	 */
	@Override
	public Injector createInjectorAndDoEMFRegistration() {
		if (injector == null) {
			injector = Guice.createInjector(new FreeBirdToolsGeneratorModule(), getDynamicModule());
			register(injector);
		}
		return injector;
	}

	/**
	 * register. it is important here that we have register all the model packages names,
	 * And that we keep this up to date when new EMF model packages are added deleted or renamed. 
	 * @param injector
	 */
	public void register(Injector injector) {
		org.eclipse.xtext.resource.IResourceFactory resourceFactory = injector.getInstance(org.eclipse.xtext.resource.IResourceFactory.class);
		org.eclipse.xtext.resource.IResourceServiceProvider serviceProvider = injector.getInstance(org.eclipse.xtext.resource.IResourceServiceProvider.class);
		
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("data_definition", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("core", resourceFactory);
		
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("efbt_data_definition", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("row_transformation_logic", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("cube_transformation_logic", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("functions", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("module_management", resourceFactory);		
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("reports", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("base_cube_data", resourceFactory);	
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("cube_data", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("cubes", resourceFactory);
		//Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("data_lineage", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("typing", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("platform_call", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("attribute_lineage", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("rendering", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("cube_schema", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("navigation_context", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("platform_call", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("trl_report_cell_views", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("trl_sql_views", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("transformation", resourceFactory);
		
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("bird_model", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("efbt_mapping", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("efbt_vtl_transformation", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("mapping", resourceFactory);
		Resource.Factory.Registry.INSTANCE.getExtensionToFactoryMap().put("vtl_transformation", resourceFactory);
		
		

		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("data_definition", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("core", serviceProvider);
		
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("efbt_data_definition", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("row_transformation_logic", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("cube_transformation_logic", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("functions", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("module_management", serviceProvider);	
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("reports", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("base_cube_data", serviceProvider);						
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("cube_data", serviceProvider);		
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("cubes", serviceProvider);
		//org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("data_lineage", serviceProvider);		
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("typing", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("platform_call", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("attribute_lineage", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("rendering", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("cube_schema", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("navigation_context", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("platform_call", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("trl_report_cell_views", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("trl_sql_views", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("transformation", serviceProvider);
		
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("bird_model", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("efbt_mapping", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("efbt_vtl_transformation", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("mapping", serviceProvider);
		org.eclipse.xtext.resource.IResourceServiceProvider.Registry.INSTANCE.getExtensionToFactoryMap().put("vtl_transformation", serviceProvider);
		
		
		addRegisterGeneratedEPackage("platform_call.Platform_callPackage");		
			}
	
	/**
	 * Register Generate EPackage
	 * @param interfacename
	 */
	public void addRegisterGeneratedEPackage(String interfacename) {
		Registry registry  = EPackage.Registry.INSTANCE;
		Class<?> clazz = ResourceLoaderFactory.createResourceLoader().loadClass(interfacename);
		if (clazz == null)
			throw new ConfigurationException("Couldn't find an interface " + interfacename);
		try {
			EPackage pack = (EPackage) clazz.getDeclaredField("eINSTANCE").get(null);
			registry.put(pack.getNsURI(), pack);
			
		}
		catch (Exception e) {
			throw new ConfigurationException("Couldn't register " + interfacename
					+ ". Is it the generated EPackage interface? : " + e.getMessage());
		}
	}
	

	
}
