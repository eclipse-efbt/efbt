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
package org.eclipse.efbt.component.datalineagecreator.generator.scala.engine;

import org.eclipse.emf.ecore.EValidator;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.xtext.generator.IGenerator;
import org.eclipse.xtext.generator.IOutputConfigurationProvider;
import org.eclipse.xtext.naming.DefaultDeclarativeQualifiedNameProvider;
import org.eclipse.xtext.naming.IQualifiedNameProvider;
import org.eclipse.xtext.resource.IResourceFactory;
import org.eclipse.xtext.resource.generic.AbstractGenericResourceRuntimeModule;
import org.eclipse.xtext.service.SingletonBinding;
import org.eclipse.efbt.component.datalineagecreator.generator.scala.templates.Root; 



/**
 * @author Neil Mackenzie
 *
 */
public class FreeBirdToolsGeneratorModule extends AbstractGenericResourceRuntimeModule {
	/**
	 * Return  a ResourceSet
	 * @return
	 */
	public Class<? extends ResourceSet> bindResourceSet() {
		return ResourceSetImpl.class;
	}

	/**
	 * Return a QualifiedNameProvider
	 */
	@Override
	public Class<? extends IQualifiedNameProvider> bindIQualifiedNameProvider() {
		return DefaultDeclarativeQualifiedNameProvider.class;
	}

	/**
	 * get file extensions
	 */
	@Override
	protected String getFileExtensions() {
		return "xmi";
	}

	/**
	 * Get the language name
	 */
	@Override
	protected String getLanguageName() {
		return "FreeBIRDTools";
	}

	/**
	 * Get a ResourceFactory 
	 * @return
	 */
	public Class<? extends IResourceFactory> bindIResourceFactory() {
		return FreeBirdToolsResourceFactory.class;
	}


	/**
	 * Get a ValidatorRegistry
	 * @return
	 */
	public EValidator.Registry bindEValidatorRegistry() {
		return EValidator.Registry.INSTANCE;
	}
	

	
	/**
	 * Get a Generator
	 * @return
	 */
	public Class<? extends IGenerator> bindIGenerator() {
		return Root.class;
	}

	/**
	 * Get  a OutputConfigurationProvider.
	 * @return
	 */
	@SingletonBinding
	public Class<? extends IOutputConfigurationProvider> bindIOutputConfigurationProvider() {
		return OutputConfigurationProvider.class;
	}

}
