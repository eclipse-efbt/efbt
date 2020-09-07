/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2015 - 2016 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.generators;

import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import org.apache.log4j.Logger;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.xtext.generator.IFileSystemAccess;
import org.eclipse.xtext.generator.IGenerator;

import com.google.inject.Inject;

/**
 * An IGenerator which delegates to other IGenerator listed in a
 * META-INF/services/org.eclipse.xtext.generator.IGenerator on the classpath of
 * the project of the input Resource being generated, or via the Eclipse plugin
 * system.
 * 
 * @author Michael Vorburger, based on ClassLoader magic tips from Stefan Oehme
 */
public class DelegatingGenerator implements IGenerator {
	private static final Logger logger = Logger.getLogger(DelegatingGenerator.class);

	protected @Inject ClassLoaderProvider classLoaderProvider;
	protected @Inject EFactoryExtensionRegistry registryReaderUtil;
	
	@Override
	public void doGenerate(Resource input, IFileSystemAccess fsa) {
		delegateToProjectRuntimeClasspathGenerators(input, fsa);
		delegateToPluginGenerators(input, fsa);
	}

	protected void delegateToPluginGenerators(Resource input, IFileSystemAccess fsa) {
		Iterable<IGenerator> generators = registryReaderUtil.getGenerators();
		invokeGeneratorsAndHandleErrors(input, fsa, generators, "project runtime classpath generator ");
	}

	protected void delegateToProjectRuntimeClasspathGenerators(Resource input, IFileSystemAccess fsa) {
		ClassLoader classLoader = classLoaderProvider.getClassLoader(input);
		Iterable<IGenerator> generators = getRuntimeGenerators(classLoader);
		invokeGeneratorsAndHandleErrors(input, fsa, generators, "project runtime classpath generator ");
	}

	protected void invokeGeneratorsAndHandleErrors(Resource input, IFileSystemAccess fsa, Iterable<IGenerator> generators, String errorMessagePrefix) {
		try {
			for (IGenerator generator : generators) {
				try {
	 				generator.doGenerate(input, fsa);
				} catch (RuntimeException e) {
					logger.error("Caught exception from doGenerate() of " + errorMessagePrefix + generator.toString() + " for URI: " + input.getURI(), e);
					throw e;
				}
			}
		} catch (ServiceConfigurationError e) {
			logger.error("Caught exception when iterating over ServiceLoader", e);
			throw e;
		}
	}

	protected ServiceLoader<IGenerator> getRuntimeGenerators(ClassLoader classLoader) {
		try {
			return ServiceLoader.load(IGenerator.class, classLoader);
		} catch (ServiceConfigurationError e) {
			logger.error("Caught exception when obtaining project runtime classpath generators", e);
			throw e;
		}
	}

}
