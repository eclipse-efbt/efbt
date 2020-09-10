/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2015 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.generators;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedHashSet;

import org.apache.log4j.Logger;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.jdt.core.JavaModelException;
import org.eclipse.xtend.ide.macro.JdtBasedProcessorProvider;

import com.google.common.collect.Sets;

/**
 * Extension of Xtend JdtBasedProcessorProvider, which allows to customize
 * ClassLoader, instead of being hard-coded to
 * org.eclipse.xtend.lib.macro.TransformationContext, as the parent class
 * (currently in Xtext 2.8.4) is.
 * 
 * @author Michael Vorburger (based on tips from Stephane Oehme)
 */
@SuppressWarnings("restriction")
public class ExtendedJdtBasedProcessorProvider extends JdtBasedProcessorProvider {
	private static Logger logger = Logger.getLogger(ExtendedJdtBasedProcessorProvider.class);

	// TODO Propose to Xtext to refactor JdtBasedProcessorProvider, and its parent ProcessorInstanceForJvmTypeProvider, into a more generic ClassLoaderProvider
	
	private ClassLoader parentClassLoader;
	
	public void setParentClassLoader(ClassLoader parentClassLoader) {
		this.parentClassLoader = parentClassLoader;
	}

	@Override
	protected ClassLoader getParentClassLoader() {
		// NOTE super() is hard-coded to TransformationContext - but we use IGenerator
		return parentClassLoader;
	}

	public URLClassLoader createClassLoaderForJavaProject(final IJavaProject projectToUse) {
		// do NOT use return super.createClassLoaderForJavaProject(projectToUse);
		// because that uses boolean includeOutputFolder = false instead of true:
		LinkedHashSet<URL> urls = Sets.newLinkedHashSet();
		try {
			collectClasspathURLs(projectToUse, urls, true, new LinkedHashSet<IJavaProject>());
		} catch(JavaModelException e) {
			if (!e.isDoesNotExist())
				logger.error(e.getMessage(), e);
		}
		return new URLClassLoader(urls.toArray(new URL[0]), getParentClassLoader());
	}
}
