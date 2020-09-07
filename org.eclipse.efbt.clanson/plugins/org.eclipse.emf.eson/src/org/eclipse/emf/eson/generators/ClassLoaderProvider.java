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
package org.eclipse.emf.eson.generators;

import org.eclipse.emf.ecore.resource.Resource;

import com.google.inject.ImplementedBy;

/**
 * Provides a ClassLoader, given an EMF Resource.
 *  
 * @author Michael Vorburger
 */
@ImplementedBy(ThreadContextClassLoaderProvider.class)
public interface ClassLoaderProvider {

	// TODO Propose this into Xtext core
	// see also very similar org.eclipse.emf.ecore.xcore.interpreter.IClassLoaderProvider

	/**
	 * Returns the class loader for this EMF Resource.
	 * 
	 * @return ClassLoader, never null
	 * @throws IllegalArgumentException if implementation could not find a ClassLoader from the given ResourceSet 
	 */
	ClassLoader getClassLoader(Resource resource) throws IllegalArgumentException;

}
