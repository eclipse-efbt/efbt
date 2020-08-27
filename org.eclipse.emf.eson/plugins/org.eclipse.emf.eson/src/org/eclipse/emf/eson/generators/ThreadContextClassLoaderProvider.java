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


/**
 * ClassLoaderProvider using Thread.currentThread().getContextClassLoader().
 *  
 * @author Michael Vorburger
 */
public class ThreadContextClassLoaderProvider implements ClassLoaderProvider {

	/**
	 * Returns Thread.currentThread().getContextClassLoader().
	 * 
	 * The argument is ignored.
	 */
	@Override
	public ClassLoader getClassLoader(Resource resource) throws IllegalArgumentException {
		return Thread.currentThread().getContextClassLoader();
	}

}
