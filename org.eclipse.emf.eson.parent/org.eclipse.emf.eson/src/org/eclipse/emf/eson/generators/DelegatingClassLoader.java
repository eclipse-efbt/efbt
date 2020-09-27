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

public class DelegatingClassLoader extends ClassLoader {

	private final ClassLoader firstClassLoader;
	private final ClassLoader secondClassLoader;

	public DelegatingClassLoader(ClassLoader firstClassLoader, ClassLoader secondClassLoader) {
		this.firstClassLoader = firstClassLoader; 
		this.secondClassLoader = secondClassLoader; 
	}

	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		// NOT return super.findClass(name);
		
		try {
			return firstClassLoader.loadClass(name);
		} catch (ClassNotFoundException e) {
		}

		return secondClassLoader.loadClass(name);
	}
	
}
