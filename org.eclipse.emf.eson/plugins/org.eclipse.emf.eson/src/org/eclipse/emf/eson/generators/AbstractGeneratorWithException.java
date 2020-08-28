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

import org.apache.log4j.Logger;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.xtext.generator.IFileSystemAccess;
import org.eclipse.xtext.generator.IGenerator;

public abstract class AbstractGeneratorWithException implements IGenerator {
	private static final Logger logger = Logger.getLogger(AbstractGeneratorWithException.class);

	abstract void doGenerateWithException(Resource input, IFileSystemAccess fsa) throws Exception;
	
	final @Override public void doGenerate(Resource input, IFileSystemAccess fsa) {
		try {
			doGenerateWithException(input, fsa);
		} catch (Exception e) {
			final String msg = "Problem when occured when " + this.getClass().toString() + " invoked doGenerateWithException() for: " + input.getURI();
			logger.error(msg, e);
			throw new RuntimeException(msg, e);
		}
		
	}
}