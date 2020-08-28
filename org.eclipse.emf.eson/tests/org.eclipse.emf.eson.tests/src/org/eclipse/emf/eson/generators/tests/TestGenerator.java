/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2016 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.generators.tests;

import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.xtext.generator.IFileSystemAccess;
import org.eclipse.xtext.generator.IGenerator;

/**
 * Example IGenerator, for Tests.
 * 
 * @author Michael Vorburger
 */
public class TestGenerator implements IGenerator {

	// TODO Test @Inject - make sure it's language specific
	
    @Override
    public void doGenerate(Resource input, IFileSystemAccess fsa) {
        fsa.generateFile(input.getURI().lastSegment() + ".txt", "hello, world: " + input.getURI().toString());
    }

}