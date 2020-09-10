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
package org.eclipse.emf.eson.tests.util.tests;

import static org.junit.Assert.assertNotNull;

import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.eson.tests.util.ESONWithTestmodelInjectorProvider;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test illustrating problem with lost *Package.eINSTANCE in @Before.
 * 
 * @author Michael Vorburger
 */
@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelInjectorProvider.class) // Not just EFactoryInjectorProvider
public class ESONWithTestmodelInjectorProviderTest {
	
	@Test
	public void testFirst() throws Exception {
		assertNotNull(EPackage.Registry.INSTANCE.getEPackage("testmodel"));
	}

	@Test
	public void testSecond() throws Exception {
		assertNotNull(EPackage.Registry.INSTANCE.getEPackage("testmodel"));
	}
}
