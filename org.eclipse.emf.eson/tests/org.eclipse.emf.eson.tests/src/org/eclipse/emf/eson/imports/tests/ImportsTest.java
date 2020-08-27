/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz, Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.imports.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.eclipse.emf.eson.tests.util.ESONWithTestmodelInjectorProvider;
import org.eclipse.emf.eson.tests.util.ResourceProvider;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import testmodel.TestModel;

import com.google.inject.Inject;

/**
 * Tests import of a XMI from an EFactory.
 */
@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelInjectorProvider.class)
public class ImportsTest {

	private @Inject ResourceProvider resourceProvider;
	private TestModel testModel;

	@Before
	public void setUp() throws Exception {
		resourceProvider.load("res/ImportTests/Imported.xmi", false); // This is an XMI file!
		this.testModel = (TestModel) resourceProvider.loadModel("res/ImportTests/Importing.efactory");
	}

	@Test
	@Ignore // TODO re-enable later; this broke as part of https://github.com/vorburger/efactory/pull/30 
	public void testImport_FromEfactory() throws Exception {
		TestModel parentReference = testModel.getSingleRequired().getParentReference();
		assertNotNull(parentReference);
		assertFalse(parentReference.eIsProxy());
		Assert.assertNotNull(parentReference.eResource());
	}

}
