/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2013 - 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.builder.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.inject.Inject;

import org.eclipse.emf.eson.tests.util.ESONWithTestmodelInjectorProvider;
import org.eclipse.emf.eson.tests.util.ResourceProvider;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import testmodel.TestModel;
import testmodel.testsubmodel.TestInnerModel;
import testmodel.testsubmodelWithTraditionalURI.TestInnerModelInPackageWithTraditionalURI;

@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelInjectorProvider.class)
public class SimplestTest {

	@Inject ResourceProvider provider;

	@Test public void testSimplestWithNamespaceURI() throws Exception {
		TestInnerModelInPackageWithTraditionalURI m = provider.loadModel("res/BuilderTests/SimplestWithNamespaceURI.eson", TestInnerModelInPackageWithTraditionalURI.class);
		assertEquals("def", m.getName());
	}

	@Test public void testSimplest() throws Exception {
		TestModel m = provider.loadModel("res/BuilderTests/Simplest.eson", TestModel.class);
		assertEquals("abc", m.getName());
		assertNotNull(m.getSingleRequired());
	}

	@Test public void testSimplestSubPackage() throws Exception {
		TestInnerModel m = provider.loadModel("res/BuilderTests/SimplestSubPackage.eson", TestInnerModel.class);
		assertNotNull(m);
	}

	@Test public void testSimplestNamedContainment() throws Exception {
		TestModel m = provider.loadModel("res/BuilderTests/SimplestNamedContainment.eson", TestModel.class);
		assertEquals("abc", m.getName());
		assertNotNull(m.getSingleRequired());
		assertEquals("d", m.getSingleRequired().getName());
	}

	@Test public void testSimplestTypelessContainment() throws Exception {
		TestModel m = provider.loadModel("res/BuilderTests/SimplestTypelessContainment.eson", TestModel.class);
		assertEquals("abc", m.getName());
		assertNotNull(m.getSingleRequired());
	}
	
	@Ignore // Named Typeless Containment currently is not needed (yet?), thus NOT supported - not sure if it ever will be.
	// The conceptual problem is that given e.g. "singleRequired: d { }" it's not clear if d is the EClass or the name... hm. 
	// @see also SerializationTest's testNotTypelessBecauseNamedContainment
	@Test public void testSimplestNamedTypelessContainment() throws Exception {
		TestModel m = provider.loadModel("res/BuilderTests/SimplestTypelessNamedContainment.eson", TestModel.class);
		assertEquals("abc", m.getName());
		assertNotNull(m.getSingleRequired());
		assertEquals("d", m.getSingleRequired().getName());
	}

}
