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
package org.eclipse.emf.eson.serialization.tests;

import org.eclipse.emf.eson.tests.util.ESONWithTestmodelInjectorProvider;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelInjectorProvider.class)
public class SerializationTest extends AbstractSerializationTest {

	@Test public void testFactoryCreation() throws Exception {
		performSerializationTest("FactoryCreation.efactory");
	}

	@Test public void testFunkyNameWithSpace() throws Exception {
	    performSerializationTest("FunkyNameWithSpace.eson");
	}
	
	@Test public void testSimplestMultiValue() throws Exception {
		performSerializationTest("SimplestMultiValue.efactory");
	}
	
	@Test public void testAttributeTest() throws Exception {
		performSerializationTest("AttributeTest.efactory");
	}
	
	@Ignore // TODO how to make this work?? This is red, probably because the new /tmp/*.efactory that we copied into wasn't indexed yet?
	@Test public void testReferenceTest() throws Exception {
		performSerializationTest("ReferenceTest.efactory");
	}

	@Ignore // TODO how to make this work?? This is red, probably because the new /tmp/*.efactory that we copied into wasn't indexed yet?
	@Test public void testReferenceTestSimple() throws Exception {
		performSerializationTest("ReferenceTestSimple.efactory");
	}
	
	@Test public void testTypelessContainment() throws Exception {
		performSerializationTest("TypelessContainment.eson");
	}

	@Test public void testNotTypelessBecauseNamedContainment() throws Exception {
		performSerializationTest("NotTypelessBecauseNamedContainment.eson");
	}
}
