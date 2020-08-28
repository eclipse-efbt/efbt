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

import javax.inject.Inject;

import org.eclipse.emf.eson.tests.util.ESONWithTestmodelInjectorProvider;
import org.eclipse.emf.eson.tests.util.ResourceProvider;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import testmodel.TestModel;

@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelInjectorProvider.class)
public class NameOneDigitOnlyTest {

	@Inject ResourceProvider provider;

	@Test public void testNameOneDigitOnly() throws Exception {
		TestModel m = provider.loadModel("res/BuilderTests/NameOneDigitOnly.eson", TestModel.class);
		assertEquals("1", m.getName());
	}

	@Test public void testNameOneDigitPlusChars() throws Exception {
		TestModel m = provider.loadModel("res/BuilderTests/NameOneDigitPlusChars.eson", TestModel.class);
		assertEquals("1a-b,c", m.getName());
	}
}
