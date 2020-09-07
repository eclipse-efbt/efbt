/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2015 Michael Vorburger
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

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.eson.tests.util.ESONWithTestmodelAndDynamicECoreInjectorProvider;
import org.eclipse.emf.eson.tests.util.ResourceProvider;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelAndDynamicECoreInjectorProvider.class)
public class SimplestDynamicECoreTest {

	@Inject ResourceProvider provider;


	@Test public void testSimplestWithoutUseButFQN() throws Exception {
		check("res/BuilderTests/SimplestWithoutUseButFQN.eson");
	}

	@Ignore // OK, this doesn't/cannot/won't work - fine; this just for reference & reminder; will delete all this eventually, when 'use' support gets completely removed 
	@Test public void testSimplestWithUse() throws Exception {
		check("res/BuilderTests/Simplest.eson");
	}

	@Test public void testSimplestWithImport() throws Exception {
		check("res/BuilderTests/SimplestWithImport.eson");
	}

	@Test public void testSimplestWithEnum() throws Exception {
		EObject em = check("res/BuilderTests/SimplestWithEnum.eson");
		// TODO assert attributeTest.oneEnum = Sample2 .. (but the fact that above even passed without exception is already a good start)
	}

	
	protected EObject check(String esonPath) throws Exception {
		EList<EObject> contents = provider.load("model/TestModel.ecore", true);
		EPackage ePackage = (EPackage) contents.get(0);
		assertEquals("testmodel", ePackage.getName());
		// Do NOT DumpIndexUtil.dumpXtextIndex(ePackage.eResource()); as that does not work yet for *.ecore as this stage (later below on an *.eson it works - and dumps the *.ecore as well)

		@SuppressWarnings("unused") EList<EObject> content = provider.load(esonPath, false);
		// DEBUG: DumpIndexUtil.dumpXtextIndex(content.get(0).eResource());

		EObject em = provider.loadModel(esonPath, EObject.class);
		assertEquals("TestModel", em.eClass().getName());
// NOTE Even this cannot work, as it mixes the dynamic and static style.. see main problem, below.
//		assertEquals("abc", em.eGet(TestmodelPackage.Literals.TEST_MODEL__NAME));

// TODO Should this be made to work? Can it? How?? ;-)
//		TestModel m = provider.loadModel("res/BuilderTests/Simplest.eson", TestModel.class);
//		assertEquals("abc", m.getName());

		return em;
	}
}
