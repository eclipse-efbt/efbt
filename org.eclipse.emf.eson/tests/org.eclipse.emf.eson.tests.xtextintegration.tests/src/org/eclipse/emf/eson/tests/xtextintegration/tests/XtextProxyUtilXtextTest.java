/*
 * #%L
 * org.eclipse.emf.eson.tests.xtextintegration.tests
 * %%
 * Copyright (C) 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.tests.xtextintegration.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.inject.Inject;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.eson.util.XtextProxyUtil;
import org.eclipse.emf.eson.xtextintegration.myDsl.Model;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.eclipse.xtext.junit4.util.ParseHelper;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests XtextProxyUtil for our pure standard Xtext test myDsl language.
 * This test actually isn't directly related to ESON.
 * 
 * @see com.googlecode.efactory.util.tests.XtextProxyUtilESONTest
 * 
 * @author Michael Vorburger
 */
@RunWith(XtextRunner.class)
@InjectWith(MyDslWithEFactoryInjectorProvider.class)
public class XtextProxyUtilXtextTest {

	@Inject
	XtextProxyUtil proxyUtil;
	
	@Inject
	ParseHelper<Model> ph;

	@Test
	public void testGetSingleProxyCrossRefAsString() throws Exception {
		Model model = ph.parse("Greetings greeting1 REF ItsNotLinkedYet");
		// DO NOT validate, as it's -intentionally- broken: new ResourceProvider(null).validate(model); // use IResourceValidator instead ResourceProvider.validate()
		Model refModel = model.getRefModel();
		assertNotNull(refModel);
		String refText = proxyUtil.getProxyCrossRefAsString(model, refModel);
		assertEquals("ItsNotLinkedYet", refText);
	}
	
	@Test
	public void testGetMultiProxyCrossRefAsString() throws Exception {
		Model model = ph.parse("Greetings greeting1 REFs greeting1 ItsNotLinkedYet");
		// DO NOT validate, as it's -intentionally- broken: new ResourceProvider(null).validate(model); // use IResourceValidator instead ResourceProvider.validate()
		EList<Model> refModels = model.getRefModels();
		assertNotNull(refModels);
		assertEquals(2, refModels.size());
		assertTrue(refModels.get(0) == model);
		String refText = proxyUtil.getProxyCrossRefAsString(model, refModels.get(1));
		assertEquals("ItsNotLinkedYet", refText);
	}

}
