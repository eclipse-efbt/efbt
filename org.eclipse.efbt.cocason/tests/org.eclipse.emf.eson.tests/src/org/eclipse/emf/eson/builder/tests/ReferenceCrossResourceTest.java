/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2013 - 2015 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.builder.tests;

import javax.inject.Inject;

import org.eclipse.emf.eson.tests.util.DumpParsedStructureUtil;
import org.eclipse.emf.eson.tests.util.ESONWithTestmodelInjectorProvider;
import org.eclipse.emf.eson.tests.util.ResourceProvider;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import testmodel.TestModel;

@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelInjectorProvider.class)
public class ReferenceCrossResourceTest {

	@Inject DumpParsedStructureUtil dumper;
	@Inject ResourceProvider provider;

	@Test public void testCrossResourceReference() throws Exception {
		TestModel aModelUnvalidated = provider.loadModel("res/BuilderTests/ReferenceCrossResourceTestA.efactory", TestModel.class, false);
		dumper.dump(aModelUnvalidated);
		provider.validate(aModelUnvalidated.eResource());

		TestModel bModel = provider.loadModel("res/BuilderTests/ReferenceCrossResourceTestB.efactory", TestModel.class, false);
		dumper.dump(bModel);
		provider.validate(bModel.eResource());
		
		Assert.assertEquals("1-target,1", bModel.getReferenceTestContainer().get(0).getReferenceToOne().getName());
		Assert.assertEquals("4", bModel.getReferenceTestContainer().get(0).getReferenceToMany().get(3).getName());
		Assert.assertEquals("target31", bModel.getReferenceTestContainer().get(1).getReferenceToMany().get(3).getName());
	}
	
}
