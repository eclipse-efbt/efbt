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
package org.eclipse.emf.eson.builder.tests;

import javax.inject.Inject;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.resource.EFactoryResource;
import org.eclipse.emf.eson.tests.util.DumpParsedStructureUtil;
import org.eclipse.emf.eson.tests.util.ESONWithTestmodelInjectorProvider;
import org.eclipse.emf.eson.tests.util.Find;
import org.eclipse.emf.eson.tests.util.ResourceProvider;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;

import testmodel.TestModel;

@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelInjectorProvider.class)
public abstract class AbstractModelBuilderTest {

	private @Inject ResourceProvider resourceProvider;
	protected Factory factory;
	protected TestModel testModel;
	protected @Inject DumpParsedStructureUtil dumper;

	@Before
	public void setUp() throws Exception {
		this.testModel = resourceProvider.loadModel("res/BuilderTests/" + getTestModelName(), TestModel.class, /* HACK TODO UNDO */ false);
		this.factory = ((EFactoryResource) testModel.eResource()).getEFactoryFactory();
		// dumper.dump(this.factory);
	}

	protected abstract String getTestModelName();

	protected <T> T checkType(Class<T> clazz, EObject eObject) {
		Assert.assertTrue(clazz.isInstance(eObject));
		return clazz.cast(eObject);
	}

	protected void checkName(TestModel testModel, String testModelName) {
		Assert.assertEquals(testModelName, testModel.getName());
	}

	public <T extends EObject> T find(final Class<T> candidateClass,
			final String candidateName) {
		return Find.child(candidateClass, candidateName, testModel);
	}

}
