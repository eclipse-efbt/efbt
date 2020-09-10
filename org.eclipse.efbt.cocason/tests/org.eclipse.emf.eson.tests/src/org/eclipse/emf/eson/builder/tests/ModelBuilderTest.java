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

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.building.ModelBuilder;
import org.eclipse.emf.eson.eFactory.EFactoryFactory;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.tests.util.ESONWithTestmodelInjectorProvider;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.base.Optional;
import com.google.inject.Inject;

import testmodel.TestmodelPackage;

/**
 * Unit Test (!) for ModelBuilder.
 * This is NOT an integration test (the others in this package are).
 * 
 * @author Michael Vorburger
 */
@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelInjectorProvider.class)
public class ModelBuilderTest {

	@Inject ModelBuilder modelBuilder;
	
	@Test
	public void testBuild_getNewObject_Caching() throws Exception {
		Factory factory = EFactoryFactory.eINSTANCE.createFactory();
		NewObject newObject = EFactoryFactory.eINSTANCE.createNewObject();
		newObject.setEClass(TestmodelPackage.Literals.TEST_MODEL);
		factory.setRoot(newObject);
		
		Optional<EObject> result = modelBuilder.build(factory);
		Optional<EObject> result2 = modelBuilder.build(factory);
		Assert.assertEquals(result.get(), result2.get());
	}

}
