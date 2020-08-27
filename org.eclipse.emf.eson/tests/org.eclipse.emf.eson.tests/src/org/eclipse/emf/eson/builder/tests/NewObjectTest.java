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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.eFactory.Attribute;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.MultiValue;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.util.ValueResolver;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class NewObjectTest extends AbstractModelBuilderTest {

	private static final String TEST_MODEL_NAME = "test";

	@Test public void testBuild_NewObject() throws Exception {
		assertTrue(testModel.getSingleRequired().getParentReference() == testModel);
		
		checkName(testModel, TEST_MODEL_NAME);
		
		NewObject newObject = factory.getRoot();
		checkFeatures(newObject, testModel);
		checkAttributeValues(newObject, testModel);
	}

	private void checkAttributeValues(NewObject newObject, EObject eObject) {
		for (Feature feature : Iterables.filter(newObject.getFeatures(),
				new Predicate<Feature>() {
					public boolean apply(Feature input) {
						return (input.getValue() instanceof Attribute);
					}
				})) {
			checkAttributeValue(feature, eObject);
		}
	}

	private void checkAttributeValue(Feature feature, EObject eObject) {
		Object expectedValue = new ValueResolver().apply(feature.getValue());
		Object actualValue = eObject.eGet(feature.getEFeature());
		assertEquals(expectedValue, actualValue);
	}

	private void checkFeatures(NewObject newObject, EObject eObject) {
		for (Feature feature : newObject.getFeatures()) {
			assertNotNull(eObject.eGet(feature.getEFeature()));
			if (feature.getEFeature().isMany()) {
				MultiValue multiValue = (MultiValue) feature.getValue();
				EList<?> eList = (EList<?>) eObject.eGet(feature.getEFeature());
				assertEquals(multiValue.getValues().size(), eList.size());
			}
		}
	}

	@Override
	protected String getTestModelName() {
		return "BuilderTest.efactory";
	}

}
