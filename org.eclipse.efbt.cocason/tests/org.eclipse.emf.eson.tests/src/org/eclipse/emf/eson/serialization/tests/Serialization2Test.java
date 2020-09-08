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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import javax.inject.Inject;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.resource.EFactoryResource;
import org.eclipse.emf.eson.tests.util.ESONWithTestmodelInjectorProvider;
import org.eclipse.emf.eson.tests.util.ResourceProvider;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.eclipse.xtext.junit4.util.ParseHelper;
import org.junit.Test;
import org.junit.runner.RunWith;

import testmodel.TestModel;
import testmodel.TestmodelFactory;

@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelInjectorProvider.class)
public class Serialization2Test {

	@Inject protected ResourceProvider resourceProvider;
	@Inject protected ParseHelper<EObject> parseHelper;
	
	@Test public void testReferenceTestSimpleAlternative() throws Exception {
		TestModel testModel = resourceProvider.loadModel("res/SerializationTests/ReferenceTestSimple.efactory", TestModel.class);
		String oldSerializedText = SerializationUtils.toString(testModel.eResource());

		testModel.getReferenceTestContainer().get(0).getContainments().get(0).setName("target2");
		String newSerializedText = SerializationUtils.toString(testModel.eResource());

		assertEquals(oldSerializedText.replace("target1", "target2"), newSerializedText);
	}
	
	@Test 
	public void testChangeGoodNameToFunkyNameWithSpace() throws Exception {
        TestModel testModel = resourceProvider.loadModel("res/SerializationTests/AttributeTest.efactory", TestModel.class);
        assertEquals("testModelName", testModel.getName());
        testModel.setName("test#Model Name");
        String text = SerializationUtils.toString(testModel.eResource());
        Resource newTestModelResource = parseHelper.parse(text).eResource();
        resourceProvider.validate(newTestModelResource);
        TestModel newTestModel = EFactoryResource.getEFactoryEObject(newTestModelResource, TestModel.class);
        assertNotNull(newTestModel);
        assertEquals("test#Model Name", newTestModel.getName());
        assertFalse(text, text.contains("testModelName"));
    }
	
    @Test 
    public void testChangeNameThenSecondAttribute() throws Exception {
        TestModel testModel = resourceProvider.loadModel("res/SerializationTests/AttributeTest.efactory", TestModel.class);
        //assertEquals("testModelName", testModel.getName());
        testModel.setName("name");
        testModel.setSingleRequired(TestmodelFactory.eINSTANCE.createSingleRequired());
        String text = SerializationUtils.toString(testModel.eResource());
        
        Resource newTestModelResource = parseHelper.parse(text).eResource();
        resourceProvider.validate(newTestModelResource);
        TestModel newTestModel = EFactoryResource.getEFactoryEObject(newTestModelResource, TestModel.class);
        assertNotNull(newTestModel);
        assertEquals("name", newTestModel.getName());
    }	
}
