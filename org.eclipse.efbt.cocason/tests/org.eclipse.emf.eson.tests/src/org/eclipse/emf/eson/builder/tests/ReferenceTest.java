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

import org.junit.Assert;
import org.junit.Test;

import testmodel.ReferenceTarget;
import testmodel.ReferenceTestContainer;
import testmodel.SingleRequired;
import testmodel.TestModel;

public class ReferenceTest extends AbstractModelBuilderTest {

	private static final String TEST2_NAME = "test2";
	private static final String TEST1_NAME = "test1";

	@Override
	protected String getTestModelName() {
		return "ReferenceTest.efactory";
	}

	@Test
	public void testReference_One() throws Exception {
		performToOneTest(TEST1_NAME, "target1");
	}

	@Test
	public void testReference_One_Subclass() throws Exception {
		performToOneTest(TEST2_NAME, "target23");
	}

	private void performToOneTest(String testName, String targetName) {
		ReferenceTestContainer fixture = getTest(testName);
		ReferenceTarget expected = find(ReferenceTarget.class, targetName);
		Assert.assertEquals(expected, fixture.getReferenceToOne());
	}

	@Test
	public void testReference_Many() throws Exception {
		String[] expectedTargetNames = new String[] { "target1", "target2",
				"target3" };
		performToManyTest(expectedTargetNames, TEST1_NAME);
	}

	@Test
	public void testReference_Many_Subclass() throws Exception {
		String[] expectedTargetNames = new String[] { "target21", "target22",
				"target23" };
		performToManyTest(expectedTargetNames, TEST2_NAME);
	}
	
	@Test
	public void testParentReference() throws Exception {
		TestModel root = (TestModel) testModel;
		SingleRequired req = root.getSingleRequired();
		Assert.assertEquals(root, req.getParentReference());
	}

	private void performToManyTest(String[] expectedTargetNames, String testName) {
		ReferenceTestContainer fixture = getTest(testName);
		for (String expectedTargetName : expectedTargetNames) {
			ReferenceTarget expected = find(ReferenceTarget.class,
					expectedTargetName);
			Assert.assertTrue(fixture.getReferenceToMany().contains(expected));
		}
	}

	private ReferenceTestContainer getTest(String testName) {
		ReferenceTestContainer fixture = find(ReferenceTestContainer.class,
				testName);
		return fixture;
	}

}
