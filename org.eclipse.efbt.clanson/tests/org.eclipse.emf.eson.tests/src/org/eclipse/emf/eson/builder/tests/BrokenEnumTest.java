/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2014 Michael Vorburger
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

import org.eclipse.emf.common.util.EList;
import org.junit.Test;

import testmodel.AttributeTestContainer;
import testmodel.SampleEnum;

/**
 * Non-regression test for the real life problem where one bad Enum Literal
 * caused the entire *DerivedStateComputer to fall over with an obscure and
 * confusing low-level ClassCastException, instead of being ignored (OK, as
 * validation would pick it up anyway), and the rest of the resource to be
 * constructed nevertheless (AKA DS-7543).
 * 
 * @author Michael Vorburger
 */
public class BrokenEnumTest extends AbstractModelBuilderTest {
	
	private static final String TEST_NAME = null; // TODO "test"; -- once named NewObject is again possibly inside MultiValue
	
	@Override
	protected String getTestModelName() {
		return "BrokenEnumTest.eson";
	}

	public void testRootModelNotNull() throws Exception {
		assertNotNull(testModel);
	}

	// The following two tests are idiotic copy/pastes from AttributeTest
	// The point here is that we expect this to work, even though
	// BrokenEnumTest.eson has ONE invalid enum literal (the
	// :IAMBrokenBadBoy)
	
	@Test public void testAttribute_OneEnum() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		SampleEnum actual = fixture.getOneEnum();
		SampleEnum expected = SampleEnum.SAMPLE2;
		assertEquals(expected, actual);
	}

	@Test public void testAttribute_ManyEnums() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		EList<? extends Object> actualValues = fixture.getManyEnums();
		Object[] expectedValues = new Object[] { SampleEnum.SAMPLE, SampleEnum.SAMPLE2 };
		AttributeTest.performManyTest(expectedValues, actualValues);
	}

}
