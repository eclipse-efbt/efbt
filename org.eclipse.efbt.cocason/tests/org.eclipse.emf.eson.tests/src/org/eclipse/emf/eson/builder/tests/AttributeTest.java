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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.eclipse.emf.common.util.EList;
import org.junit.Assert;
import org.junit.Test;

import testmodel.AttributeTestContainer;
import testmodel.SampleEnum;

public class AttributeTest extends AbstractModelBuilderTest {

	private static final String TEST_NAME = null; // TODO "test"; -- once named NewObject is again possibly inside MultiValue
	private SimpleDateFormat dateFormat = new SimpleDateFormat("MM.dd.yyyy");

	@Override
	protected String getTestModelName() {
		return "AttributeTest.efactory";
	}

	@Test public void testAttribute_OneBool() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		boolean actual = fixture.isOneBool();
		boolean expected = true;
		Assert.assertEquals(expected, actual);
	}

	@Test public void testAttribute_OneInt() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		int actual = fixture.getOneInt();
		int expected = 1;
		Assert.assertEquals(expected, actual);
	}

	@Test public void testAttribute_OneInteger() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		Integer actual = fixture.getOneInteger();
		Integer expected = 1;
		Assert.assertEquals(expected, actual);
	}
	
	@Test public void testAttribute_OneDouble() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		double actual = fixture.getOneDouble();
		double expected = 1.1;
		Assert.assertEquals(expected, actual, 0);
	}

	@Test public void testAttribute_OneEnum() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		SampleEnum actual = fixture.getOneEnum();
		SampleEnum expected = SampleEnum.SAMPLE2;
		Assert.assertEquals(expected, actual);
	}

	@Test public void testAttribute_OneDate() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		Date actual = fixture.getOneDate();
		Date expected = dateFormat.parse("09.11.1979");
		Assert.assertEquals(expected, actual);
	}

	@Test public void testAttribute_OneBigDecimal() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		BigDecimal actual = fixture.getOneBigDecimal();
		BigDecimal expected = new BigDecimal("1.0"); // TODO "123798234.223940293480923842");
		Assert.assertEquals(expected, actual);
	}
	
	@Test public void testAttribute_OneBigInteger() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		BigInteger actual = fixture.getOneBigInteger();
		BigInteger expected = new BigInteger("1"); // TODO "1230820834928349283498234");
		Assert.assertEquals(expected, actual);
	}
	
	@Test public void testAttribute_ManyBool() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		EList<? extends Object> actualValues = fixture.getManyBool();

		Object[] expectedValues = new Object[] { true, true };
		performManyTest(expectedValues, actualValues);
	}

	@Test public void testAttribute_ManyInt() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		EList<? extends Object> actualValues = fixture.getManyInt();

		Object[] expectedValues = new Object[] { 1, 2, 3 };
		performManyTest(expectedValues, actualValues);
	}

	@Test public void testAttribute_ManyDouble() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		EList<? extends Object> actualValues = fixture.getManyDouble();

		Object[] expectedValues = new Object[] { 1.1, 2.2, 3.3 };
		performManyTest(expectedValues, actualValues);
	}

	@Test public void testAttribute_ManyEnums() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		EList<? extends Object> actualValues = fixture.getManyEnums();

		Object[] expectedValues = new Object[] { SampleEnum.SAMPLE,
				SampleEnum.SAMPLE2 };
		performManyTest(expectedValues, actualValues);
	}

	@Test public void testAttribute_ManyDates() throws Exception {
		AttributeTestContainer fixture = find(AttributeTestContainer.class, TEST_NAME);
		EList<? extends Object> actualValues = fixture.getManyDates();

		Object[] expectedValues = new Object[] {
				dateFormat.parse("09.11.1979"), dateFormat.parse("08.10.0868") };
		performManyTest(expectedValues, actualValues);
	}

	static void performManyTest(Object[] expectedValues, EList<? extends Object> actualValues) {
		Assert.assertEquals(expectedValues.length, actualValues.size());
		for (int i = 0; i < expectedValues.length; i++) {
			Object expectedValue = expectedValues[i];
			Object actualValue = actualValues.get(i);
			Assert.assertEquals(expectedValue, actualValue);
		}
	}

}
