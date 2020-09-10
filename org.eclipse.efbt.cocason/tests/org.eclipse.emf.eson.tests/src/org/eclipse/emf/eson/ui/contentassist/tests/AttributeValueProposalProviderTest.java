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
package org.eclipse.emf.eson.ui.contentassist.tests;

import org.junit.Test;

/**
 * @author Sebastian Benz - initial API and implementation
 * @author Michael Vorburger - completely valid text, and use of cursorBack()
 */
public class AttributeValueProposalProviderTest extends AbstractEFactoryContentAssistProcessorTest {

	private static final String body = "use testmodel.* "
			+ "TestModel testModelName {\n"
			+ "attributeTest: [ AttributeTestContainer test {  } ] }";
	
	@Test
	public void testCompleteBooleanAttribute_Value() throws Exception {
		newBuilder().append(body).cursorBack(6).insert("oneBool: ").assertText("true", "false");
	}

	@Test
	public void testCompleteStringAttribute_Value() throws Exception {
		newBuilder().append(body).cursorBack(6).insert("oneString: ").assertText("\"String\"");
	}

	@Test
	public void testCompleteIntegerAttribute_Attribute() throws Exception {
		newBuilder().append(body).cursorBack(6).insert("oneInt: ").assertText("1");
	}

	@Test
	public void testCompleteDateAttribute_Attribute() throws Exception {
		newBuilder().append(body).cursorBack(6).insert("oneDate: ").assertText("09.07.2013");
	}

	@Test
	public void testCompleteBigIntegerAttribute_Attribute() throws Exception {
		newBuilder().append(body).cursorBack(6).insert("oneBigInteger: ").assertText("1");
	}

	@Test
	public void testCompleteShortAttribute_Attribute() throws Exception {
		newBuilder().append(body).cursorBack(6).insert("oneShort: ").assertText("1");
	}

	@Test
	public void testCompleteBigDecimalAttribute_Attribute() throws Exception {
		newBuilder().append(body).cursorBack(6).insert("oneBigDecimal: ").assertText("1.2");
	}

	@Test
	public void testCompleteEnumAttribute_Attribute() throws Exception {
		newBuilder().append(body).cursorBack(6).insert("oneEnum: ").assertText(":Sample", ":Sample2");
	}

	@Test
	public void testCompleteDoubleAttribute_Attribute() throws Exception {
		newBuilder().append(body).cursorBack(6).insert("oneDouble: ").assertText("1.2");
	}
	
}
