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

public class MultiplicityProposalProviderTest extends AbstractEFactoryContentAssistProcessorTest {

	private static final String body = "use testmodel.* "
			+ "TestModel testModelName {\n"
			+ "referenceTestContainer: [ ReferenceTestContainer test1 {\n";

	@Test
	public void testMultiplicityAttribute_One() throws Exception {
		newBuilder().append(body + "oneAttribute ").assertText(":");
	}

	@Test
	public void testMultiplicityAttribute_Many() throws Exception {
		newBuilder().append(body + "manyAttributes ").assertText(":");
	}

	@Test
	public void testMultiplicityContainment_One() throws Exception {
		newBuilder().append(body + "containment ").assertText(":");
	}

	@Test
	public void testMultiplicityContainment_Many() throws Exception {
		newBuilder().append(body + "containments ").assertText(":");
	}

	@Test
	public void testMultiplicityReference_One() throws Exception {
		newBuilder().append(body + "referenceToOne ").assertText(":");
	}

	@Test
	public void testMultiplicityReference_Many() throws Exception {
		newBuilder().append(body + "referenceToMany ").assertText(":");
	}

}
