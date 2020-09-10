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
public class FeatureProposalProviderTest extends AbstractEFactoryContentAssistProcessorTest {

	private static final String body = "use testmodel.* "
			+ "TestModel testModelName {\n"
			+ "referenceTestContainer: [ ReferenceTestContainer test1 {  } ] }";

	@Test
	public void testCompleteFeatures_All() throws Exception {
		newBuilder().append(body).cursorBack(6).assertText("containments", "referenceToOne",
				"referenceToMany", "name", "containment", "oneAttribute",
				"manyAttributes");
	}

	@Test
	public void testCompleteFeaturesWithSingleContainmentAlreadyUsed() throws Exception {
		newBuilder().append(body).cursorBack(6)
				.insert("containment: ReferenceTarget target1 {  }\n")
				.assertText("containments", "referenceToOne",
						"referenceToMany", "name",
						"oneAttribute", "manyAttributes");
						// NOTE: does not contain "containment"
	}

	@Test
	public void testCompleteFeaturesWithMultiValueContainmentAlreadyUsed() throws Exception {
		newBuilder().append(body).cursorBack(6)
				.insert("containments: [ ReferenceTarget target1 { } ]\n")
				.assertText("containment", "referenceToOne",
						"referenceToMany", "name", "oneAttribute",
						"manyAttributes");
						// NOTE: does not contain "containments"
	}

}
