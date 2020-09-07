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
package org.eclipse.emf.eson.ui.contentassist;

import static org.junit.Assert.assertArrayEquals;

import javax.inject.Inject;

import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import testmodel.TestmodelPackage;

import org.eclipse.emf.eson.EFactoryUiInjectorProvider;
import org.eclipse.emf.eson.ui.contentassist.TerminalsEAttributeStringProposalProvider;

@RunWith(XtextRunner.class)
@InjectWith(EFactoryUiInjectorProvider.class)
public class TerminalsAttributeProposalProviderTest {

	@Inject TerminalsEAttributeStringProposalProvider attributeProposalProvider;
	
	@Test
	public void testStringProposal() {
		assertArrayEquals(new String[] { "\"" + attributeProposalProvider.getStringProposal() + "\"" },
				attributeProposalProvider.getProposals(TestmodelPackage.Literals.ATTRIBUTE_TEST_CONTAINER__ONE_STRING));
	}

	@Test
	public void testIntProposal() {
		assertArrayEquals(new String[] { attributeProposalProvider.getLongProposal().toString() },
				attributeProposalProvider.getProposals(TestmodelPackage.Literals.ATTRIBUTE_TEST_CONTAINER__ONE_INT));
	}

	@Test
	public void testIntegerProposal() {
		assertArrayEquals(new String[] { attributeProposalProvider.getLongProposal().toString() },
				attributeProposalProvider.getProposals(TestmodelPackage.Literals.ATTRIBUTE_TEST_CONTAINER__ONE_INTEGER));
	}

	@Test
	public void testShortProposal() {
		assertArrayEquals(new String[] { attributeProposalProvider.getLongProposal().toString() },
				attributeProposalProvider.getProposals(TestmodelPackage.Literals.ATTRIBUTE_TEST_CONTAINER__ONE_SHORT));
	}

	@Test
	public void testDoubleProposal() {
		assertArrayEquals(new String[] { attributeProposalProvider.getDoubleProposal().toString() },
				attributeProposalProvider.getProposals(TestmodelPackage.Literals.ATTRIBUTE_TEST_CONTAINER__ONE_DOUBLE));
	}

	@Test
	public void testDateProposal() {
		assertArrayEquals(new String[] { "09.07.2013" },
				attributeProposalProvider.getProposals(TestmodelPackage.Literals.ATTRIBUTE_TEST_CONTAINER__ONE_DATE));
	}

	@Test
	public void testBooleanProposal() {
		assertArrayEquals(new String[] { "true", "false" },
				attributeProposalProvider.getProposals(TestmodelPackage.Literals.ATTRIBUTE_TEST_CONTAINER__ONE_BOOL));
	}

	@Test
	public void testEnumProposal() {
		assertArrayEquals(new String[] { ":Sample", ":Sample2" },
				attributeProposalProvider.getProposals(TestmodelPackage.Literals.ATTRIBUTE_TEST_CONTAINER__ONE_ENUM));
	}

}
