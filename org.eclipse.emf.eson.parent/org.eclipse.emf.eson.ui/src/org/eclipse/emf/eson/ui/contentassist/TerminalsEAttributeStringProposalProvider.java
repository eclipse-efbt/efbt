/*
 * #%L
 * org.eclipse.emf.eson.ui
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

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.EcorePackage;
import org.eclipse.emf.eson.services.EFactoryGrammarAccess;
import org.eclipse.emf.eson.util.EcoreUtil3;
import org.eclipse.xtext.AbstractRule;
import org.eclipse.xtext.conversion.IValueConverterService;

/**
 * IEAttributeProposalProvider for common basic types.
 * 
 * @author Michael Vorburger
 */
public class TerminalsEAttributeStringProposalProvider implements IEAttributeStringProposalProvider {

	protected @Inject EFactoryGrammarAccess grammarAccess;
	protected @Inject IValueConverterService valueConverter;
	
	public String[] getProposals(EAttribute eAttribute) {
		// TODO better use org.eclipse.emf.eson.validation.EFactoryJavaValidator.AttributeValidator.equals(EClassifier, EDataType) here too? (and move it to EcoreUtils3)
		EDataType dataType = eAttribute.getEAttributeType();
		if (dataType == null)
			return new String[] { };
		
		if (EcorePackage.Literals.ESTRING.equals(dataType)) {
			return proposals(grammarAccess.getSTRINGRule(), getStringProposal());

		} else if (EcoreUtil3.isIntegerAttribute(dataType)) {
			return proposals(grammarAccess.getLongRule(), getLongProposal());
		} else if (EcoreUtil3.isDoubleAttribute(dataType)) {
			return proposals(grammarAccess.getDoubleRule(), getDoubleProposal());
		} else if (EcoreUtil3.isBooleanAttribute(dataType)) {
			return proposals(grammarAccess.getBooleanRule(), (Object[]) getBooleanProposals());
		} else if (EcorePackage.Literals.EDATE.equals(dataType) || EcoreUtil3.isDateAttribute(dataType)) {
			return proposals(grammarAccess.getDateRule(), getDateProposal());

		} else if (EcoreUtil3.isEnum(dataType)) {
			return getEnumProposals((EEnum) dataType);
		}
		return new String[] { };
	}

	protected String getStringProposal() {
		return "String";
	}

	protected Long getLongProposal() {
		return 1L;
	}

	protected Double getDoubleProposal() {
		return 1.2;
	}

	@SuppressWarnings("deprecation")
	protected Date getDateProposal() {
		return new Date(113, 8 /* 0-indexed!*/, 7); // == 2013-09-07 ;)
	}

	protected Boolean[] getBooleanProposals() {
		return new Boolean[] { true, false };
	}
	
	protected String[] getEnumProposals(EEnum eEnum) {
		final EList<EEnumLiteral> literals = eEnum.getELiterals();
		List<String> proposals = new ArrayList<String>(literals.size());
		for (EEnumLiteral literal : literals) {
			proposals.add(":" + literal.getName());
		}
		return proposals.toArray(new String[0]);
	}

	
	protected String[] proposals(AbstractRule grammarTerminalRule, Object... proposalValues) {
		List<String> proposals = new ArrayList<String>(proposalValues.length);
		for (Object proposalValue : proposalValues) {
			proposals.add(valueConverter.toString(proposalValue, grammarTerminalRule.getName()));
		}
		return proposals.toArray(new String[0]);
	}

}
