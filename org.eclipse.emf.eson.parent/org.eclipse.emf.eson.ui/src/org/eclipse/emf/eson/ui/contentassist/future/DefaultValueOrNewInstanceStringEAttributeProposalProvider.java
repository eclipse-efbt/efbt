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
package org.eclipse.emf.eson.ui.contentassist.future;

import javax.inject.Inject;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EDataType;

import org.eclipse.emf.eson.ui.contentassist.IEAttributeStringProposalProvider;

/**
 * An IEAttributeProposalProvider which used EMF Defaults and Java Reflection
 * New Instance to obtain a Proposal for an EDataType.
 * 
 * @author Michael Vorburger
 */
public class DefaultValueOrNewInstanceStringEAttributeProposalProvider implements IEAttributeStringProposalProvider {

	// This isn't actually currently used in EFactory, but here as part of the (possible, to confirm) basis for an idea I had for later - to support dynamic data types.

	protected @Inject IValueObjectToString valueObjectToString;
	
	public String[] getProposals(EAttribute eAttribute) {
		final EDataType eDataType = eAttribute.getEAttributeType();
/*		
		String proposal = eAttribute.getDefaultValueLiteral();
		if (proposal == null)
			proposal = getDefault(eAttribute);
		if (proposal == null)
			proposal = getNewInstanceAsString(eAttribute);
*/
		Object proposal = eAttribute.getDefaultValue();
		if (proposal == null)
			proposal = eDataType.getDefaultValue();
		if (proposal == null)
			proposal = getNewInstance(eDataType);

		String proposalText = toString(proposal, eDataType);

		return new String[] { proposalText };
	}

	/**
	 * Convert EAttribute proposal Object value to String.
	 * 
	 * @param value value (as Object, not String)
	 * @param eDataType data type
	 * 
	 * @return value as String (not Object)
	 */
	protected String toString(Object value, EDataType eDataType) {
		// cannot use an IValueConverterService here, because the EDataType may be dynamic and not have a lexerRule
		return valueObjectToString.toString(value, eDataType);
	}

	protected Object getNewInstance(final EDataType eAttributeType) {
		try {
			return eAttributeType.getInstanceClass().newInstance();
		} catch (Exception e) {
			return null;
		}
	}

}
