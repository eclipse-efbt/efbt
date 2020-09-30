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

import org.eclipse.emf.ecore.EDataType;
import org.eclipse.xtext.conversion.IValueConverterService;

/**
 * IValueObjectToString implementation uses the org.eclipse.xtext.conversion stuff.
 * 
 * @author Michael Vorburger
 */
public class ValueObjectToStringViaValueConverterService implements IValueObjectToString {

	// This isn't actually currently used in EFactory, but here as part of the (possible, to confirm) basis for an idea I had for later - to support dynamic data types.

	private IValueConverterService valueConverter;
	
	public String toString(Object value, EDataType eDataType) {
		final String lexerRuleName = getLexerRuleName(eDataType);
		return valueConversion(value, lexerRuleName);
	}

	protected String getLexerRuleName(EDataType eAttributeType) {
		// TODO This isn't possible... :-( Might remove this class all together later, @see TerminalsAttributeProposalProvider
		throw new UnsupportedOperationException();
	}
	
	protected String valueConversion(Object object, String lexerRuleName) {
		return getValueConverter().toString(object, lexerRuleName);
	}

	protected IValueConverterService getValueConverter() {
		return valueConverter;
	}

}
