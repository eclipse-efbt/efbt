/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2016 Michael Vorburger and others. 
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.conversion;

import org.eclipse.emf.eson.serialization.ValidIDChecker;
import org.eclipse.emf.eson.services.EFactoryGrammarAccess;
import org.eclipse.xtext.conversion.ValueConverterException;
import org.eclipse.xtext.conversion.impl.AbstractNullSafeConverter;
import org.eclipse.xtext.nodemodel.INode;

import com.google.inject.Inject;

/**
 * Value Converter which optionally escapes a string, if needed.
 *  
 * @author Michael Vorburger
 */
public class StringOrQualifiedNameWithWildcardValueConverter extends AbstractNullSafeConverter<String> {

	protected @Inject ValidIDChecker validChecker;
	protected @Inject StringRuleValueConverter delegateStringValueConverter;
	protected @Inject EFactoryGrammarAccess grammarAccess;

	@Override
	public String internalToValue(String string, INode node) throws ValueConverterException {
		if (string.startsWith("\"") || string.startsWith("\'")) {
			return delegateStringValueConverter.toValue(string, node);
		} else if (string.endsWith(".*")) {
			return string.substring(0, string.length() - 2);
		} else {
			return string;
		}
	}

	@Override
	public String internalToString(String value) throws ValueConverterException {
		String valueWithoutWildcard = value.endsWith(".*") ? value.substring(0, value.length() - 2): value;
		if (!validChecker.isValidQualifiedName(valueWithoutWildcard)) {
			return delegateStringValueConverter.toString(value);
		} else
			return value;
	}

	// NOTE: We cannot use STRINGValueConverter, because that's an AbstractLexerBasedConverter (i.e. only applicable for a terminal STRING, not for our Rule)
	protected static class StringRuleValueConverter extends AbstractNullSafeConverter<String> {

		@Override
		protected String internalToValue(String string, INode node) throws ValueConverterException {
			return string.substring(1, string.length() - 1);
		}
	
		@Override
		protected String internalToString(String value) {
			// could do same as-in STRINGValueConverter using Strings.convertToJavaString, but should not be needed just for EPackage nsURI here, so simpler: 
			return '"' + value + '"';
		}
		
	}
}
