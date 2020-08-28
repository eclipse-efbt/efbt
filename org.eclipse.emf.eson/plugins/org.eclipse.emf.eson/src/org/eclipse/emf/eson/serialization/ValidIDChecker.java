/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2015 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.serialization;

import java.io.StringReader;

import org.eclipse.emf.eson.services.EFactoryGrammarAccess;
import org.eclipse.xtext.ParserRule;
import org.eclipse.xtext.parser.IParseResult;
import org.eclipse.xtext.parser.IParser;
import org.eclipse.xtext.xbase.lib.IterableExtensions;

import com.google.common.base.Strings;
import com.google.inject.Inject;

/**
 * Utility.
 * 
 * @see https://bugs.eclipse.org/bugs/show_bug.cgi?id=469808
 * 
 * @author Dietmar Stoll
 */
public class ValidIDChecker {

	private @Inject IParser parser;
	private @Inject EFactoryGrammarAccess grammar;

	/**
	 * Check if a String is a ValidID.
	 * 
	 * @param candidate the string to parse which may be a ValidID
	 * @return true if the parser successfully parsed it as a ValidID
	 */
	public boolean isValidID(String candidate) {
		return isValid(grammar.getValidIDRule(), candidate);
	}

	public boolean isValidQualifiedName(String candidate) {
		return isValid(grammar.getQualifiedNameRule(), candidate);		
	}
	
	protected boolean isValid(ParserRule rule, String candidate) {
		if (Strings.isNullOrEmpty(candidate)) {
			// This is not strictly speaking true from the Grammar's point of
			// view of the ValidID regexp definition, but still required / makes
			// sense in this context; see tests.
			return true;
		}
		IParseResult parseResult = parser.parse(rule, new StringReader(candidate));
		return IterableExtensions.isNullOrEmpty(parseResult.getSyntaxErrors());		
	}
}
