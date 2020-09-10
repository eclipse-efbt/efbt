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
package org.eclipse.emf.eson.ui.highlighting;

import java.util.Set;

import org.eclipse.xtext.ui.editor.syntaxcoloring.DefaultAntlrTokenToAttributeIdMapper;

import com.google.common.collect.ImmutableSet;

/**
 * AbstractAntlrTokenToAttributeIdMapper which highlights
 * some key character tokens with a special color.
 * 
 * @author Michael Vorburger
 */
public class EFactoryTokenTypeToStringMapper extends DefaultAntlrTokenToAttributeIdMapper {

	protected final Set<String> KEYCHARS;

	public EFactoryTokenTypeToStringMapper() {
		super();
		KEYCHARS = ImmutableSet.of("'['", "']'", "'{'", "'}'", "':'", "'='");
	}

	@Override
	protected String calculateId(String tokenName, int tokenType) {
		if (KEYCHARS.contains(tokenName))
			return EFactorySemanticHighlightingConfiguration.KEYCHAR_ID;
		else
			return super.calculateId(tokenName, tokenType);
	}
}
