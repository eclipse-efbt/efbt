/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2013 - 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.formatting;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.services.EFactoryGrammarAccess;
import org.eclipse.xtext.formatting.impl.AbstractDeclarativeFormatter;
import org.eclipse.xtext.formatting.impl.FormattingConfig;
import org.eclipse.xtext.formatting.impl.FormattingConfig.LinewrapLocator;

/**
 * Xtext Formatter.
 * @author Michael Vorburger
 */
public class EFactoryFormatter extends AbstractDeclarativeFormatter {

	@Override
	protected void configureFormatting(FormattingConfig c) {
		EFactoryGrammarAccess f = (EFactoryGrammarAccess) getGrammarAccess();
		
		// NO c.setAutoLinewrap(...);
		
		// NOTE: Please use our setPreservingLinewrap() instead of just setLinewrap(...)
		
		setPreservingLinewraps(c, 2).after(f.getNamespaceImportRule());

		setPreservingLinewraps(c, 2).after(f.getPackageImportRule());

		setIndentationIncrementAndDecrementAndLinewrapAfter(c,
				f.getNewObjectAccess().getLeftCurlyBracketKeyword_2(),
				f.getNewObjectAccess().getRightCurlyBracketKeyword_4());
		
		c.setNoSpace().after(f.getFeatureAccess().getEFeatureAssignment_0());
		setPreservingLinewrap(c).after(f.getFeatureRule()); // or better? setPreservingLinewrap(c).before(f.getFeatureAccess().getEFeatureEStructuralFeatureCrossReference_0_0());
		
		setIndentationIncrementAndDecrementAndLinewrapAfter(c,
				f.getMultiValueAccess().getLeftSquareBracketKeyword_1(),
				f.getMultiValueAccess().getRightSquareBracketKeyword_3());
		setPreservingLinewrap(c).after(f.getMultiValueAccess().getValuesAssignment_2());
		
		setIndentationIncrementAndDecrementAndLinewrapAfter(c,
				f.getValueAccess().getLeftCurlyBracketKeyword_2_0_0_3(),
				f.getValueAccess().getRightCurlyBracketKeyword_2_2());
		
		c.setNoSpace().after(f.getEnumAttributeAccess().getColonKeyword_0());
		
		setPreservingLinewrap(c).before(f.getML_COMMENTRule());
	}

	protected void setIndentationIncrementAndDecrementAndLinewrapAfter(FormattingConfig c, EObject increment, EObject decrement) {
		c.setIndentationIncrement().after(increment);
		setPreservingLinewrap(c).after(increment);
		
		c.setIndentationDecrement().before(decrement);
		setPreservingLinewrap(c).after(decrement);
	}

	protected LinewrapLocator setPreservingLinewraps(FormattingConfig c, int lines) {
		return c.setLinewrap(lines, lines, Integer.MAX_VALUE);
	}
	protected LinewrapLocator setPreservingLinewrap(FormattingConfig c) {
		return setPreservingLinewraps(c, 1);
	}
	
}
