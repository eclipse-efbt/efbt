/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz, Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.highlighting;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.xtext.ui.editor.syntaxcoloring.DefaultHighlightingConfiguration;
import org.eclipse.xtext.ui.editor.syntaxcoloring.IHighlightingConfiguration;
import org.eclipse.xtext.ui.editor.syntaxcoloring.IHighlightingConfigurationAcceptor;
import org.eclipse.xtext.ui.editor.utils.TextStyle;

public class EFactorySemanticHighlightingConfiguration implements IHighlightingConfiguration {

	public static final String DEFAULT_ID = DefaultHighlightingConfiguration.DEFAULT_ID;
	public static final String COMMENT_ID = DefaultHighlightingConfiguration.COMMENT_ID;
	public static final String KEYWORD_ID = DefaultHighlightingConfiguration.KEYWORD_ID;
	public static final String INVALID_TOKEN_ID = DefaultHighlightingConfiguration.INVALID_TOKEN_ID;
	
	public final static String ANNOTATION_ID = "annotation";
	public final static String ECLASS_ID = "eClass";
	public final static String EFEATURE_ID = "eFeature";
	public final static String NAME_ID = "eObjectName";
	public final static String CROSSREF_ID = "eReference";
	public final static String KEYCHAR_ID = "keycharacter";
	
	public void configure(IHighlightingConfigurationAcceptor acceptor) {
		acceptor.acceptDefaultHighlighting(DEFAULT_ID, "Default", defaultTextStyle());
		acceptor.acceptDefaultHighlighting(KEYWORD_ID, "Keyword (use, import)", keywordTextStyle());
		acceptor.acceptDefaultHighlighting(COMMENT_ID, "Comment", commentTextStyle());
		acceptor.acceptDefaultHighlighting(INVALID_TOKEN_ID, "Invalid Symbol", errorTextStyle());

		acceptor.acceptDefaultHighlighting(ANNOTATION_ID, "Annotation", annotationTextStyle());
		acceptor.acceptDefaultHighlighting(ECLASS_ID, "EClass", eClassTextStyle());
		acceptor.acceptDefaultHighlighting(EFEATURE_ID, "EFeature", eFeatureTextStyle());

		acceptor.acceptDefaultHighlighting(NAME_ID, "EObject Name", nameTextStyle());
		acceptor.acceptDefaultHighlighting(CROSSREF_ID, "EReference (Cross Ref)", crossReferenceTextStyle());
		
		acceptor.acceptDefaultHighlighting(KEYCHAR_ID, "Symbol ({}[])", keyCharTextStyle());
	}

	protected TextStyle newTextStyle() {
		TextStyle textStyle = new TextStyle();
		// textStyle.setBackgroundColor(new RGB(255, 255, 255)); ?
		return textStyle;
	}

	protected TextStyle defaultTextStyle() {
		TextStyle textStyle = newTextStyle();
		textStyle.setColor(new RGB(0, 0, 0));
		return textStyle;
	}
	
	protected TextStyle commentTextStyle() {
		TextStyle textStyle = newTextStyle();
		textStyle.setColor(new RGB(63, 127, 95));
		return textStyle;
	}
	
	public TextStyle keywordTextStyle() {
		TextStyle textStyle = newTextStyle();
		textStyle.setColor(new RGB(127, 0, 85));
		textStyle.setStyle(SWT.BOLD);
		return textStyle;
	}
	
	protected TextStyle errorTextStyle() {
		TextStyle textStyle = newTextStyle();
		textStyle.setColor(new RGB(255, 0, 0));
		return textStyle;
	}

	protected TextStyle annotationTextStyle() {
		TextStyle textStyle = newTextStyle();
		textStyle.setColor(new RGB(100, 100, 100));
		return textStyle;
	}

	protected TextStyle eClassTextStyle() {
		TextStyle textStyle = newTextStyle();
		textStyle.setColor(new RGB(0, 128, 128));
		return textStyle;
	}

	protected TextStyle eFeatureTextStyle() {
		TextStyle textStyle = newTextStyle();
		textStyle.setColor(new RGB(127, 0, 127));
		return textStyle;
	}

	protected TextStyle nameTextStyle() {
		TextStyle textStyle = newTextStyle();
		textStyle.setColor(new RGB(0, 0, 128));
		textStyle.setStyle(SWT.BOLD);
		return textStyle;
	}

	protected TextStyle crossReferenceTextStyle() {
		TextStyle textStyle = newTextStyle();
		textStyle.setColor(new RGB(42, 0, 255));
		textStyle.setStyle(SWT.ITALIC);
		return textStyle;
	}
	
	protected TextStyle keyCharTextStyle() {
		TextStyle textStyle = newTextStyle();
		textStyle.setColor(new RGB(191, 191, 191));
		return textStyle;
	}

}
