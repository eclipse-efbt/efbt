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
package org.eclipse.emf.eson.formatting.tests;

import javax.inject.Inject;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.tests.util.ESONWithTestmodelInjectorProvider;
import org.eclipse.emf.eson.tests.util.LineEndingUtil;
import org.eclipse.emf.eson.tests.util.ResourceProvider;
import org.eclipse.xtext.formatting.INodeModelFormatter;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.eclipse.xtext.nodemodel.ICompositeNode;
import org.eclipse.xtext.parser.IParseResult;
import org.eclipse.xtext.resource.XtextResource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests EFactoryFormatter.
 * 
 * @author Michael Vorburger
 */
@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelInjectorProvider.class)
public class FormatterTest {

	// using our ResourceProvider instead of a ParseHelper<org.eclipse.emf.eson.eFactory.Factory>
	// just because it has some convenience methods we like - if you're looking
	// at this code to write your own Xtext Formatter unit test, just use a
	// ParseHelper.
	@Inject ResourceProvider resourceProvider;

	@Inject INodeModelFormatter formatter;

	@Test
	public void testFormatting() throws Exception {
		URI uri = resourceProvider.getUri("res/FormatterTests/FormatterTest.efactory");
		String text = resourceProvider.loadAsStringFromURI(uri);
		EObject eo = resourceProvider.load(uri, true).get(0);
        IParseResult parseResult = ((XtextResource) eo.eResource()).getParseResult();
        Assert.assertNotNull(parseResult);
		ICompositeNode rootNode = parseResult.getRootNode();
		String formattedText = formatter.format(rootNode, 0, text.length()).getFormattedText();
		text = LineEndingUtil.fixLineEndings(text);
		formattedText = LineEndingUtil.fixLineEndings(formattedText);
		Assert.assertEquals(text, formattedText);
	}
}
