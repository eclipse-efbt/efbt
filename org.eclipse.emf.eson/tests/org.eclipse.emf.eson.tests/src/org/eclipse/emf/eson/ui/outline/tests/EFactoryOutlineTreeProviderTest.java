/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2013 - 2014 Michael Vorburger and others.
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.outline.tests;

import java.io.IOException;

import javax.inject.Inject;

import org.eclipse.emf.eson.EFactoryUiInjectorProvider;
import org.eclipse.emf.eson.ui.outline.EFactoryOutlineTreeProvider;
import org.eclipse.xtext.junit4.AbstractXtextTests;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.eclipse.xtext.resource.XtextResource;
import org.eclipse.xtext.ui.editor.model.XtextDocument;
import org.eclipse.xtext.ui.editor.outline.IOutlineNode;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.inject.Injector;

/**
 * Test for the EFactoryOutlineTreeProvider.
 * 
 * @author Michael Vorburger
 */
@RunWith(XtextRunner.class)
@InjectWith(EFactoryUiInjectorProvider.class)
public class EFactoryOutlineTreeProviderTest extends AbstractXtextTests {
	// Implementation inspired by org.eclipse.xtext.xtext.ui.editor.outline.XtextOutlineTreeProviderTest
	// TODO suggest an Xtext patch refactoring common re-usable code into an e.g. AbstractXtextOutlineTests, or an OutlineTestsFixture, and document it in Xtext UG doc 

	@Inject Injector injector;
	@Inject XtextDocument document;
	@Inject EFactoryOutlineTreeProvider treeProvider;
	
	@Override
	public void setUp() throws Exception {
		super.setUp();
		setInjector(injector);
		disableSerializerTest();
	}

	@Test
	public void testOutline() throws Exception {
		String text = readFileIntoString("/FormatterTests/FormatterTest.efactory");
		IOutlineNode root = getOutlineRootNode(text);
		assertNodeText(root, "TestModel testModelName", 4);
		assertNodeTextRegion(root, text, "testModelName", 1438, 13);
		assertNodeText(root.getChildren().get(0), "SingleRequired ", 0);
		// TODO more
	}

	protected void assertNodeText(IOutlineNode node, String expectedNodeText, int numChildren) {
		assertEquals(numChildren, node.getChildren().size());
		assertEquals(expectedNodeText, node.getText().toString());
	}
	
	protected void assertNodeTextRegion(IOutlineNode node, String fullModelText, String keyword, int fullLength, int significantLength) {
		int index = fullModelText.indexOf(keyword);
		assertTrue("keyword not found: " + keyword + "; fullModelText: " + fullModelText, index > -1);
		assertEquals(fullLength, node.getFullTextRegion().getLength());
		// ? assertEquals(index, node.getFullTextRegion().getOffset());
		assertEquals(index, node.getSignificantTextRegion().getOffset());
		assertEquals(significantLength, node.getSignificantTextRegion().getLength());		
	}
	
	private IOutlineNode getOutlineRootNode(String text) throws IOException, Exception {
		XtextResource resource = getResourceAndExpect(getAsStream(text), 0);
		document.setInput(resource);
		IOutlineNode root = treeProvider.createRoot(document);
		traverseChildren(root);
		root = root.getChildren().get(0);
		return root;
	}

	private void traverseChildren(IOutlineNode node) {
		for (IOutlineNode child : node.getChildren()) {
			traverseChildren(child);
		}
	}
}
