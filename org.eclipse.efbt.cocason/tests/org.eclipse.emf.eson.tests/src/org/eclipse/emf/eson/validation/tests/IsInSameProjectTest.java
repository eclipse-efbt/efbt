/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2015 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.validation.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.eson.validation.EFactoryJavaValidator;
import org.junit.Test;

public class IsInSameProjectTest {

	@Test
	public void testPluginProject() {
		URI uri1 = URI.createPlatformPluginURI("/com.corp.plugin/model/some.ecore", true);
		URI uri2 = URI.createPlatformResourceURI("/some-project/src/folder/my.eson", true);
		assertFalse(EFactoryJavaValidator.isInSameProject(uri1, uri2));
	}

	@Test
	public void testNSURI() {
		URI uri1 = URI.createURI("http://testmodel/1.0");
		URI uri2 = URI.createPlatformResourceURI("/some-project/src/folder/my.eson", true);
		assertFalse(EFactoryJavaValidator.isInSameProject(uri1, uri2));
	}

	@Test
	public void testTwoDifferentWorkspaceProjects() {
		URI uri1 = URI.createPlatformResourceURI("/another-project/model/fancy.xcore", true);
		URI uri2 = URI.createPlatformResourceURI("/some-project/src/folder/my.eson", true);
		assertFalse(EFactoryJavaValidator.isInSameProject(uri1, uri2));
	}

	@Test
	public void testSameProject() {
		URI uri1 = URI.createPlatformResourceURI("/some-project/model/my.xcore", true);
		URI uri2 = URI.createPlatformResourceURI("/some-project/src/folder/my.eson", true);
		assertTrue(EFactoryJavaValidator.isInSameProject(uri1, uri2));
	}

	@Test
	public void nonPlatformURI() {
		URI uri1 = URI.createFileURI("afile.txt");
		URI uri2 = URI.createFileURI("bfile.txt");
		assertFalse(EFactoryJavaValidator.isInSameProject(uri1, uri2));
	}

}
