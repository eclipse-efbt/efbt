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
package org.eclipse.emf.eson.conversion.tests;

import static org.junit.Assert.assertEquals;

import org.eclipse.emf.eson.EFactoryInjectorProvider;
import org.eclipse.emf.eson.conversion.StringOrQualifiedNameWithWildcardValueConverter;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.inject.Inject;

@RunWith(XtextRunner.class)
@InjectWith(EFactoryInjectorProvider.class)
public class StringOrQualifiedNameWithWildcardValueConverterTest {

	@Inject StringOrQualifiedNameWithWildcardValueConverter conv;

	@Test
	public void testToValue() {
		assertEquals(null, conv.toValue(null, null));
		assertEquals("", conv.toValue("", null));
		assertEquals("simplePackageLikeNsURI", conv.toValue("simplePackageLikeNsURI", null));
		assertEquals("dotted.Package.Like.NsURI", conv.toValue("dotted.Package.Like.NsURI", null));
		assertEquals("dotted.Package.Like.NsURI", conv.toValue("dotted.Package.Like.NsURI.*", null));
		assertEquals("http://namespace/", conv.toValue("\"http://namespace/\"", null));		
		assertEquals("http://namespace/", conv.toValue("\'http://namespace/\'", null));		
		assertEquals("", conv.toValue("\"\"", null));		
	}

	@Test
	public void testToString() {
		assertEquals(null, conv.toString(null));
		assertEquals("", conv.toString(""));
		assertEquals("simplePackageLikeNsURI", conv.toString("simplePackageLikeNsURI"));
		assertEquals("dotted.Package.Like.NsURI", conv.toString("dotted.Package.Like.NsURI"));
		assertEquals("dotted.Package.Like.NsURI.*", conv.toString("dotted.Package.Like.NsURI.*"));
		assertEquals("\"http://namespace/\"", conv.toString("http://namespace/"));
	}

}
