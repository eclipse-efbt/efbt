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

/**
 * Something that can convert a Value Object to a String (used e.g. as a Proposal).
 * 
 * This is obviously very similar to the org.eclipse.xtext.conversion.IValueConverterService,
 * but its based on an EDataType instead of a String lexerRule.
 * 
 * @author Michael Vorburger
 */
public interface IValueObjectToString {

	// This isn't actually currently used in EFactory, but here as part of the (possible, to confirm) basis for an idea I had for later - to support dynamic data types.

	String toString(Object value, EDataType eDataType);
	
}
