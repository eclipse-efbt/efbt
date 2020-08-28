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
import org.eclipse.emf.ecore.EDataType.Internal.ConversionDelegate;
import org.eclipse.emf.ecore.util.EcoreUtil;

/**
 * IValueObjectToString implementation which uses the core EMF stuff.
 * 
 * @author Michael Vorburger
 */
public class ValueObjectToStringViaEMF implements IValueObjectToString {

	// This isn't actually currently used in EFactory, but here as part of the (possible, to confirm) basis for an idea I had for later - to support dynamic data types.

	public String toString(Object value, EDataType eDataType) {
		String string = convertToStringViaConversionDelegate(eDataType, value);
		if (string == null)
			string = EcoreUtil.convertToString(eDataType, value);
		return string;
	}

	protected String convertToStringViaConversionDelegate(EDataType eDataType, Object value) {
		String defaultString = null;
		if (eDataType instanceof EDataType.Internal) {
			EDataType.Internal internalEAttributeType = (EDataType.Internal) eDataType;
			ConversionDelegate converter = internalEAttributeType.getConversionDelegate();
			if (converter != null)
				defaultString = converter.convertToString(value);
		}
		return defaultString;
	}

}
