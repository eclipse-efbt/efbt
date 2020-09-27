/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz, Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.util;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EStructuralFeature;

import com.google.common.base.Function;

public class ValueResolver implements Function<EObject, Object> {

	public Object apply(EObject eObject) {
		EStructuralFeature valueFeature = eObject.eClass().getEStructuralFeature("value");
		if (valueFeature == null) {
			throw new IllegalArgumentException("'" + eObject.eClass().getName() + "' has no feature 'value'");
		}
		return eObject.eGet(valueFeature);
	}

}
