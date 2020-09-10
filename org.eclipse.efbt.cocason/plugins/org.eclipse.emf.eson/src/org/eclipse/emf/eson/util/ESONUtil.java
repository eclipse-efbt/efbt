/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2015 Michael Vorburger & others.
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.util;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.serialization.EFactoryAdapter;

public class ESONUtil {
	
	/**
	 * Determine if an EObject is part of the primary (source) AST model, or the secondary "real" derived EMF model.
	 *  
	 * This is needed in EFactoryTextEditComposer due to Xtext bug 460309; and in {@link EFactoryAdapter}.
	 * 
	 * @see https://bugs.eclipse.org/bugs/show_bug.cgi?id=460309 
	 * 
	 * @author Anton Kosyakov (JavaDoc by Michael Vorburger)
	 */
	public static boolean isSource(EObject obj) {
		EObject container = obj.eContainer();
		if (container != null) {
			return isSource(container);
		}
		EList<EObject> contents = obj.eResource().getContents();
		return !contents.isEmpty() && contents.get(0) == obj;
	}

}
