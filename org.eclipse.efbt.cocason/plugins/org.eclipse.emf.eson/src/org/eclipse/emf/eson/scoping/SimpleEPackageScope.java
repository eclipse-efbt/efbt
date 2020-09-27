/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2016 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.scoping;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EcorePackage;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.scoping.IScope;
import org.eclipse.xtext.scoping.impl.SimpleScope;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class SimpleEPackageScope extends SimpleScope implements IScope {

	public SimpleEPackageScope(IScope parent, Iterable<IEObjectDescription> transform) {
		super(parent, transform);
	}

	@Override
	public Iterable<IEObjectDescription> getElements(EObject object) {
		Iterable<IEObjectDescription> result = super.getElements(object);
		if (Iterables.isEmpty(result) && (object instanceof EPackage)) {
			final EPackage ePackage = (EPackage) object;
			Iterable<IEObjectDescription> localElements = getLocalElementsByEClass(EcorePackage.Literals.EPACKAGE);
			result = Iterables.filter(localElements, new Predicate<IEObjectDescription>() {
				@Override
				public boolean apply(IEObjectDescription input) {
					return input.getQualifiedName().getFirstSegment().equals(ePackage.getNsURI());
				}
			});
		}
		return result;
	}
	
	protected Iterable<IEObjectDescription> getLocalElementsByEClass(final EClass eClass) {
		Iterable<IEObjectDescription> localElements = getAllLocalElements();
		return Iterables.filter(localElements, new Predicate<IEObjectDescription>() {
			@Override
			public boolean apply(IEObjectDescription input) {
				return input.getEClass().equals(eClass);
			}
		});
	}

}
