/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2002 - 2014 Sebastian Benz, Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.serialization;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.eson.eFactory.EFactoryFactory;
import org.eclipse.emf.eson.eFactory.Reference;
import org.eclipse.emf.eson.eFactory.Value;

// intentionally a package local class, the entry point to this package is FactoryBuilder, only
class ReferenceBuilder extends FeatureBuilder {

	private EReference eReference;

	protected ReferenceBuilder(EReference reference, IFactoryBuilder factoryBuilder) {
		super(factoryBuilder);
		this.eReference = reference;
	}

	@Override
	protected Value createValue() {
		Reference reference = EFactoryFactory.eINSTANCE.createReference();
		reference.setValue((EObject) value);
		return reference;
	}

	@Override
	protected EStructuralFeature getEFeature() {
		return eReference;
	}

	public static ReferenceBuilder reference(EReference reference, IFactoryBuilder factoryBuilder) {
		return new ReferenceBuilder(reference, factoryBuilder);
	}

}
