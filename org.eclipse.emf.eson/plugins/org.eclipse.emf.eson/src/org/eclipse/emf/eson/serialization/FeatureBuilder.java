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

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.eson.eFactory.EFactoryFactory;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.Value;

// intentionally a package local class, the entry point to this package is FactoryBuilder, only
abstract class FeatureBuilder {

	protected Object value;
	protected final IFactoryBuilder factoryBuilder;

	protected FeatureBuilder(IFactoryBuilder factoryBuilder) {
		super();
		this.factoryBuilder = factoryBuilder;
	}

	protected Feature createFeature(EStructuralFeature eFeature) {
		Feature feature = EFactoryFactory.eINSTANCE.createFeature();
		feature.setEFeature(eFeature);
		feature.setValue(createValue());
		return feature;
	}

	public Feature build() {
		return createFeature(getEFeature());
	}

	protected abstract EStructuralFeature getEFeature();

	protected abstract Value createValue();

	public FeatureBuilder value(Object value) {
		this.value = value;
		return this;
	}

}
