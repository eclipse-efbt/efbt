/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2013 - 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.serialization;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.eson.eFactory.Value;

/**
 * EObject2NewObject FeatureBuilder for [] MultiValue.
 * 
 * @author Michael Vorburger
 */
//intentionally a package local class, the entry point to this package is FactoryBuilder, only
class MultiValueBuilder extends FeatureBuilder {

	private EStructuralFeature isManyFeature;

	protected MultiValueBuilder(EStructuralFeature isManyFeature, IFactoryBuilder factoryBuilder) {
		super(factoryBuilder);
		if (!isManyFeature.isMany()) 
			throw new IllegalArgumentException(isManyFeature.toString() + " is not isMany()");
		this.isManyFeature = isManyFeature;
	}

	public static FeatureBuilder multiValue(EStructuralFeature isManyFeature, IFactoryBuilder factoryBuilder) {
		return new MultiValueBuilder(isManyFeature, factoryBuilder);
	}
	
	@Override
	protected Value createValue() {
		return FeatureBuilderFactory.createValue(isManyFeature, factoryBuilder, value);
	}

	@Override
	protected EStructuralFeature getEFeature() {
		return isManyFeature;
	}

}
