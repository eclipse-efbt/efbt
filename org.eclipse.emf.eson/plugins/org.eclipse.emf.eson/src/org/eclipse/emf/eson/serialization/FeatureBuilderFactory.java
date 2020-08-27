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

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.Value;

/**
 * Helper Factory for FeatureBuilders.
 *  
 * @author Michael Vorburger
 */
//intentionally a package local class, the entry point to this package is FactoryBuilder, only
class FeatureBuilderFactory {
	private FeatureBuilderFactory() { }

	static FeatureBuilder newFeatureBuilder(EStructuralFeature eFeature, IFactoryBuilder factoryBuilder, Object value) {
		FeatureBuilder builder;
//		if (eFeature.isMany()) {
//			builder = MultiValueBuilder.multiValue(eFeature, factoryBuilder).value(value);
//		} else 
		if (eFeature instanceof EAttribute) {
			final EAttribute eAttribute = (EAttribute) eFeature;
			builder = AttributeBuilder.attribute(eAttribute, factoryBuilder).value(value);
		} else if (eFeature instanceof EReference) {
			final EReference eReference = (EReference) eFeature;
			if (eReference.isContainment()) {
				builder = ContainmentBuilder.containment(eReference, factoryBuilder).value(value);
			} else {
				builder = ReferenceBuilder.reference(eReference, factoryBuilder).value(value);
			}
		} else {
			throw new IllegalArgumentException("Huh, WTF is an EStructuralFeature that is neither an EAttribute nor an EReference?! " + eFeature.toString());
		}
		return builder;
	}
	
	static Value createValue(EStructuralFeature eFeature, IFactoryBuilder factoryBuilder, Object value) {
		return newFeatureBuilder(eFeature, factoryBuilder, value).createValue();
	}
	
	static Feature createFeature(EStructuralFeature eFeature, IFactoryBuilder factoryBuilder, Object value) {
		return newFeatureBuilder(eFeature, factoryBuilder, value).build();
	}
}
