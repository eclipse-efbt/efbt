/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz, Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.building;

import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.eson.eFactory.Attribute;
import org.eclipse.emf.eson.eFactory.Containment;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.MultiValue;
import org.eclipse.emf.eson.eFactory.Reference;
import org.eclipse.emf.eson.eFactory.Value;
import org.eclipse.emf.eson.eFactory.impl.ReferenceImpl;
import org.eclipse.emf.eson.eFactory.util.EFactorySwitch;
import org.eclipse.emf.eson.util.EcoreUtil3;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.xtext.EcoreUtil2;

/**
 * Switch creating appropriate FeatureBuilder.
 * 
 * This checks if the requested Value actually makes sense for its Feature, and
 * returns null if it doesn't. This can happen during typing in editor - user
 * might not have typed {} yet, or user might have forgotten a [ ] or is
 * missing the = and so the Parser cannot create the correct Value; we
 * want the ModelBuider to ignore all these cases.
 */
public class FeatureSwitch extends EFactorySwitch<FeatureBuilder> {
	
	@Override
	public FeatureBuilder caseContainment(Containment object) {
		EStructuralFeature eFeature = getEFeature(object);
		if (!EcoreUtil3.isEContainment(eFeature))
			return null;
		
		if (object.getValue() == null)
			return null;
		
		return new ContainmentBuilder(object);
	}

	@Override
	public FeatureBuilder caseReference(Reference object) {
		if (!EcoreUtil3.isEReference(getEFeature(object)))
			return null;
		
		final ReferenceImpl objectImpl = (ReferenceImpl)object;
		if (objectImpl.basicGetValue() == null)
			return null;
		
		return new ReferenceBuilder(objectImpl);
	}

	@Override
	public FeatureBuilder caseAttribute(Attribute object) {
		if (!EcoreUtil3.isEAttribute(getEFeature(object)))
			return null;

		return new AttributeBuilder(object);
	}

	@Override
	public FeatureBuilder caseMultiValue(MultiValue object) {
		EStructuralFeature eFeature = getEFeature(object);
		if (eFeature == null || !eFeature.isMany())
			return null; 

		return new MultiValueBuilder(object);
	}

	@Override
	public FeatureBuilder caseFeature(Feature object) {
		final Value value = object.getValue();
		if (value == null) {
			return null;
		}
		return doSwitch(value);
	}
	
	private @Nullable EStructuralFeature getEFeature(Value value) {
		final Feature containingFeature = EcoreUtil2.getContainerOfType(value, Feature.class);
		if ( containingFeature != null )
			return containingFeature.getEFeature();
		return null;
	}
}
