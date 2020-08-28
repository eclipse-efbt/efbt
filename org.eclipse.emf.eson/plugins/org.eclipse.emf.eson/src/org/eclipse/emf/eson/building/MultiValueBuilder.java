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
package org.eclipse.emf.eson.building;

import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.eson.eFactory.MultiValue;
import org.eclipse.emf.eson.eFactory.Value;

/**
 * NewObject2EObject FeatureBuilder for [] MultiValue.
 * 
 * @author Michael Vorburger
 */
public class MultiValueBuilder extends FeatureBuilder {

	protected MultiValue multiValue;
	private final FeatureSwitch featureSwitch = new FeatureSwitch(); // safer not to use static here
	
	public MultiValueBuilder(MultiValue multiValue) {
		this.multiValue = multiValue;
	}

	@Override
	public void build(boolean preLinkingPhase) throws ModelBuilderException {
		final EStructuralFeature eFeature = getFeature().getEFeature();
		if (eFeature.eIsProxy())
			return;
		
		for (Value listValue : multiValue.getValues()) {
			FeatureBuilder itemFeatureBuilder = featureSwitch.doSwitch(listValue);
			if (itemFeatureBuilder != null)
				itemFeatureBuilder.modelBuilder(getModelBuilder()).container(getContainer()).feature(getFeature()).build(preLinkingPhase);
		}
	}

}
