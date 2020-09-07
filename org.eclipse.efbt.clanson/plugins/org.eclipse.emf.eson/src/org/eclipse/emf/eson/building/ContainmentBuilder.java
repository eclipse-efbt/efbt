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

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.eson.eFactory.Containment;
import org.eclipse.emf.eson.util.EcoreUtil3;

import com.google.common.base.Optional;

public class ContainmentBuilder extends FeatureBuilder {

	private Containment containment;

	public ContainmentBuilder(Containment containment) {
		this.containment = containment;
	}

	@Override
	public void build(boolean preLinkingPhase) throws ModelBuilderException {
		Optional<EObject> newValue = getModelBuilder().build(containment.getValue(), preLinkingPhase);
		final EStructuralFeature eFeature = getFeature().getEFeature();
		if (eFeature.eIsProxy())
			return;
		if (newValue.isPresent())
			EcoreUtil3.setOrAddValue(getContainer(), eFeature, newValue.get());
	}

}
