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

import org.apache.log4j.Logger;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.eson.eFactory.Attribute;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.scoping.EFactoryQualifiedNameProvider;
import org.eclipse.emf.eson.util.EcoreUtil3;
import org.eclipse.emf.eson.util.ValueResolver;

public class AttributeBuilder extends FeatureBuilder {
	private static final Logger logger = Logger.getLogger(AttributeBuilder.class);

	private final static ValueSwitch valueSwitch = new ValueSwitch();
	private final static ValueResolver valueResolver = new ValueResolver();

	private Attribute attribute;

	public AttributeBuilder(Attribute object) {
		this.attribute = object;
	}

	@Override
	public void build(boolean preLinkingPhase) throws ModelBuilderException {
		EStructuralFeature eFeature = getFeature().getEFeature();
		if (!(eFeature instanceof EAttribute))
			return;
		EAttribute eAttribute = (EAttribute) eFeature;
		if (eAttribute.eIsProxy())
			return;

		String name = eFeature.getName();
		if (preLinkingPhase
				// see point a) of TODO in ModelBuilder build() about this: 
				&& name != null && !name.equals(EFactoryQualifiedNameProvider.NAME_ATTRIBUTE_NAME))
			return;

		Object newValue = valueResolver.apply(attribute);
		
		// @see BrokenEnumTest
		if (newValue instanceof EEnumLiteral) {
			EEnumLiteral enumLiteral = (EEnumLiteral) newValue;
			if (enumLiteral.eIsProxy()) {
				return;
			}
		}
		
		Class<?> clazz = eAttribute.getEAttributeType().getInstanceClass();
		if (clazz == null) {
			if (!(newValue instanceof EEnumLiteral)) {
				throw new ModelBuilderException("Uh uh, EAttributeType().getInstanceClass() == null, but its not an EEnumLiteral, for feature: " + eAttribute.toString());
			} // else, if newValue is an EEnumLiteral, then ahl-iz-wehl - it's a dynamic ecore model, without generated code for the Enum literal - which is still fine.
		} else {
			try {
				newValue = convertToTargetType(clazz, newValue, getFeature());
			} catch (RuntimeException e) {
				throw new ModelBuilderException("convertToTargetType() failed for feature: " + eAttribute.toString(), e);
			}
		}

		EcoreUtil3.setOrAddValue(getContainer(), eAttribute, newValue);
	}

	protected Object convertToTargetType(Class<?> clazz, Object newValue, Feature feature) throws IllegalArgumentException {
		Object o = valueSwitch.doSwitch(clazz, newValue);
		if (!clazz.isPrimitive() && o != null && !clazz.isInstance(o)) {
			logger.warn("Likely upcoming ClassCastException 'heads up' - failed to convert value '"
					+ newValue.toString() + "' to an instance of class '"
					+ clazz.toString() + "', instead its: " + o.toString());
		}
		return o;
	}
}
