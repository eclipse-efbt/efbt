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
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.eson.eFactory.CustomNameMapping;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.xtext.EcoreUtil2;

import com.google.common.collect.Iterables;

@NonNullByDefault
public class NameAccessor {
	@SuppressWarnings("null")
	private static Logger logger = Logger.getLogger(ModelBuilder.class);

	protected static final String DEFAULT_NAME_FEATURE = "name";

	public void setName(NewObject newObject, EObject eObject, String name) {
		EAttribute nameAttribute = getNameAttribute(newObject);
		if (nameAttribute != null)
			setName(eObject, name, nameAttribute);
	}

//	public String getName(EObject context, EObject eObject) throws NoNameFeatureMappingException {
//		Check.hasEPackage(context, EFactoryPackage.eINSTANCE);
//		EAttribute nameAttribute = getNameAttribute(context, eObject);
//		Object name = eObject.eGet(nameAttribute);
//		if (name == null) {
//			name = "";
//		}
//		return name.toString();
//	}
	
	protected void setName(EObject eObject, String name, EAttribute nameAttribute) {
		try {
			eObject.eSet(nameAttribute, name);
		} catch (IllegalArgumentException e) {
			logger.error("setName() failed to eSet, on EObject " + eObject.toString() + ", nameAttribute: " + nameAttribute + ", name: " + name, e);
		}
	}

	public @Nullable EAttribute getNameAttribute(NewObject newObject) {
		Factory context = EcoreUtil2.getContainerOfType(newObject, Factory.class);
		return getNameAttribute(newObject, context);
	}
	
	public @Nullable EAttribute getNameAttribute(NewObject newObject, @Nullable Factory context) {
		if (newObject.getEClass() == null)
			return null;
		if (context != null) {
			for (CustomNameMapping mapping : getCustomNameMappings(context)) {
				if (EcoreUtil2.isAssignableFrom(mapping.getEClass(), newObject.getEClass())) {
					EAttribute attribute = mapping.getNameFeature();
					if (attribute != null)
						return attribute;
					else
						return null;
				}
			}
		}
		// If no explicit mapping found, fall back to default:
		EStructuralFeature defaultNameFeature = newObject.getEClass().getEStructuralFeature(DEFAULT_NAME_FEATURE);
		if (defaultNameFeature != null && defaultNameFeature instanceof EAttribute)
			return (EAttribute) defaultNameFeature;
		else
			return null;
	}

	@SuppressWarnings("null")
	protected Iterable<CustomNameMapping> getCustomNameMappings(Factory factory) {
		return Iterables.filter(factory.getAnnotations(), CustomNameMapping.class);
	}

}
