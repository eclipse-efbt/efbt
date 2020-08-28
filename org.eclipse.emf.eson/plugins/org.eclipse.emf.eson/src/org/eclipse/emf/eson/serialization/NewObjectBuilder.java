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

import java.util.List;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.eson.building.NameAccessor;
import org.eclipse.emf.eson.eFactory.EFactoryFactory;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.MultiValue;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.eFactory.Value;
import org.eclipse.jdt.annotation.NonNull;

// intentionally a package local class, the entry point to this package is FactoryBuilder, only
class NewObjectBuilder {

	private final Factory context;
	private NameAccessor nameAccessor = new NameAccessor();
	private EAttribute nameEAttribute;
	private final IFactoryBuilder factoryBuilder;
	
	private NewObjectBuilder(Factory context, IFactoryBuilder factoryBuilder) {
		this.context = context;
		this.factoryBuilder = factoryBuilder;
	}

	public static NewObjectBuilder context(Factory context, IFactoryBuilder factoryBuilder) {
		return new NewObjectBuilder(context, factoryBuilder);
	}

	public NewObject build(EObject input) {
		NewObject newObject = EFactoryFactory.eINSTANCE.createNewObject();
		newObject.setEClass(input.eClass()); // NOTE ContainmentBuilder.createNewObject() may null-ify this again
		setName(newObject, input);
		addAttributes(newObject, input);
		addContainments(newObject, input);
		addReferences(newObject, input);
		addIsManyStructuralFeatures(newObject, input);
		return newObject;
	}

	private void setName(@NonNull NewObject newObject, EObject input) {
		nameEAttribute = nameAccessor.getNameAttribute(newObject, context);
		if (nameEAttribute != null) {
			if (input.eIsSet(nameEAttribute)) {
				String name = (String) input.eGet(nameEAttribute);
				if (name != null) {
					if (isNameAfterClass(name)) {
						newObject.setName(name);
					} else {
						final EList<Feature> features = newObject.getFeatures();
						features.add(createAttribute(nameEAttribute, name));
					}
				}
			}
		}
	}

	private boolean isNameAfterClass(String name) {
		return factoryBuilder.getEFactoryServiceServiceProvider().getValidIDChecker().isValidID(name);
	}

	private void addContainments(NewObject newObject, EObject input) {
		final EList<Feature> features = newObject.getFeatures();
		for (EReference containment : input.eClass().getEAllContainments()) {
			if (!containment.isMany()) { // isMany are handled by addIsManyStructuralFeatures
				Object containmentValue = input.eGet(containment);
				if (containmentValue != null) {
					features.add(createContainment(containment, containmentValue));
				}
			}
		}
	}

	private Feature createContainment(EReference containment, Object containmentValue) {
		return ContainmentBuilder.containment(containment, factoryBuilder).factory(context)
				.value(containmentValue).build();
	}
	
	private void addReferences(NewObject newObject, EObject input) {
		final EList<Feature> features = newObject.getFeatures();
		for (EReference eReference : input.eClass().getEAllReferences()) {
			if (!eReference.isContainment()) { // isContainment are handled by addContainments
				if (!eReference.isMany()) {    // isMany are handled by addIsManyStructuralFeatures
					Object referenceValue = input.eGet(eReference);
					if (referenceValue != null) {
						features.add(createReference(eReference, referenceValue));
					}
				}
			}
		}
	}

	private Feature createReference(EReference eReference, Object referencedElement) {
		return ReferenceBuilder.reference(eReference, factoryBuilder).value(referencedElement).build();
	}

	private void addAttributes(NewObject newObject, EObject input) {
		final EList<Feature> features = newObject.getFeatures();
		for (EAttribute attribute : input.eClass().getEAllAttributes()) {
			if (attribute != nameEAttribute) {
				if (!attribute.isMany()) {    // isMany are handled by addIsManyStructuralFeatures
					Object attributeValue = input.eGet(attribute);
					if (attributeValue != null) {
						features.add(createAttribute(attribute, attributeValue));
					}
				}
			}
		}
	}

	private Feature createAttribute(EAttribute attribute, Object value) {
		return AttributeBuilder.attribute(attribute, factoryBuilder).value(value).build();
	}

	private void addIsManyStructuralFeatures(NewObject newObject, EObject input) {
		final EList<Feature> features = newObject.getFeatures();
		for (EStructuralFeature eFeature : input.eClass().getEAllStructuralFeatures()) {
			if (eFeature.isMany()) {
				List<?> eListValues = (List<?>) input.eGet(eFeature);
				if (eListValues.isEmpty())
					continue;
				
				final Feature newFeature = EFactoryFactory.eINSTANCE.createFeature();
				newFeature.setEFeature(eFeature);
				features.add(newFeature);
				MultiValue multiValue = EFactoryFactory.eINSTANCE.createMultiValue();
				newFeature.setValue(multiValue);
				
				EList<Value> values = multiValue.getValues();
				for (Object value : eListValues) {
					Value multiValueItem = (Value) MultiValueBuilder.multiValue(eFeature, factoryBuilder).value(value).createValue();
					values.add(multiValueItem);
				}

			}
		}
	}
}
