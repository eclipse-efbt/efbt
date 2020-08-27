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
package org.eclipse.emf.eson.scoping;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.eson.building.NewObjectExtensions;
import org.eclipse.emf.eson.eFactory.Attribute;
import org.eclipse.emf.eson.eFactory.CustomNameMapping;
import org.eclipse.emf.eson.eFactory.EnumAttribute;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.MultiValue;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.util.EcoreUtil3;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.scoping.ICaseInsensitivityHelper;
import org.eclipse.xtext.scoping.IScope;
import org.eclipse.xtext.scoping.Scopes;
import org.eclipse.xtext.scoping.impl.AbstractDeclarativeScopeProvider;
import org.eclipse.xtext.scoping.impl.FilteringScope;
import org.eclipse.xtext.scoping.impl.SimpleScope;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.inject.Inject;

/**
 * Scope Provider which finds everything only from within the workspace using the Xtext index, as usual.
 * 
 * @see EFactoryScopeProvider
 */
public class EFactoryScopeProviderNG extends AbstractDeclarativeScopeProvider {

	protected @Inject ICaseInsensitivityHelper caseInsensitivityHelper;
	protected @Inject NewObjectExtensions newObjectExtensions;

	public IScope scope_EnumAttribute_value(EnumAttribute attribute, EReference reference) {
		Feature feature = getFeature(attribute);
		if (feature.getEFeature().getEType() instanceof EEnum) {
			EEnum enumType = (EEnum) feature.getEFeature().getEType();
			Iterable<IEObjectDescription> elements = Scopes.scopedElementsFor(enumType.getELiterals(), DottedQualifiedNameFixer.FUNCTION);
			return new SimpleScope(elements);
		}
		return IScope.NULLSCOPE;
	}
	
	// This may look a bit strange, but is required for 
	// org.eclipse.emf.eson.ui.contentassist.EFactoryProposalProvider.completeFeature_EFeature()
	public IScope scope_Feature_eFeature(NewObject newObject, EReference reference) {
		Optional<EClass> optionalEClass = newObjectExtensions.getDeclaredOrInferredEClass(newObject);
		if (optionalEClass.isPresent()) {
			EClass eClass = optionalEClass.get();
			Iterable<? extends EObject> assignableFeature = EcoreUtil3.getAssignableFeatures(eClass );
			Iterable<IEObjectDescription> descs = Scopes.scopedElementsFor(assignableFeature, DottedQualifiedNameFixer.FUNCTION);
			return new SimpleScope(descs);
		} else {
			return IScope.NULLSCOPE;
		}
	}

	public IScope scope_Feature_eFeature(Feature feature, EReference reference) {
		NewObject newObject = (NewObject) feature.eContainer();
		return scope_Feature_eFeature(newObject, reference);
	}

	public IScope scope_Feature_reference(NewObject newObject, EReference reference) {
		Optional<EClass> optionalEClass = newObjectExtensions.getDeclaredOrInferredEClass(newObject);
		if (optionalEClass.isPresent()) {
			return new SimpleScope(Scopes.scopedElementsFor(optionalEClass.get().getEAllStructuralFeatures(), DottedQualifiedNameFixer.FUNCTION));
		} else {
			return IScope.NULLSCOPE;
		}
	}

	public IScope scope_EClass(EObject context, EReference reference) {
		IScope scope = delegateGetScope(context, reference);
		scope = new FilteringScope(scope, new Predicate<IEObjectDescription>() {
			public boolean apply(IEObjectDescription desc) {
				// @see org.eclipse.xtext.ecore.EcoreResourceDescriptionStrategy
				return !"true".equals(desc.getUserData("nsURI"));
			}
		});
		return new DottedQualifiedNameAwareScope(scope, isIgnoreCase(reference));
	}

	public IScope scope_Reference_value(Feature feature, EReference eReference) {
		EStructuralFeature sourceFeature = feature.getEFeature();
		if (EcoreUtil3.isEReference(sourceFeature)) {
			EReference realEReference = (EReference) sourceFeature;
			IScope parentScope = delegateGetScope(feature, realEReference);
			final EClass referenceType = realEReference.getEReferenceType();
			return new FilteringScope(parentScope, new Predicate<IEObjectDescription>() {
				public boolean apply(IEObjectDescription desc) {
					return referenceType.isSuperTypeOf(desc.getEClass());
				}
			});
		}
		return IScope.NULLSCOPE;
	}

	public IScope scope_CustomNameMapping_nameFeature(CustomNameMapping mapping, EReference reference) {
		Iterable<EAttribute> attributes = EcoreUtil3.getAllAttributes(mapping.getEClass(), String.class);
		Iterable<IEObjectDescription> elements = Scopes.scopedElementsFor(attributes, DottedQualifiedNameFixer.FUNCTION);
		return new SimpleScope(elements);
	}

	public IScope scope_Containment_value(Feature feature, EReference eReference) {
		if (EcoreUtil3.isEContainment(feature.getEFeature())) {
			return super.getDelegate().getScope(feature, eReference);
		}
		return IScope.NULLSCOPE;
	}

	protected Feature getFeature(Attribute attribute) {
		 EObject container = attribute.eContainer();
		 if (container instanceof MultiValue) {
			 // MultiValue mv = (MultiValue) container;
			 container = container.eContainer();
		 }
		 return (Feature) container; 
	}

	protected boolean isIgnoreCase(EReference reference) {
		return caseInsensitivityHelper.isIgnoreCase(reference);
	}

}
