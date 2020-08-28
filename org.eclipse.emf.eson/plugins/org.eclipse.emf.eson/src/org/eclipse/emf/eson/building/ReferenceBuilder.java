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

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.eFactory.impl.ReferenceImpl;
import org.eclipse.emf.eson.util.EcoreUtil3;

public class ReferenceBuilder extends FeatureBuilder {

	private ReferenceImpl reference;

	public ReferenceBuilder(ReferenceImpl reference) {
		this.reference = reference;
	}

	@Override
	public void build(boolean preLinkingPhase) throws ModelBuilderException {
		if (preLinkingPhase)
			return;
		getModelBuilder().addDeferredLinkingFeatureBuilder(this);
	}

	public void link() throws ModelBuilderException {
		EObject newValue = getReferencedObject();
		final EStructuralFeature eFeature = getFeature().getEFeature();
		if (eFeature.eIsProxy())
			return;
		EcoreUtil3.setOrAddValue(getContainer(), eFeature, newValue);
	}
	
	protected EObject getReferencedObject() throws ModelBuilderException {
		EObject referencedObject = ((ReferenceImpl)reference).basicGetValue();
		if (referencedObject instanceof NewObject) {
			throw new IllegalStateException("We shouldn't have any NewObject here anymore now?!");
		}
		if (referencedObject.eIsProxy()) {
			// TODO subclass problem.. :-( @see org.eclipse.xtext.linking.lazy.LazyLinker.findInstantiableCompatible(EClass)
			EReference eRef = (EReference) getFeature().getEFeature();
			EClass refEClass = eRef.getEReferenceType();
			InternalEObject newProxy = (InternalEObject) EcoreUtil.create(refEClass);
			URI proxyURI = ((InternalEObject)referencedObject).eProxyURI();
			newProxy.eSetProxyURI(proxyURI);
			return newProxy;
		}
		return referencedObject;
	}
}
