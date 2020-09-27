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

import java.util.Collections;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.PackageImport;
import org.eclipse.emf.eson.util.EPackageRegistry;
import org.eclipse.xtext.naming.IQualifiedNameConverter;
import org.eclipse.xtext.resource.EObjectDescription;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.scoping.IScope;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

/**
 * Scope Provider which finds Ecore EPackage both from within the workspace using the Xtext index as well as from the global EPackage Registry.
 */
public class EFactoryScopeProvider extends EFactoryScopeProviderNG {

	protected @Inject EPackageRegistry ePackageRegistry;
	protected @Inject IQualifiedNameConverter qualifiedNameConverter;

	// TODO rm!
	@Inject	private IEPackageScopeProvider ePackageScopeProvider;

	public IScope scope_PackageImport_ePackage(PackageImport packageImport, EReference eReference) {
		final IScope parent = delegateGetScope(packageImport, eReference);
		// Following inspired & shamelessly ;) stolen from org.eclipse.xtext.xtext.XtextScopeProvider.createEPackageScope(Grammar):
		// but fixed up as discussed in https://bugs.eclipse.org/bugs/show_bug.cgi?id=489748
		Iterable<String> nsURIs = ePackageRegistry.getNsURIs(packageImport);
		return new SimpleEPackageScope(parent, Iterables.transform(nsURIs, new Function<String, IEObjectDescription>() {
			@Override
			public IEObjectDescription apply(String from) {
				InternalEObject proxyPackage = (InternalEObject) EcoreFactory.eINSTANCE.createEPackage();
				proxyPackage.eSetProxyURI(URI.createURI(from).appendFragment("/"));
				return EObjectDescription.create(qualifiedNameConverter.toQualifiedName(from), proxyPackage, Collections.singletonMap("nsURI", "true"));
			}
		}));
	}

	@Override
	// This is implicitly also used for scope_CustomNameMapping_eClass
	public IScope scope_EClass(EObject context, EReference reference) {
		final IScope parent = super.scope_EClass(context, reference); // delegateGetScope + DottedQualifiedNameAwareScope
		IScope scope = ePackageScopeProvider.createEClassScope(context.eResource(), parent);
		return scope;
	}

	// Feature == Containment here, always, is it?
	public IScope scope_NewObject_eClass(Feature feature, EReference eReference) {
		if (feature.getEFeature() instanceof EReference) {
			final IScope parent = super.scope_EClass(feature, eReference); // delegateGetScope + DottedQualifiedNameAwareScope
			return ePackageScopeProvider.createEClassScope(feature.eResource(), (EClass) feature.getEFeature().getEType(), parent);
		} else
			return IScope.NULLSCOPE;

	}

}
