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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.PackageImport;
import org.eclipse.emf.eson.util.EPackageRegistry;
import org.eclipse.emf.eson.util.EcoreUtil3;
import org.eclipse.xtext.naming.IQualifiedNameConverter;
import org.eclipse.xtext.resource.EObjectDescription;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.scoping.IScope;
import org.eclipse.xtext.scoping.Scopes;
import org.eclipse.xtext.scoping.impl.SimpleScope;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

/**
 * Scope Provider which finds Ecore EPackage both from within the workspace using the Xtext index as well as from the global EPackage Registry.
 */
public class EFactoryScopeProvider extends EFactoryScopeProviderNG {

	protected @Inject EPackageRegistry ePackageRegistry;
	protected @Inject IQualifiedNameConverter qualifiedNameConverter;

	public IScope scope_PackageImport_ePackage(PackageImport packageImport, EReference eReference) {
		final IScope parent = delegateGetScope(packageImport, eReference);
		// Following shamelessly ;) stolen from org.eclipse.xtext.xtext.XtextScopeProvider.createEPackageScope(Grammar):
		Iterable<String> nsURIs = ePackageRegistry.getNsURIs(packageImport);
		return new SimpleEPackageScope(parent, Iterables.transform(nsURIs, new Function<String, IEObjectDescription>() {
			@Override
			public IEObjectDescription apply(String from) {
				InternalEObject proxyPackage = (InternalEObject) EcoreFactory.eINSTANCE.createEPackage();
				proxyPackage.eSetProxyURI(URI.createURI(from));
				return EObjectDescription.create(qualifiedNameConverter.toQualifiedName(from), proxyPackage, Collections.singletonMap("nsURI", "true"));
			}
		}));
	}

	@Override
	// This is implicitly also used for scope_CustomNameMapping_eClass
	public IScope scope_EClass(EObject context, EReference reference) {
		final IScope parent = super.scope_EClass(context, reference); // delegateGetScope + DottedQualifiedNameAwareScope
		IScope scope = createEClassScope(context.eResource(), null, parent);
		return scope;
	}

	// Feature == Containment here, always, is it?
	public IScope scope_NewObject_eClass(Feature feature, EReference eReference) {
		if (feature.getEFeature() instanceof EReference) {
			final IScope parent = super.scope_EClass(feature, eReference); // delegateGetScope + DottedQualifiedNameAwareScope
			return createEClassScope(feature.eResource(), (EClass) feature.getEFeature().getEType(), parent);
		} else
			return IScope.NULLSCOPE;
	}

	protected IScope createEClassScope(Resource resource, EClass type, IScope parent) {
		Iterable<EPackage> ePackages = resolvePackages(resource);
		Iterable<EClass> eClasses = getAllEClasses(ePackages);
	//	List<EClass> eobjectclass = new ArrayList<EClass>();
	//	 eobjectclass.add(EcoreFactory.eINSTANCE.createEObject().eClass());
	//	 Iterable<EClass> eClasses = Iterables.concat(eClasses1, eobjectclass);
		if (type != null)
			eClasses = filterAssignableEClasses(eClasses, type);
		Iterable<IEObjectDescription> scopedElements = Scopes.scopedElementsFor(eClasses, DottedQualifiedNameFixer.FUNCTION);
		return new SimpleScope(parent, scopedElements);
	}

	private Iterable<EClass> filterAssignableEClasses(Iterable<EClass> eClasses, final EClass targetType) {
		return Iterables.filter(eClasses, new Predicate<EClass>() {
			public boolean apply(EClass input) {
				if (EcoreUtil3.isSubClass(targetType, input)
						&& EcoreUtil3.isInstantiatable(input))
					return true;
				else
					return false;
			}
		});
	}

	private Iterable<EPackage> resolvePackages(Resource resource) {
		Factory root = (Factory) resource.getContents().get(0);
		if (root == null) {
			return Collections.emptyList();
		}
		return getEPackages(root);
	}

	/**
	 * Gets EPackages from the 'use' clause.
	 */
	private Iterable<EPackage> getEPackages(Factory factory) {
		List<PackageImport> ePackageUris = factory.getEPackages();
		List<EPackage> result = new ArrayList<EPackage>(ePackageUris.size());
		for (PackageImport packageImport : ePackageUris) {
			EPackage ePackage = packageImport.getEPackage();
			if (ePackage != null)
				result.add(ePackage);
		}
		return result;
	}
	
	public Iterable<EClass> getAllEClasses(Iterable<? extends EPackage> ePackages) {
		return Iterables.concat(Iterables.transform(ePackages,
				new Function<EPackage, Iterable<EClass>>() {
					@Override
					public Iterable<EClass> apply(EPackage input) {
						// only the EClass/es of the given EPackage, but NOT their sub-packages
						return Iterables.filter(input.getEClassifiers(), EClass.class);
						// OLD BEHAVIOUR: return Iterables.filter(ImmutableList.copyOf(input.eAllContents()), EClass.class);
					}
				}));
	}

}
