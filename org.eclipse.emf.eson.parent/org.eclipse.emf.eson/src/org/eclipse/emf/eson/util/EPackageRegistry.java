/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2016 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.util;

import java.util.List;

import org.eclipse.emf.common.util.WrappedException;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EPackage.Registry;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.eson.util.EPackageRegistry.EPackageRegistryESONImpl;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.ImplementedBy;

/**
 * Wrapper around EMF's EPackage Registry, with some fixes.
 *
 * @see org.eclipse.emf.ecore.EPackage.Registry
 *
 * @author Michael Vorburger
 */
@ImplementedBy(EPackageRegistryESONImpl.class)
public interface EPackageRegistry {

	Iterable<String> getNsURIs();
	Iterable<String> getNsURIs(EObject context);
	Iterable<String> getNsURIs(ResourceSet context);

	Optional<EPackage> getEPackage(String nsURI);
	Optional<EPackage> getEPackage(EObject context, String nsURI);
	Optional<EPackage> getEPackage(ResourceSet context, String nsURI);

    // EFactory getEFactory(String nsURI);
	
	
	public class EPackageRegistryESONImpl implements EPackageRegistry {

		protected final EPackage.Registry delegate;

		public EPackageRegistryESONImpl(Registry delegate) {
			super();
			if (delegate == null)
				throw new IllegalArgumentException("delegate == null");
			this.delegate = delegate;
		}
		
		public EPackageRegistryESONImpl() {
			this(EPackage.Registry.INSTANCE);
		}

		@Override
		public Iterable<String> getNsURIs() {
			return getNsURIs(delegate);
		}

		@Override
		public Iterable<String> getNsURIs(EObject context) {
			ResourceSet rs = context.eResource().getResourceSet();
			return getNsURIs(rs);
		}

		@Override
		public Iterable<String> getNsURIs(ResourceSet context) {
			return getNsURIs(context.getPackageRegistry());
		}

		protected Iterable<String> getNsURIs(EPackage.Registry registry) {
			Iterable<String> packageURIsIterable = registry.keySet();
			// hah! org.eclipse.emf.ecore.impl.EPackageRegistryImpl actually behaves some weired/unexpectly:
			// Whereas for getEPackage() it delegates to its delegateRegistry, for keySet() etc. it does not!
			// So we have to compensate for that here ourselves:
			if (!delegate.equals(registry)) 
				packageURIsIterable = Iterables.concat(packageURIsIterable, delegate.keySet());
			// copy due to potential ConcurrentModificationException (CME) while resolving EPackages later.. :(
			List<String> packageURIs = Lists.newArrayList(packageURIsIterable);
			// Strange, but yes we've seen packages with null nsURI in the wild!
			Iterable<String> packageUrisWithoutAnyNulls = Iterables.filter(packageURIs, Predicates.notNull());
			return packageUrisWithoutAnyNulls;
		}
		
		@Override
		public Optional<EPackage> getEPackage(String nsURI) {
			return getEPackage(delegate, nsURI);
		}
		
		@Override
		public Optional<EPackage> getEPackage(EObject context, String nsURI) {
			ResourceSet rs = context.eResource().getResourceSet();
			return getEPackage(rs, nsURI);
		}
		
		@Override
		public Optional<EPackage> getEPackage(ResourceSet context, String nsURI) {
			return getEPackage(context.getPackageRegistry(), nsURI);
		}

		/**
		 * We've seen a corner case (DS-6421) where there is no EMF ECore model gen.
		 * code, in the case of another Xtext lang. which "overloaded" Xbase, but doesn't have
		 * ANY grammar rules or terminals of it's own (Vorburger's ELang).
		 * 
		 * @return EPackage for nsURI, or null if there was a lookup problem.
		 */
		protected Optional<EPackage> getEPackage(EPackage.Registry registry, String nsURI) {
			try {
				if (registry.containsKey(nsURI))
					return Optional.fromNullable(registry.getEPackage(nsURI));
				else
					return Optional.absent();
			} catch (WrappedException e) {
				return Optional.absent();
			}
		}
	}

}
