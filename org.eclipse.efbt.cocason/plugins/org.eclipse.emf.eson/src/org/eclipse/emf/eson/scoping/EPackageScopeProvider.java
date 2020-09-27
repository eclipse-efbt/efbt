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

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.util.EFactoryUtil;
import org.eclipse.emf.eson.util.EcoreUtil3;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.scoping.IScope;
import org.eclipse.xtext.scoping.Scopes;
import org.eclipse.xtext.scoping.impl.SimpleScope;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

public class EPackageScopeProvider implements IEPackageScopeProvider {

	@Inject
	private EFactoryUtil eFactoryUtil;

	public IScope createEClassScope(Resource resource, EClass type, IScope parent) {
		Iterable<EPackage> ePackages = resolvePackages(resource);
		Iterable<EClass> eClasses = getAllEClasses(ePackages);
		if (type != null)
			eClasses = filterAssignableEClasses(eClasses, type);
		Iterable<IEObjectDescription> scopedElements = Scopes.scopedElementsFor(eClasses, DottedQualifiedNameFixer.FUNCTION);
		return new SimpleScope(parent, scopedElements);
	}

	public IScope createEClassScope(Resource resource, IScope parent) {
		return createEClassScope(resource, null, parent);
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
		return eFactoryUtil.getEPackages(root);
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
