/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.tests.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.xtext.util.SimpleAttributeResolver;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public final class Find {

	public static <T> T child(final Class<T> candidateClass, final String candidateName, EObject rootObject) {
		final TreeIterator<EObject> contents = rootObject.eAllContents();
//		if (!contents.hasNext()) {
//			throw new IllegalArgumentException();
//		}
		EObject result = Iterators.find(contents,
				new Predicate<EObject>() {

					public boolean apply(EObject input) {
						return candidateClass.isInstance(input)
								&& (candidateName == null 
									|| candidateName.equals(SimpleAttributeResolver.NAME_RESOLVER.apply(input)));
					}
				});
		return candidateClass.cast(result);
	}

	public static <T extends EObject> Iterator<T> allInResourceSet(EObject context, Class<T> type) {
		Iterator<EObject> contentIterator = getResourceSetIterator(context);
		return Iterators.filter(contentIterator, type);
	}

	private static Iterator<EObject> getResourceSetIterator(EObject context) {
		Iterator<EObject> result = Iterators.emptyIterator();
		Resource resource = context.eResource();
		if (resource == null) {
			result = getRootObjectIterator(context);
		} else if (resource.getResourceSet() == null) {
			result = resource.getAllContents();
		} else {
			result = getResourceSetIterator(resource.getResourceSet());
		}
		return result;
	}
	private static Iterator<EObject> getResourceSetIterator(ResourceSet resourceSet) {
		Iterator<EObject> result = Iterators.emptyIterator();
		for (Resource resource : resourceSet.getResources()) {
			result = Iterators.concat(result, resource.getAllContents());
		}
		return result;
	}

	private static Iterator<EObject> getRootObjectIterator(EObject context) {
		EObject rootContainer = EcoreUtil.getRootContainer(context);
		Iterator<EObject> result = Iterators.singletonIterator(rootContainer);
		return Iterators.concat(result, rootContainer.eAllContents());
	}

	public static <T extends EObject> List<T> allAncestors(EObject root,
			Class<T> type) {
		List<T> result = new ArrayList<T>();
		Iterator<EObject> iterator = root.eAllContents();
		while (iterator.hasNext()) {
			EObject ancestor = iterator.next();
			if (type.isInstance(ancestor)) {
				result.add(type.cast(ancestor));
			}
		}
		return result;
	}
}
