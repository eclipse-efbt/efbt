/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2014 - 2015 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.scoping;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.eFactory.EFactoryPackage;
import org.eclipse.xtext.naming.IQualifiedNameConverter;
import org.eclipse.xtext.naming.IQualifiedNameProvider;
import org.eclipse.xtext.naming.QualifiedName;
import org.eclipse.xtext.util.SimpleAttributeResolver;

import com.google.common.base.Function;
import com.google.inject.Inject;

/**
 * IQualifiedNameProvider for ESON/EFactory.
 * 
 * The most important thing this does is to NOT expose our own internal EFactory
 * objects.. as we want only the "real" derived objects to be in the index
 * (scope) and referenced.
 * 
 * @author Michael Vorburger
 */
public class EFactoryQualifiedNameProvider extends IQualifiedNameProvider.AbstractImpl {
	// NOTE That we do not extend DefaultDeclarativeQualifiedNameProvider, as we don't need its PolymorphicDispatcher

	// TODO evaluate (measure) if using an IResourceScopeCache (OnChangeEvictingCache) here, like DefaultDeclarativeQualifiedNameProvider seems to, makes any noticeable perf. diff?

	// TODO make this more pluggable.. the core logic below (skip our own) should be "core", the actual naming resolution itself should be Guice pluggable?
	public final static String NAME_ATTRIBUTE_NAME = "name"; // as is hard-coded in NAME_RESOLVER
	protected Function<EObject, String> resolver = SimpleAttributeResolver.NAME_RESOLVER;

	protected @Inject IQualifiedNameConverter converter;
	
	@Override
	public final QualifiedName getFullyQualifiedName(EObject obj) {
		if (obj.eClass().getEPackage().equals(EFactoryPackage.eINSTANCE)) {
			return null; // skip!
		};
		return _getFullyQualifiedName(obj);
	}

	protected final QualifiedName _getFullyQualifiedName(EObject obj) {
		EObject temp = obj;
		String name = getResolver().apply(temp);
		if(name == null || name.length() == 0)
			return null;
	
		QualifiedName qualifiedNameFromConverter = converter.toQualifiedName(name);
		while (temp.eContainer() != null) {
			temp = temp.eContainer();
			QualifiedName parentsQualifiedName = getFullyQualifiedName(temp);
			if (parentsQualifiedName != null)
				return parentsQualifiedName.append(qualifiedNameFromConverter);
		}
	
		return qualifiedNameFromConverter;
	}
	
	protected Function<EObject, String> getResolver() {
		return resolver;
	}
}
