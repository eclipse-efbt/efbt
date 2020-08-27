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
package org.eclipse.emf.eson.resource;

import org.apache.log4j.Logger;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.building.ModelBuilder;
import org.eclipse.emf.eson.building.ModelBuilderException;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.xtext.resource.DerivedStateAwareResource;

import com.google.common.base.Optional;
import com.google.inject.Inject;

public class EFactoryResource extends DerivedStateAwareResource {
	private static final Logger logger = Logger.getLogger(EFactoryDerivedStateComputer.class);

	@Inject private ModelBuilder builder;
	
	public boolean isBuilt() {
		return getBuilder().isBuilt();
	}
	
	public @Nullable NewObject getEFactoryNewObject(EObject eObject) {
		if (builder == null) {
			return null;
		}
		if (!builder.isBuilt())
			return null;
		try {
			return builder.getSource(eObject);
		} catch (ModelBuilderException e) {
			return null;
		}
	}

	public @NonNull NewObject getExistingEFactoryNewObject(EObject eObject) {
		NewObject newObject = getEFactoryNewObject(eObject);
		if (newObject == null) {
			throw new IllegalArgumentException(eObject.toString());
		}
		return newObject ;
	}
	
	@SuppressWarnings("null")
	public @NonNull Optional<EObject> getEFactoryEObject(NewObject nObject) throws ModelBuilderException {
		ModelBuilder _builder = getBuilder();
		if (_builder.isBuilt())
			return _builder.getBuilt(nObject);
		else
			return Optional.absent();
	}

	// package-private, as only used by EFactoryDerivedStateComputer
	@NonNull ModelBuilder getBuilder() throws IllegalStateException {
		// written in this weird style just to satisfy Eclipse' slightly dumb null check
		final ModelBuilder _builder = builder;
		if (_builder != null) {
			return _builder;
		} else {
			throw new IllegalStateException("EFactoryResource is missing @Injected ModelBuilder?!");
		}
	}
	
	public @Nullable org.eclipse.emf.eson.eFactory.Factory getEFactoryFactory() {
		if (getContents().isEmpty()) {
			return null;
		}
		return (org.eclipse.emf.eson.eFactory.Factory) getContents().get(0);
	}

	public void putEObjectNewObjectPair(EObject eObject, NewObject newObject) {
		builder.putEObjectNewObjectPair(eObject, newObject);
	}

	public static @Nullable EObject getEFactoryEObject(Resource r) {
		if (!(r instanceof EFactoryResource)) {
			logger.warn("EMF Resource is not an EFactoryResource (caller should not even have invoked this method): " + r.getURI().toString());
			return null;
		}
		final EFactoryResource eFactoryResource = (EFactoryResource) r;
		if (!eFactoryResource.isBuilt())
			return null;
		org.eclipse.emf.eson.eFactory.Factory factory = eFactoryResource.getEFactoryFactory();
		if (factory == null)
			return null;
		try {
			return eFactoryResource.getEFactoryEObject(factory.getRoot()).orNull();
		} catch (ModelBuilderException e) {
			return null;
		}
	}

	public static @Nullable <T> T getEFactoryEObject(Resource r, Class<T> clazz) {
		EObject object = getEFactoryEObject(r);
		if (object == null)
			return null;
		if (!clazz.isInstance(object)) {
			final URI rURI = object.eResource().getURI();
			throw new IllegalArgumentException("EObject built by EFactory in resource '"
					+ rURI + "' is of type '"
					+ object.getClass().getName() + "' and not '"
					+ clazz.getName() + "'");
		}
		return clazz.cast(object);
	}
}
