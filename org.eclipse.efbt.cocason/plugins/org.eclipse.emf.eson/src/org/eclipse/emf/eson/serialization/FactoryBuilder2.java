/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2013 - 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.serialization;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.EFactoryServiceProvider;
import org.eclipse.emf.eson.building.ModelBuilder;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.resource.EFactoryResource;

/**
 * FactoryBuilder based on an EFactoryResource (which in turn will delegate to
 * its ModelBuilder).
 * 
 * @see FactoryBuilder
 * @see ModelBuilder
 * 
 * @author Michael Vorburger
 */
public class FactoryBuilder2 implements IFactoryBuilder {
	// TODO indented as temporary, to be re-unified with the original
	// FactoryBuilder in the planned grand re-unificiation of classic
	// Serialization with my new re-syncing..
	
	private final EFactoryResource resource;
	private final EFactoryServiceProvider serviceProvider;

	public FactoryBuilder2(EFactoryResource resource, EFactoryServiceProvider serviceProvider) {
		if (resource == null)
			throw new IllegalArgumentException("FactoryBuilder2(null)");
		this.resource = resource;
		this.serviceProvider = serviceProvider;
	}

	public NewObject getOrBuildNewObject(EObject eObject) {
		if (eObject == null)
			throw new IllegalArgumentException("getOrBuildNewObject(null)");
		
		// we cannot rely on eObject.eResource(), because programmatically newly added objects
		// might not be in our EFactoryResource, yet.  But if the EObject does have a Resource,
		// it better be ours.  (Or are there scenarios where this doesn't make sense?)
		if (eObject.eResource() != null && eObject.eResource() != resource) {
			throw new IllegalArgumentException("getOrBuildNewObject(eObject) eResource is not ours");
		}
		
		NewObject newObject = resource.getEFactoryNewObject(eObject);
		if (newObject == null) {
			newObject = createNewObject(eObject);
			resource.putEObjectNewObjectPair(eObject, newObject);
		}
		return newObject;
	}

	protected NewObject createNewObject(EObject eObject) {
		Factory factory = resource.getEFactoryFactory();
		NewObjectBuilder builder = NewObjectBuilder.context(factory, this);
		NewObject newObject = builder.build(eObject);
		return newObject;
	}

	@Override
	public EFactoryServiceProvider getEFactoryServiceServiceProvider() {
		return serviceProvider;
	}

}
