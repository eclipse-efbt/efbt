/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2002 - 2014 Sebastian Benz, Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.serialization;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.EFactoryServiceProvider;
import org.eclipse.emf.eson.eFactory.EFactoryFactory;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.eFactory.PackageImport;

public class FactoryBuilder implements IFactoryBuilder {

	// TODO the grand re-unificiation of classic Serialization with my new re-syncing..
	// merge this class with the new FactoryBuilder2; the mapping below will become 
	// the same as the mapping in the ModelBuilder
	
	private final EFactoryServiceProvider serviceProvider;
	private Map<EObject, NewObject> mapping = new HashMap<EObject, NewObject>();
	private Factory factory;
	
	public FactoryBuilder(EFactoryServiceProvider serviceProvider) {
		super();
		this.serviceProvider = serviceProvider;
	}

	public Factory buildFactory(EObject eObject) {
		factory = EFactoryFactory.eINSTANCE.createFactory();
		PackageImport packageImport = createPackageImport(eObject);
		factory.getEPackages().add(packageImport);
		
		NewObject root = createNewObject(eObject);
		factory.setRoot(root);
		
		return factory;
	}
	
	public NewObject getOrBuildNewObject(EObject eObject) {
		if (eObject == null)
			throw new IllegalArgumentException("getOrBuildNewObject(null)");

		NewObject newObject = mapping.get(eObject);
		if (newObject == null) {
			newObject = createNewObject(eObject);
			mapping.put(eObject, newObject);
		}
		return newObject;
	}
	
	private NewObject createNewObject(EObject eObject) {
		NewObjectBuilder builder = NewObjectBuilder.context(factory, this);
		NewObject newObject = builder.build(eObject);
		return newObject;
	}
	
	private PackageImport createPackageImport(EObject eObject) {
		PackageImport packageImport = EFactoryFactory.eINSTANCE.createPackageImport();
		packageImport.setEPackage(eObject.eClass().getEPackage());
		return packageImport;
	}

	@Override
	public EFactoryServiceProvider getEFactoryServiceServiceProvider() {
		return serviceProvider;
	}
}
