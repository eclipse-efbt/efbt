/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.resource;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.xtext.resource.XtextResource;
import org.eclipse.xtext.resource.XtextResourceFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;

public class EFactoryResourceFactory extends XtextResourceFactory {
	
	@Inject
	private Provider<EFactoryResource> efactoryResourceProvider;

	@Inject
	public EFactoryResourceFactory(Provider<XtextResource> resourceProvider) {
		super(resourceProvider);
	}

	@Override
	public Resource createResource(URI uri) {
		EFactoryResource resource = efactoryResourceProvider.get();
		resource.setURI(uri);
		return resource;
	}
}
