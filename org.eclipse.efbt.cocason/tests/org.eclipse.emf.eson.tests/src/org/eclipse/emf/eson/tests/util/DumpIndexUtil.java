/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2015 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.tests.util;

import java.util.List;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.xtext.resource.IContainer;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.resource.IResourceDescription;
import org.eclipse.xtext.resource.IResourceDescriptions;
import org.eclipse.xtext.resource.IResourceServiceProvider;
import org.eclipse.xtext.resource.impl.ResourceSetBasedResourceDescriptions;

/**
 * Test helper to dump the Xtext index.
 * Can sometimes be useful for debugging.
 * 
 * @author Michael Vorburger
 */
public class DumpIndexUtil {
	private DumpIndexUtil() {}
	
	private static final IResourceServiceProvider.Registry registry = IResourceServiceProvider.Registry.INSTANCE;
	
	public static void dumpXtextIndex(Resource r) {
		new DumpIndexUtil()._dumpXtextIndex(r);
	}
	
	public void _dumpXtextIndex(Resource r) {
		List<IContainer> visibleContainers = getVisibleContainers(r);
		for (IContainer visibleContainer : visibleContainers) {
			for (IResourceDescription rd : visibleContainer.getResourceDescriptions()) {
				System.out.println(rd.getURI());
				Iterable<IEObjectDescription> descs = rd.getExportedObjects();
				for (IEObjectDescription desc : descs) {
					System.out.print("   ");
					System.out.print(desc.getQualifiedName().toString("::"));
					System.out.print(" (");
					System.out.print(desc.getEClass().getName());
					System.out.print(") => ");
					System.out.println(desc.getEObjectURI());
				}
			}
		}
		
	}
	
	protected List<IContainer> getVisibleContainers(Resource resource) {
		// we normalize the URI in order to properly retrieve its Xtext resource description
		URI normalizedURI = resource.getResourceSet().getURIConverter().normalize(resource.getURI());
		
		IResourceServiceProvider rsp = getResourceServiceProvider(resource);
		IResourceDescriptions index = rsp.get(IResourceDescriptions.class);
		
		if (index instanceof ResourceSetBasedResourceDescriptions) {
			ResourceSetBasedResourceDescriptions resourceSetBasedResourceDescriptions = (ResourceSetBasedResourceDescriptions) index;
			if (resourceSetBasedResourceDescriptions.getResourceSet() == null) {
				// This seems to be needed in some cases - incl. from standalone (SE) tests
				resourceSetBasedResourceDescriptions.setContext(resource);
			}
		}
		IResourceDescription descr = index.getResourceDescription(normalizedURI);
		if (descr == null) {
			throw new IllegalArgumentException("The resource ["+normalizedURI+"] has no resource description (Xtext)");
		}

		IContainer.Manager containerManager = rsp.getContainerManager();
		List<IContainer> visibleContainers = containerManager.getVisibleContainers(descr, index);
		
		return visibleContainers;
	}

	protected IResourceServiceProvider getResourceServiceProvider(Resource resource) {
		URI uri = resource.getURI();
		IResourceServiceProvider rsp = registry.getResourceServiceProvider(uri);
		if (rsp == null)
			throw new IllegalArgumentException("No IResourceServiceProvider available in Registry for URI: " + uri);
		return rsp;
	}

}
