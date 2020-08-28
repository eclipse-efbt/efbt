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
package org.eclipse.emf.eson.resource;

import org.eclipse.emf.common.util.URI;
import org.eclipse.xtext.resource.impl.DefaultResourceServiceProvider;

/**
 * An IResourceServiceProvider which ignores resources in the target/ folder.
 * 
 * This is required because in at least one (proprietary, non open-source)
 * product using this, files frequently end up in the Maven target/ directory by
 * local builds, but those should not be picked up by the Xtext Builder Indexing
 * (in this product, the respective Eclipse projects are NOT Java JDT projects,
 * so they don't have an Build output directory).
 * 
 * @author Michael Vorburger
 */
public class EFactoryResourceServiceProvider extends DefaultResourceServiceProvider {

	@Override
	public boolean canHandle(URI uri) {
		if (uri.isPlatform()) {
			if (uri.segmentCount() > 3) {
				if ("target".equals(uri.segment(2))) {
					return false;
				}
			}
		}
		return super.canHandle(uri);
	}

}
