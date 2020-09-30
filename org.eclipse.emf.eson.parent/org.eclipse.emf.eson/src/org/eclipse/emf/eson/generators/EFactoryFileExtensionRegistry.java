/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2016 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.generators;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.Platform;

/**
 * Registry to handle custom Eson Extension.
 *
 * @author vgirishkumar
 */
public class EFactoryFileExtensionRegistry {

	private static EFactoryFileExtensionRegistry _factory = null;

	public static EFactoryFileExtensionRegistry getRegistry() {
		if (_factory == null) {
			_factory = new EFactoryFileExtensionRegistry();
		}
		return _factory;
	}

	private final Set<String> associateFileExtensions;

	private EFactoryFileExtensionRegistry() {
		associateFileExtensions = initializeRegistry();
	}

	private Set<String> initializeRegistry() {
		Set<String> _associateFileExtensions = new HashSet<String>();
		IConfigurationElement[] config = Platform.getExtensionRegistry()
				.getConfigurationElementsFor(
						"org.eclipse.emf.eson.file");

		for (IConfigurationElement e : config) {
			String extensions = e.getAttribute("extensions");
			String extensions_withoutspace = extensions.replaceAll("\\s", "");
			_associateFileExtensions.addAll(Arrays.asList(extensions_withoutspace.split(",")));
		}

		return Collections.unmodifiableSet(_associateFileExtensions);
	}

	public Set<String> getAssociateFileExtensions() {
		return associateFileExtensions;
	}

	public Set<String> getAssociateFileExtensionsWithDefaults() {
		HashSet<String> hashSet = new HashSet<String>(
				getAssociateFileExtensions());
		hashSet.add("coca");
		
		return hashSet;
	}
}
