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
package org.eclipse.emf.eson.ui;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.Platform;
import org.eclipse.emf.eson.generators.EFactoryExtensionRegistry;

/**
 * Registry to handle custom Property Sheet Entries.
 *
 * @see EFactoryExtensionRegistry for a similar utility (TODO unify?)
 *
 * @author vgirishkumar
 */
public class EFactoryUiExtensionRegistry {

    private static EFactoryUiExtensionRegistry _factory = null;

    public static EFactoryUiExtensionRegistry getRegistry() {
        if (_factory == null) {
            _factory = new EFactoryUiExtensionRegistry();
        }
        return _factory;
    }

    private final Set<String> filteredBasicPropertySectionEClassNames;

    private EFactoryUiExtensionRegistry() {
        filteredBasicPropertySectionEClassNames = initializeRegistry();
    }

    private Set<String> initializeRegistry() {
        Set<String>  _filteredBasicPropertySectionEClassNames = new HashSet<String>();
        IConfigurationElement[] config = Platform.getExtensionRegistry().getConfigurationElementsFor("org.eclipse.emf.eson.ui.properties");

        for (IConfigurationElement e : config) {
            String type = e.getAttribute("eClass");
            _filteredBasicPropertySectionEClassNames.add(type);
        }
        
        return Collections.unmodifiableSet(_filteredBasicPropertySectionEClassNames);
    }

    public Set<String> getFilteredBasicPropertySectionEClassNames() {
        return filteredBasicPropertySectionEClassNames;
    }
}
