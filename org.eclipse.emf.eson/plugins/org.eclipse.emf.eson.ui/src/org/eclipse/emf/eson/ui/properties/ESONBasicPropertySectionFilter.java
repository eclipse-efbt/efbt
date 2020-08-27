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
package org.eclipse.emf.eson.ui.properties;

import java.util.Set;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.ui.EFactoryUiExtensionRegistry;
import org.eclipse.emf.eson.util.EcoreUtil3;
import org.eclipse.jface.viewers.IFilter;

/**
 * Filter for Property Sheet tab for Custom Property to enabled.
 *
 * @see ESONBasicPropertySection for some documentation
 *
 * @author vgirishkumar
 */
public class ESONBasicPropertySectionFilter implements IFilter {

    @Override
    public boolean select(Object toTest) {
        Set<String> customPropertyClassNames = EFactoryUiExtensionRegistry.getRegistry().getFilteredBasicPropertySectionEClassNames();
        if (toTest instanceof EObject) {
            EClass toTestEClass = ((EObject) toTest).eClass();
            String toTestEClassName = EcoreUtil3.getFullyQualifiedName(toTestEClass);
            if (customPropertyClassNames.contains(toTestEClassName)) {
                return false;
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

}
