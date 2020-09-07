/*
 * #%L
 * org.eclipse.emf.eson.tests.xtextintegration.ui
 * %%
 * Copyright (C) 2016 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.example.properties;

import org.eclipse.emf.eson.example.library.Library;
import org.eclipse.jface.viewers.IFilter;

/**
 * Example Property Section, enabled only for example Library EClass.
 *
 * @author vgirishkumar
 */
public class ExamplePropertyFilter implements IFilter {

    @Override
    public boolean select(Object toTest) {
        if(toTest instanceof Library) {
            return true;
        }
        return false;
    }

}
