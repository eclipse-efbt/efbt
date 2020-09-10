/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2015 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.resource;

import org.eclipse.emf.common.util.URI;
import org.eclipse.xtext.parser.IEncodingProvider;

import com.google.common.base.Charsets;

public class EFactoryEncodingProvider implements IEncodingProvider {

    @Override
    public String getEncoding(URI uri) {
    	// When Java 7 only (currently ESON is still Java 6 also), use java.nio.charset.StandardCharsets.UTF_8.name() instead of this: 
        return Charsets.UTF_8.name();
    }

}
