/*
 * #%L
 * org.eclipse.emf.eson
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

import org.eclipse.emf.eson.EFactoryInjectorProvider;
import org.eclipse.emf.eson.EFactoryStandaloneSetupNG;

import com.google.inject.Injector;

public class EFactoryInjectorProviderNG extends EFactoryInjectorProvider {

	@Override
	protected Injector internalCreateInjector() {
	    return new EFactoryStandaloneSetupNG().createInjectorAndDoEMFRegistration();
	}

}
