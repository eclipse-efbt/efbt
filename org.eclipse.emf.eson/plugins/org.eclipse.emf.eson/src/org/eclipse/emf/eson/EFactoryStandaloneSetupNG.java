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
package org.eclipse.emf.eson;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class EFactoryStandaloneSetupNG extends EFactoryStandaloneSetup {

	@Override
	public Injector createInjector() {
		return Guice.createInjector(new EFactoryRuntimeModuleNG());
	}
	
}
