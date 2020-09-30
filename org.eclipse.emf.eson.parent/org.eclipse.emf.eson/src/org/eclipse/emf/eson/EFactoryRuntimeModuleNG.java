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

import org.eclipse.emf.eson.scoping.EFactoryScopeProviderNG;
import org.eclipse.xtext.scoping.IScopeProvider;

public class EFactoryRuntimeModuleNG extends EFactoryRuntimeModule {

	@Override
	public Class<? extends IScopeProvider> bindIScopeProvider() {
		return EFactoryScopeProviderNG.class;
	}

}
