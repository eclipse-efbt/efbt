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

import org.eclipse.xtext.ecore.EcoreSupportStandaloneSetup;

import com.google.inject.Injector;

/**
 * Injector Provider for integration tests with dynamic Ecore.
 *
 * While ESON supports Xcore, note that there is a test using this, the main
 * (no-test) ESON does NOT include Xtext's Ecore support, which indexes *.ecore,
 * due to perf concern, to avoid full scan of all *.ecore.  Should that ever be
 * of interest, then the dependency to the org.eclipse.xtext.ecore should be
 * made in core org.eclipse.emf.eson instead of only in org.eclipse.emf.eson.tests,
 * and a component = org.eclipse.xtext.ecore.EcoreSupport { } would have to be added
 * to the GenerateEFactory.mwe2.
 * 
 * @author Michael Vorburger
 */
public class ESONWithTestmodelAndDynamicECoreInjectorProvider extends EFactoryInjectorProviderNG // NOT ESONWithTestmodelInjectorProvider, we do NOT want TestmodelPackageImpl.init() in this scenario  
{	// We're intentionally extends EFactoryInjectorProviderNG, note *NG, which is the variant with the new simplified Scope provider
	
	@Override
	protected Injector internalCreateInjector() {
		EcoreSupportStandaloneSetup.setup();
		return super.internalCreateInjector();
	}
}
