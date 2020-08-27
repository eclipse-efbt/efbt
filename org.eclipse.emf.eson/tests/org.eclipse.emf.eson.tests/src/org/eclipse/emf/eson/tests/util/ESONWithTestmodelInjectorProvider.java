/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.tests.util;

import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.eson.EFactoryInjectorProvider;
import org.eclipse.emf.eson.tests.util.tests.ESONWithTestmodelInjectorProviderTest;

import com.google.inject.Injector;

import testmodel.TestmodelPackage;
import testmodel.testsubmodel.TestsubmodelPackage;

/**
 * IInjectorProvider for ESON + TestmodelPackage.
 * 
 * This is needed because the approach of just putting
 * TestmodelPackageImpl.init() into the @Before of a *Test doesn't quite work
 * reliably, it works for one @Test but not several, because "*PackageImpl
 * init() will not always put itself into the registry. It does that only on
 * first call. IRegistryConfigurator . That will create a copy of the registries
 * before each test and set that state back after the test has run." Using @BeforeClass
 * works as well, but completely defeats the purpose.
 * 
 * @see ESONWithTestmodelInjectorProviderTest
 * 
 * @author Michael Vorburger, based on advise from Sven Efftinge & Anton Kosyakov
 */
public class ESONWithTestmodelInjectorProvider extends EFactoryInjectorProvider {

	@Override
	protected Injector internalCreateInjector() {
	    // Needed because.. XtextRunner & *InjectorProvider & *StandaloneSetup
	    // stuff due to GlobalStateMemento in restoreGlobalState() will cause a
	    // confusing mess with hard-to-reproduce test failures if we don't do this.  
	    // To reproduce the original problem, create a JUnit Suite of 1. XcoreTest,
	    // 2. EcoreUtil3Test, 3. SimplestTest (in this exact order!), and you'll see.
        if (!EPackage.Registry.INSTANCE.containsKey(TestmodelPackage.eNS_URI))
            EPackage.Registry.INSTANCE.put(TestmodelPackage.eNS_URI, TestmodelPackage.eINSTANCE);
        if (!EPackage.Registry.INSTANCE.containsKey(TestsubmodelPackage.eNS_URI))
            EPackage.Registry.INSTANCE.put(TestsubmodelPackage.eNS_URI, TestsubmodelPackage.eINSTANCE);

		// NORMALLY TestmodelPackage.eINSTANCE.getTestModel(); // NOT TestmodelPackageImpl.init();
	    //  ^^^^^ but this doesn't really work reliably, so needs above

		return super.internalCreateInjector();
	}

}
