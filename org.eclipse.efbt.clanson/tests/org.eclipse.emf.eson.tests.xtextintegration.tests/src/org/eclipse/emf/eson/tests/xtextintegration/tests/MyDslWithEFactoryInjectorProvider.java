/*
 * #%L
 * org.eclipse.emf.eson.tests.xtextintegration.tests
 * %%
 * Copyright (C) 2013 - 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.tests.xtextintegration.tests;

import org.eclipse.core.runtime.Platform;
import org.eclipse.emf.ecore.EcorePackage;
import org.eclipse.emf.eson.EFactoryStandaloneSetup;
import org.eclipse.xtext.junit4.GlobalRegistries;
import org.eclipse.xtext.junit4.GlobalRegistries.GlobalStateMemento;
import org.eclipse.xtext.junit4.IInjectorProvider;
import org.eclipse.xtext.junit4.IRegistryConfigurator;

import testModelWithXtextReference.TestModelWithXtextReferencePackage;

import com.google.inject.Injector;
import org.eclipse.emf.eson.xtextintegration.MyDslStandaloneSetup;

public class MyDslWithEFactoryInjectorProvider implements IInjectorProvider, IRegistryConfigurator {

    protected GlobalStateMemento stateBeforeInjectorCreation;
	protected GlobalStateMemento stateAfterInjectorCreation;
	protected Injector myDslInjector;
	protected Injector eFactoryInjector;

	static {
		GlobalRegistries.initializeDefaults();
	}

	public Injector getInjector() {
		if (myDslInjector == null) {
			if (!Platform.isRunning()) {
				EcorePackage.eINSTANCE.toString();
				TestModelWithXtextReferencePackage.eINSTANCE.toString();
			}
			stateBeforeInjectorCreation = GlobalRegistries.makeCopyOfGlobalState();
			this.myDslInjector = new MyDslStandaloneSetup().createInjectorAndDoEMFRegistration();
			this.eFactoryInjector = new EFactoryStandaloneSetup().createInjectorAndDoEMFRegistration();
			stateAfterInjectorCreation = GlobalRegistries.makeCopyOfGlobalState();
		}
		return myDslInjector;
	}

	public void setupRegistry() {
		getInjector();
		stateAfterInjectorCreation.restoreGlobalState();
	}

	public void restoreRegistry() {
		stateBeforeInjectorCreation.restoreGlobalState();
	}

}
