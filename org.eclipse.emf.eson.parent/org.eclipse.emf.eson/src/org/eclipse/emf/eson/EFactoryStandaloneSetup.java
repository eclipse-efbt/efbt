/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2013 - 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson;

/**
 * Initialization support for running Xtext languages 
 * without equinox extension registry
 */
public class EFactoryStandaloneSetup extends EFactoryStandaloneSetupGenerated {

	// TODO move this helper class into new TBD *.standalone project
	
	public static void doSetup() {
		new EFactoryStandaloneSetup().createInjectorAndDoEMFRegistration();
	}
}
