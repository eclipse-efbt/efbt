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
package org.eclipse.emf.eson;

import org.eclipse.emf.eson.building.NameAccessor;
import org.eclipse.emf.eson.serialization.ValidIDChecker;

import com.google.inject.Inject;

public class EFactoryServiceProvider {

	protected @Inject ValidIDChecker validIDChecker;
	protected @Inject NameAccessor nameAccessor;
	
	public ValidIDChecker getValidIDChecker() {
		return validIDChecker;
	}

	public NameAccessor getNameAccessor() {
		return nameAccessor;
	}
}
