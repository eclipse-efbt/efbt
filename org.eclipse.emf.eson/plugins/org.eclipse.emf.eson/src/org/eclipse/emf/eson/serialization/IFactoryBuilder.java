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
package org.eclipse.emf.eson.serialization;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.EFactoryServiceProvider;
import org.eclipse.emf.eson.eFactory.NewObject;

/**
 * FactoryBuilder can turn an EObject into a NewObject.
 * 
 * Maybe the NewObject already existed and is just being found and returned,
 * or maybe an entirely new one is being created.
 * 
 * @author Michael Vorburger
 */
public interface IFactoryBuilder {

	NewObject getOrBuildNewObject(EObject eObject);

	EFactoryServiceProvider getEFactoryServiceServiceProvider();

}
