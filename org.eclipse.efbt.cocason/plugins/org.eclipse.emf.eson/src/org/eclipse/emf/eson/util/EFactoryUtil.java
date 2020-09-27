/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz, Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.util;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.PackageImport;

public final class EFactoryUtil {

	/**
	 * Gets EPackages from the 'use' clause.
	 */
	public Iterable<EPackage> getEPackages(Factory factory) {
		List<PackageImport> ePackageUris = factory.getEPackages();
		List<EPackage> result = new ArrayList<EPackage>(ePackageUris.size());
		for (PackageImport packageImport : ePackageUris) {
			EPackage ePackage = packageImport.getEPackage();
			if (ePackage != null)
				result.add(ePackage);
		}
		return result;
	}
}
