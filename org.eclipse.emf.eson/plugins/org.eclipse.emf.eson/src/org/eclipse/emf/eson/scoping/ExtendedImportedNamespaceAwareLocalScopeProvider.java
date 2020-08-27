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
package org.eclipse.emf.eson.scoping;

import java.util.List;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.NamespaceImport;
import org.eclipse.xtext.naming.QualifiedName;
import org.eclipse.xtext.scoping.impl.ImportNormalizer;
import org.eclipse.xtext.scoping.impl.ImportedNamespaceAwareLocalScopeProvider;
import org.eclipse.xtext.util.Strings;

/**
 * ImportedNamespaceAwareLocalScopeProvider to make Xcore "import package.sub.*" and "import package.sub.MyEClass" work.
 * 
 * Required because Xcore does not create multi-Segment Qualified Names for packages.
 * 
 * Tested by org.eclipse.emf.eson.xcore.tests.XcoreTest.
 * 
 * @author Michael Vorburger, based on a tip by IM from Anton Kosyakov
 */
public class ExtendedImportedNamespaceAwareLocalScopeProvider extends ImportedNamespaceAwareLocalScopeProvider {

	@Override
	protected List<ImportNormalizer> internalGetImportedNamespaceResolvers(final EObject context, boolean ignoreCase) {
		List<ImportNormalizer> importedNamespaceResolvers = super.internalGetImportedNamespaceResolvers(context, ignoreCase);

		if (!(context instanceof Factory)) // faster
			return importedNamespaceResolvers;
		Factory factory = (Factory) context;
		
		// copy/paste from super(), the only difference here is that we use our own method instead of super.createImportedNamespaceResolver()
//		EList<EObject> eContents = context.eContents();
		EList<NamespaceImport> eContents = factory.getImports();
		for (NamespaceImport child : eContents) {
			String value = getImportedNamespace(child);
			ImportNormalizer resolver = createImportedNamespaceResolverWithoutSplitting(value, ignoreCase); // sic!
			if (resolver != null)
				importedNamespaceResolvers.add(resolver);
		}
		
		return importedNamespaceResolvers;
	}

	/**
	 * Instead of using qualifiedNameConverter.toQualifiedName() like in the similar super method, we do our own magic here.
	 */
	protected ImportNormalizer createImportedNamespaceResolverWithoutSplitting(final String namespace, boolean ignoreCase) {
		if (Strings.isEmpty(namespace))
			return null;
		boolean hasWildCard = namespace.endsWith(getWildCard()); 
		if (hasWildCard) {
			String namespaceWithoutWildCard = namespace.substring(0, namespace.length() - getWildCard().length() - 1);
			QualifiedName importedNamespace = QualifiedName.create(namespaceWithoutWildCard);			
			return doCreateImportNormalizer(importedNamespace /* do NOT skipLast(1) */, true, ignoreCase);
		} else {
			int lastDotPos = namespace.lastIndexOf('.');
			String baseNamespaceIsEPackage = namespace.substring(0, lastDotPos);
			String lastPartIsEClass = namespace.substring(lastDotPos + 1);
			QualifiedName importedNamespace = QualifiedName.create(baseNamespaceIsEPackage, lastPartIsEClass);
			return doCreateImportNormalizer(importedNamespace, false, ignoreCase);
		}
	}
}
