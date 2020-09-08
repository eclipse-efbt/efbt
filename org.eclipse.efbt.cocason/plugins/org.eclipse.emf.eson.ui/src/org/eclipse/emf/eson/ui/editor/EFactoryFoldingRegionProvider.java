/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2010 - 2014 Sebastian Benz, Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.editor;

import org.eclipse.xtext.ui.editor.folding.DefaultFoldingRegionProvider;

public class EFactoryFoldingRegionProvider extends DefaultFoldingRegionProvider {

	// @Override
	// protected List<IFoldingRegion> doGetFoldingRegions(
	// IXtextDocument xtextDocument, XtextResource xtextResource) {
	// List<IFoldingRegion> foldingRegions = Lists.newArrayList();
	// if (xtextResource instanceof EFactoryResource) {
	// EFactoryResource eFactoryResource = (EFactoryResource) xtextResource;
	// Factory factory = eFactoryResource.getFactory();
	// if (factory != null) {
	// xtextResource = (XtextResource) factory.eResource();
	// }
	// }
	// Iterator<EObject> allContents = xtextResource.getAllContents();
	// while (allContents.hasNext()) {
	// EObject eObject = allContents.next();
	// if (isHandled(eObject)) {
	// addFoldingRegions(xtextDocument, eObject, foldingRegions);
	// }
	// }
	// return foldingRegions;
	// }

}
