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
package org.eclipse.emf.eson.resource;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.xtext.resource.DefaultLocationInFileProvider;
import org.eclipse.xtext.util.ITextRegion;

public class EFactoryLocationInFileProvider extends DefaultLocationInFileProvider {

	/** Inverse of org.eclipse.emf.eson.ui.outline.EFactoryOutlineTreeProvider.createNewObjectNode(IOutlineNode, NewObject) */
//	@Override
//	protected ICompositeNode findNodeFor(EObject semanticObject) {
//		EObject newObject = getRealObject(semanticObject);
//		return super.findNodeFor(newObject);
//	}

	@Override
	protected ITextRegion getTextRegion(EObject obj, boolean isSignificant) {
		EObject realEObject = getRealObject(obj);
		if (realEObject == null)
			return ITextRegion.EMPTY_REGION;
		else
			return super.getTextRegion(realEObject, isSignificant);
/*		
		if (obj.eResource() instanceof EFactoryResource) {
			EFactoryResource efactoryResource = (EFactoryResource) obj.eResource();
			EObject eFactoryElement = efactoryResource.getEFactoryElement(obj);
			if (eFactoryElement == null) {
				return new TextRegion(0, 0);
			}
			return super.getTextRegion(eFactoryElement, isSignificant);
		}
		return super.getTextRegion(obj, isSignificant);
*/		
	}
	
	protected EObject getRealObject(EObject semanticObject) {
		if (semanticObject == null)
			return null;
		Resource resource = semanticObject.eResource();
		EFactoryResource efResource = (EFactoryResource) resource;
		NewObject newObject = efResource.getEFactoryNewObject(semanticObject);
		if (newObject != null)
			return newObject;
		else
			return semanticObject;
	}
}
