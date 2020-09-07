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
package org.eclipse.emf.eson.building;

import org.apache.log4j.Logger;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.xtext.EcoreUtil2;

import com.google.common.base.Optional;

public class NewObjectExtensions {
	private static Logger logger = Logger.getLogger(NewObjectExtensions.class);

	// see also org.eclipse.emf.eson.serialization.ContainmentBuilder.nullifyEClassIfDefault(NewObject)
	
	public Optional<EClass> getDeclaredOrInferredEClass(NewObject newObject) {
		EClass eClass = newObject.getEClass();
		
		if (eClass == null) {
			Feature newObjectFeature = EcoreUtil2.getContainerOfType(newObject, Feature.class);
			if (newObjectFeature != null) {
				EStructuralFeature eFeature = newObjectFeature.getEFeature();
				if (!(eFeature instanceof EReference))
					return Optional.absent();
				EReference eReference = (EReference) eFeature; 
				eClass = eReference.getEReferenceType();
			} else {
				// Do NOT logger.error here!
				return Optional.absent();
			}
		}
		if (eClass.getEPackage() == null) {
			EcoreUtil.resolve(newObject.eClass(), newObject);
		}
		if (eClass.eIsProxy()) {
			if (logger.isDebugEnabled())
				logger.debug("The EClass for NewObject " + getNewObjectDescriptionForErrorMessage(newObject) + " is still an unresolved EMF Proxy, something isn't working in your cross-Resource reference resolution");
			return Optional.absent();
		}
		EPackage ePackage = eClass.getEPackage();
		if (ePackage == null) {
			logger.error("No EPackage registered for EClass '" + eClass.getName() + "' defined in NewObject " + getNewObjectDescriptionForErrorMessage(newObject));
			return Optional.absent();
		}
		
		return Optional.of(eClass);
	}
	
	protected String getNewObjectDescriptionForErrorMessage(NewObject newObject) {
		return "name '" + newObject.getName()
            + "' at URI " + newObject.eResource().getURI()
            + "#" + newObject.eResource().getURIFragment(newObject);
	}

}
