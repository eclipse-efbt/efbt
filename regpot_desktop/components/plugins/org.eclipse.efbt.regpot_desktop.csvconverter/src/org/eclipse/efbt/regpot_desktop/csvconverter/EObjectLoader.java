package org.eclipse.efbt.regpot_desktop.csvconverter;

/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDE-License-Identifier: EPL-2.0
 *
 * Contributors:
 *    Neil Mackenzie - initial API and implementation
 *******************************************************************************/
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Map;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;

public class EObjectLoader {
	
	public static ResourceSet resSet  = new ResourceSetImpl();
	
	public static boolean resourceFactoryInitialised = false;

	public static EObject loadObject(String fullFileURI) {
		if (!resourceFactoryInitialised) {
			initialiseREsourceFactory();
		}
		
		Resource resource1  = resSet.getResource(URI.createFileURI(fullFileURI), true);
		return loadObject(resource1);
	}
	
	public static EObject findTableInXMIFile(EClassifier eClassifier, String resourceURI) {
		try{
			Resource resource1  = EObjectLoader.resSet.getResource(URI.createFileURI(resourceURI + '/' + eClassifier.getName() +".xmi"), true);
			return loadObject(resource1);
		}
		catch (Exception e) {
			return null;
		}
		
	}
	
	private static EObject loadObject(Resource resource1) {
		

		EObject returnVal = null;
		
		try{
		resource1.load(Collections.EMPTY_MAP);
		returnVal = resource1.getContents().get(0);
		}
		catch (Exception e) {
			
		}

		return returnVal;
	}

	private static void initialiseREsourceFactory() {
		Resource.Factory.Registry reg = Resource.Factory.Registry.INSTANCE;
		Map<String, Object> m = reg.getExtensionToFactoryMap();
		m.put("xmi", new XMIResourceFactoryImpl());
		resourceFactoryInitialised = true;
	}

}
