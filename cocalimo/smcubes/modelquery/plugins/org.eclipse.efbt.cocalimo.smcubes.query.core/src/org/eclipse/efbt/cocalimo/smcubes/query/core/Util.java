/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *    Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.cocalimo.smcubes.query.core;

import java.io.File;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;



/**
 * This class is responsible for  getting the default BIRD model associated 
 * with any object 
 * 
 * @author Neil Mackenzie
 *
 */
public class Util {

	/**
	 * Tet the default BIRD model associated with any object. This relies on 
	 * the  file layout convention being follwoed correcty.
	 *  
	 * @param o
	 * @return
	 */
	public static SmcubesModel getDefaultBirdModel(EObject o) {
	
		/**ResourceSet rs = o.eResource().getResourceSet();
		    String birdModelXMLFile = o.eResource().getURI().trimSegments(1)
		        + "/smcubes_model.smcubes_model";
		    File file = new File(birdModelXMLFile);
		    URI uri = file.isFile() ? URI.createFileURI(file.getAbsolutePath()) : URI.createURI(birdModelXMLFile);
		    Resource resource = rs.getResource(uri, true);*/
		    return (SmcubesModel) o.eResource().getContents().get(1);
	}

}
