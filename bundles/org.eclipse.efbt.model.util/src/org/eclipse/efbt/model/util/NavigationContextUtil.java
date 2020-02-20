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
 *  Neil Mackenzie - initial API and implementation
 *  */

package org.eclipse.efbt.model.util;

import java.io.File;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;


import navigation_context.NavigationContext;


/**
 * @author Neil Mackenzie
 *
 */
public class NavigationContextUtil {

  

 

 

 

  

  /**
   * Returns the DefaultNavigationContext
   * @param o
   * @return
   */
  public static NavigationContext getDefaultNavigationContext(EObject o) {
    ResourceSet rs = o.eResource().getResourceSet();
    String tagsXMLFile = o.eResource().getURI().trimSegments(1)
        + "//defaultNavigationContext.navigation_context";
    File file = new File(tagsXMLFile);
    URI uri = file.isFile() ? URI.createFileURI(file.getAbsolutePath()) : URI.createURI(tagsXMLFile);
    Resource resource = rs.getResource(uri, true);
    return (NavigationContext) resource.getContents().get(0);

  }

}
