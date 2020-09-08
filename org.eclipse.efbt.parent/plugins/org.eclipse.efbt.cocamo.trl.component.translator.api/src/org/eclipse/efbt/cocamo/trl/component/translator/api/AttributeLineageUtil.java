
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
 *  Neil Mackenzie - initial API and interfaceementation
 *  */

package org.eclipse.efbt.cocamo.trl.component.translator.api;

import org.eclipse.efbt.cocamo.lineage.model.attribute_lineage.AttributeLineageModel;
import org.eclipse.efbt.language.trl.model.transformation.VersionedComponentsSet;



/**
 * @author Neil Mackenzie
 *
 */
public interface AttributeLineageUtil {

  

/**
 * Creates an attributeLineageModel according to the details of the VersionedComponentsSet
 * 
 * 
 * @param vcs
 */
public  AttributeLineageModel createAttributeLineageModel(VersionedComponentsSet vcs );


}

