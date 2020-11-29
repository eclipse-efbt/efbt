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
package org.eclipse.efbt.language.trl.component.translator.test;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.eclipse.efbt.language.trl.component.translator.api.AttributeLineageUtil;
import org.eclipse.efbt.language.trl.component.translator.impl.AttributeLineageUtilImpl;
import org.eclipse.efbt.language.trl.model.transformation.VersionedComponentsSet;
import org.eclipse.efbt.lineage.attributelineage.model.attribute_lineage.AttributeLineageModel;

public class AttributeLineageUtil_Test {

	 @Test
	    public void exampleTest() {
		 AttributeLineageUtil tester = new AttributeLineageUtilImpl(); // AttributeLineageUtil tests
		 VersionedComponentsSet vcs = null;
	//	 AttributeLineageModel alm = tester.createAttributeLineageModel(vcs);
	        // assert statements
	        assertEquals(0, 0, "zero equals zero");
	      
	    }
	
}
