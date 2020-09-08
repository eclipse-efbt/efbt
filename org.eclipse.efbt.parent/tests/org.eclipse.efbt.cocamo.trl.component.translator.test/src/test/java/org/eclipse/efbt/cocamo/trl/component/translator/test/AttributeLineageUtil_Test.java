package org.eclipse.efbt.cocamo.trl.component.translator.test;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.eclipse.efbt.cocamo.lineage.model.attribute_lineage.AttributeLineageModel;
import org.eclipse.efbt.cocamo.trl.component.translator.api.AttributeLineageUtil;
import org.eclipse.efbt.cocamo.trl.component.translator.impl.AttributeLineageUtilImpl;
import org.eclipse.efbt.language.trl.model.transformation.VersionedComponentsSet;

public class AttributeLineageUtil_Test {

	 @Test
	    public void exampleTest() {
		 AttributeLineageUtil tester = new AttributeLineageUtilImpl(); // AttributeLineageUtil tests
		 VersionedComponentsSet vcs = null;
		 AttributeLineageModel alm = tester.createAttributeLineageModel(vcs);
	        // assert statements
	        assertEquals(0, 0, "zero equals zero");
	      
	    }
	
}
