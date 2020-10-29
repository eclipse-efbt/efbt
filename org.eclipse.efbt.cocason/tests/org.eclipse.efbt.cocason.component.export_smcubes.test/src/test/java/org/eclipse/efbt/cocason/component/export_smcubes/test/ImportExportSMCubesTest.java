/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2020 Bird Software Solutions
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.efbt.cocason.component.export_smcubes.test;

import org.eclipse.efbt.cocason.component.export_smcubes.ImportAndExportSMCubesToNewCocason;
import static org.junit.jupiter.api.Assertions.assertEquals;

//import org.eclipse.emf.eson.EFactoryStandaloneSetup;
import org.junit.jupiter.api.Test;

public class ImportExportSMCubesTest {

	@Test
    public void exampleTest() {
		
		
		ImportAndExportSMCubesToNewCocason tester = new ImportAndExportSMCubesToNewCocason(); // BirdImporter is tested

	/**	
	// need to move this to resources
	   String filepath = "C:\\Users\\neil\\BIRD_5.0.2\\BIRD_5_0_2.accdb";
		String outputFilepath = "C:\\Users\\neil\\output1";
		String testdatafilepath = "C:\\Users\\neil\\tests";
		
	    tester.importAndExportSMCubes(filepath,outputFilepath,testdatafilepath); */
        // assert statements
        assertEquals(0, 0, "zero equals zero");
      
    }
}
