package org.eclipse.efbt.data_structures.smcubes.component.importexport.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

//import org.eclipse.efbt.clan.clanStandaloneSetup;
import org.eclipse.efbt.data_structures.smcubes.component.importexport.api.BirdImporter;
import org.eclipse.efbt.data_structures.smcubes.component.importexport.impl.BIRDImporterImpl;
import org.eclipse.efbt.data_structures.smcubes.component.importexport.impl.EFactoryInjectorProvider;
import org.eclipse.efbt.data_structures.smcubes.component.importexport.impl.ESONWithClanmodelInjectorProvider;
import org.eclipse.emf.eson.EFactoryStandaloneSetup;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;




public class ImportExport_Test {

	@Test
    public void exampleTest() {
		
		
		BIRDImporterImpl tester = new BIRDImporterImpl(); // BirdImporter is tested
System.out.println("tester = " + tester.toString());

tester.sayHello2();
	 	String filepath = "C:\\Users\\neil\\BIRD_5.0.2\\BIRD_5_0_2.accdb";  
		String outputFilepath = "C:\\Users\\neil\\output4";
		String testdatafilepath = "C:\\Users\\neil\\output5";
		
		tester.doImport(filepath,outputFilepath,testdatafilepath);
        // assert statements
        assertEquals(0, 0, "zero equals zero");
      
    }
	
}
