package org.eclipse.efbt.bird.component.rules_and_data_structures_manager.import_export;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.eclipse.efbt.aorta.component.exporter.InputDataExporter;
import org.eclipse.efbt.component.birdartefactscreator.importer.birdaccessdb.BIRDImporter;

import org.eclipse.efbt.aorta.aorta_program.Program;
import org.eclipse.efbt.aorta.column_structures.ColumnStructuredEntity;

/**
 * This class is responsible for importing tests in a particular csv format,
 * creating tests from these which are instances of the Aorta Program Ecore
 * model, and then exporting them  in another CSV format.
 * 
 * Note that the initail CSV format for test data has one row for every column
 * It is a very long list of rows in one file. The New CSV file format for
 * exporting the test data is one csv file per cube, where each csv file
 * has one column per cube column (so many columns) and then one row for each
 * row of test data for a cube.
 * 
 * @author Neil Mackenzie
 *
 */
public class ImportBirdTestCasesAndExportThem {

	/**
	 * The main class responsible for retrieving input arguments, calling the
	 * method to import tests from csv into instances  of the Aorta Program Ecore 
	 * model, and then calling the methofd to export them to another csv format.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		BIRDImporter importer = new BIRDImporter();
		
		//get the filePath of the BIRD Access Database which allows us to 
		//understand the context of the tests (types of columns,
		//which columns are in which cubes etc.)
		importer.filepath = args[0];
		//get the output filepath where we export the xml representation of the
		//Aorta Program ecore model
		importer.outputFilepath = args[1];		
		//get the  filepath where we input  the test data from
		importer.testdatafilepath = args[2];
		//get the output filepath where we export the test data in the new 
		//CSV format
		String testdataOutputfilepath = args[3];

		//input the tests data as model instances of Aorta Program
		importer.doImport();
		// create the model instances as EObjects, particularly storing the 
		//structure of cubes
		importer.createAortaFiles();
		//import  the actual test data into the model instances
		importer.importTestData();
		//create the XML files that represent the model instances
		importer.persistAortaFiles();

		List<Program> testPrograms = importer.testPrograms;
		
		InputDataExporter exporter = new InputDataExporter();

		//export the test data into the New CSV format
		for (Iterator iterator = testPrograms.iterator(); iterator.hasNext();) 
		{
			Program program = (Program) iterator.next();
			exporter.exportInputData(program, testdataOutputfilepath);

		}

		Set<Entry<ColumnStructuredEntity, FileWriter>> fileWriters = 
				exporter.fileHashMap.entrySet();
		for (Iterator iterator = fileWriters.iterator(); iterator.hasNext();)
		{
			Entry<ColumnStructuredEntity, FileWriter> entry = 
					(Entry<ColumnStructuredEntity, FileWriter>) iterator
					.next();
			try {
				entry.getValue().flush();
				entry.getValue().close();
			} catch (IOException e) {
			
				e.printStackTrace();
			}

		}

	}
}
