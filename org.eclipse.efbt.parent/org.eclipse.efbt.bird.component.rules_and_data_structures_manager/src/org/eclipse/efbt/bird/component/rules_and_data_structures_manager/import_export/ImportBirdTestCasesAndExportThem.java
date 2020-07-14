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

public class ImportBirdTestCasesAndExportThem {

	public static void main (String[] args)
	{
		BIRDImporter importer = new BIRDImporter();
		importer.filepath = args[0];
		importer.outputFilepath = args[1];
		importer.testdatafilepath = args[2];
		String testdataOutputfilepath = args[3];
		//importer.filepath = "C:\\freebirdtools-master2\\ws\\org.eclipse.efbt.regmodules.bird\\bird\\";
		importer.doImport();
		importer.createAortaFiles();
		importer.importTestData();
		importer.persistAortaFiles();
		
		List<Program> testPrograms = importer.testPrograms;
		InputDataExporter exporter = new InputDataExporter();
		
		for (Iterator iterator = testPrograms.iterator(); iterator.hasNext();) {
			Program program = (Program) iterator.next();
			exporter.exportInputData(program, testdataOutputfilepath);
			
		}
		
		Set<Entry<ColumnStructuredEntity, FileWriter>> fileWriters = exporter.fileHashMap.entrySet();
		for (Iterator iterator = fileWriters.iterator(); iterator.hasNext();) {
			Entry<ColumnStructuredEntity, FileWriter> entry = (Entry<ColumnStructuredEntity, FileWriter>) iterator
					.next();
			try {
				entry.getValue().flush();
				entry.getValue().close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	
	}
}
