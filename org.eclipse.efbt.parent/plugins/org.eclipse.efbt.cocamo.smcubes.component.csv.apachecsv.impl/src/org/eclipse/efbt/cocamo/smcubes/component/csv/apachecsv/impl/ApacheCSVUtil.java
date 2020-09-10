package org.eclipse.efbt.cocamo.smcubes.component.csv.apachecsv.impl;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * This class is responsable for access to CSV files using the 
 * Apache Commons CSV library.
 * 
 * @author Neil Mackenzie
 *
 */
public class ApacheCSVUtil {

	/**
	 * Gets CSVRecords form a file.
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public List<CSVRecord> getCSVRowsFromFile(String fileName) throws IOException {
	
		File csvData = new File (fileName);
		 CSVParser parser = CSVParser.parse(csvData,StandardCharsets.UTF_8, CSVFormat.EXCEL);
		 List<CSVRecord> list = new ArrayList<CSVRecord>();
		 for (CSVRecord csvRecord : parser) {
			
			 list.add(csvRecord);
		 }
		 return list;

	}

}

