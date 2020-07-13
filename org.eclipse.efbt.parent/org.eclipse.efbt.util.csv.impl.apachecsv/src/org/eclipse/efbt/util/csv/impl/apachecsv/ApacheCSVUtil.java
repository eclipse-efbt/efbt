package org.eclipse.efbt.util.csv.impl.apachecsv2;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;


public class ApacheCSVUtil {


	public List<CSVRecord> getCSVRowsFromFile(String fileName) throws IOException {
		// TODO Auto-generated method stub
		 FileReader csvData = new FileReader(new File (fileName));
		 CSVParser parser = CSVParser.parse(csvData,  CSVFormat.EXCEL);
		 List<CSVRecord> list = new ArrayList<CSVRecord>();
		 for (CSVRecord csvRecord : parser) {
			
			 list.add(csvRecord);
		 }
		 return list;

	}

}

