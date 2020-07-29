package org.eclipse.efbt.util.csv.impl.apachecsv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVRecord;
import org.eclipse.efbt.util.csv.CSVRow;
import org.eclipse.efbt.util.csv.CSVUtils;

/**
 * A class for accessing CSV files, using the Apache Commons CSV library
 * as the underlying implementations.
 * This class implements the more  CSVUtils interface which is not
 * library specific.
 * 
 * @author Neil Mackenzie
 *
 */
public class ApacheCSVUtils implements CSVUtils {

	@Override
	public List<CSVRow> getCSVRowsFromFile(String fileName) throws IOException {

		ApacheCSVUtil cu = new ApacheCSVUtil();
		List<CSVRecord> csvRecordList = cu.getCSVRowsFromFile(fileName);

		List<CSVRow> list = new ArrayList<CSVRow>();
		for (CSVRecord csvRecord : csvRecordList) {
			CSVRow row = new ApacheCSVRow(csvRecord);
			list.add(row);
		}
		return list;

	}

}
