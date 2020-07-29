package org.eclipse.efbt.util.csv.impl.apachecsv;

import org.apache.commons.csv.CSVRecord;
import org.eclipse.efbt.util.csv.CSVRow;

/**
 * Access to CSV rows using the Apache Commons CSV library.
 * This class wraps a CSV record which is a class from the Apache library.
 * This implements the CSV Row interface which is not library specific..
 * 
 * @author Neil Mackenzie
 *
 */
public class ApacheCSVRow implements CSVRow {
	
	/**
	 * The underlying CSVRecord.
	 */
	CSVRecord underlyingRecord;
	
	/**
	 * Constructor which takes CSVRecord to wrap.
	 * 
	 * @param record
	 */
	public ApacheCSVRow(CSVRecord record)
	{
		underlyingRecord = record;
	}

	@Override
	public String get(int colNum) {
		
		return underlyingRecord.get(colNum);
	}

}
