package org.eclipse.efbt.util.csv.impl.apachecsv;

import org.apache.commons.csv.CSVRecord;
import org.eclipse.efbt.util.csv.CSVRow;

public class ApacheCSVRow implements CSVRow {
	
	CSVRecord underlyingRecord;
	
	public ApacheCSVRow(CSVRecord record)
	{
		underlyingRecord = record;
	}

	@Override
	public String get(int colNum) {
		// TODO Auto-generated method stub
		return underlyingRecord.get(colNum);
	}

}
