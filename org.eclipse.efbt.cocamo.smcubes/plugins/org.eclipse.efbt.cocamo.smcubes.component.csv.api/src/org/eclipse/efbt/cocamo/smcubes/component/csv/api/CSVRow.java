package org.eclipse.efbt.cocamo.smcubes.component.csv.api;

/**
 * An interface for describing rows in a CSV file
 * 
 * @author Neil Mackenzie
 *
 */
public interface  CSVRow {
	/**
	 * Get the String value at a particular column in the CSV row.
	 * 
	 * @param colNum
	 * @return
	 */
	public String get(int colNum);
}
