package org.eclipse.efbt.cocamo.smcubes.component.csv.api;

import java.io.IOException;
import java.util.List;

/**
 * An interface for decribing access to CSV files.
 * 
 * @author Neil Mackenzie
 *
 */
public interface CSVUtils {
	
	/**
	 * get the rows from a CSV file.
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public List<CSVRow> getCSVRowsFromFile(String fileName) throws IOException;

}
