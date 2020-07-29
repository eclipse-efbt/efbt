package org.eclipse.efbt.util.csv.provider;

import org.eclipse.efbt.util.csv.CSVUtils;
import org.eclipse.efbt.util.csv.impl.apachecsv.ApacheCSVUtils;

/**
 * Provider for CSVUtils, for now this always provides the implementation of 
 * CSVUtils that uses the Apache Commons CSV library.
 *  
 * @author Neil Mackenzie
 *
 */
public class CSVUtilsProvider {
	
	/**
	 * returns the CSVUtils implementation.
	 */
	public static CSVUtils getCSVUtils()
	{
		return new ApacheCSVUtils();
	}


}
