package org.eclipse.efbt.cocamo.smcubes.component.csv.provider;

import org.eclipse.efbt.cocamo.smcubes.component.csv.apachecsv.impl.ApacheCSVUtils;
import org.eclipse.efbt.cocamo.smcubes.component.csv.api.CSVUtils;

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
