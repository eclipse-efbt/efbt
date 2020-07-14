package org.eclipse.efbt.util.csv.provider;

import org.eclipse.efbt.util.csv.CSVUtils;
import org.eclipse.efbt.util.csv.impl.apachecsv.ApacheCSVUtils;

public class CSVUtilsProvider {
	
	public static CSVUtils getCSVUtils()
	{
		return new ApacheCSVUtils();
	}


}
