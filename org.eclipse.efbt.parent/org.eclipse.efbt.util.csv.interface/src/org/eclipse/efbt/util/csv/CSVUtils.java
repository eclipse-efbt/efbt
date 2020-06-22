package org.eclipse.efbt.util.csv;

import java.io.IOException;
import java.util.List;

public interface CSVUtils {
	
	public List<CSVRow> getCSVRowsFromFile(String fileName) throws IOException;

}
