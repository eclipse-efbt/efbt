package org.eclipse.efbt.util.accessdb;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface AccessUtils {
	
	public List<AccessRow> getRowsForTable(String accessDB, String tableName) throws IOException;
	
	


}
