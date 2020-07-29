package org.eclipse.efbt.util.accessdb;


import java.io.IOException;
import java.util.List;
/**
 * Interface for accessing a MS Access Database.
 * 
 * @author Neil Mackenzie
 *
 */
public interface AccessUtils {
	
	public List<AccessRow> getRowsForTable(String accessDB, String tableName) throws IOException;

}
