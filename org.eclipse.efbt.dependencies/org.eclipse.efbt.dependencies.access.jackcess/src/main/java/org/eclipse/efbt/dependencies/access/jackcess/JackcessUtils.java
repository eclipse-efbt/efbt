package org.eclipse.efbt.dependencies.access.jackcess;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.healthmarketscience.jackcess.*;

/**
 * class responsable for using the Jackcess library to read in data from an
 * access database
 * 
 * @author Neil Mackenzie
 *
 */
public class JackcessUtils {

	/**
	 * Get the Rows from a Table in an access database
	 * 
	 * @param accessDB
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public List<Row> getRowsForTable(String accessDB, String tableName) throws IOException {

		List<Row> list = new ArrayList<Row>();
		{
			Table table;

			table = DatabaseBuilder.open(new File(accessDB)).getTable(tableName);
			for (Row row : table) {

				list.add(row);
			}

		}

		return list;

	}

}
