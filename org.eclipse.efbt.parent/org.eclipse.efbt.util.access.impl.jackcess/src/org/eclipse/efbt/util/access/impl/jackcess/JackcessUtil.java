package org.eclipse.efbt.util.access.impl.jackcess;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.efbt.dependencies.access.jackcess.JackcessUtils;
import org.eclipse.efbt.util.accessdb.AccessRow;
import org.eclipse.efbt.util.accessdb.AccessUtils;

import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.DatabaseBuilder;

/**
 * A Utils class for accessing access databases using Jackess. Implements the
 * AccessUtils interface.
 * 
 * @author Neil Mackenzie
 *
 */
public class JackcessUtil implements AccessUtils {

	/**
	 * Gets rows from the access database for a particulr table
	 * 
	 * @param accessDB
	 * @param tableName
	 */
	public List<AccessRow> getRowsForTable(String accessDB, String tableName) 
			throws IOException {

		JackcessUtils ju = new JackcessUtils();
		List<Row> a = ju.getRowsForTable(accessDB, tableName);

		List<AccessRow> list = new ArrayList<AccessRow>();

		for (Iterator iterator = a.iterator(); iterator.hasNext();) {
			Row row = (Row) iterator.next();

			AccessRow accessRow = new JackcessRow(row);
			list.add(accessRow);

		}

		return list;

	}

}
