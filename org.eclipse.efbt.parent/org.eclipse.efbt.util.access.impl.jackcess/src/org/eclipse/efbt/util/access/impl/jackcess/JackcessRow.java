package org.eclipse.efbt.util.access.impl.jackcess;

import java.util.Date;


import com.healthmarketscience.jackcess.Row;
import org.eclipse.efbt.util.accessdb.AccessRow;
/**
 * A wrapper around a Jackcess Row, the wrapper implements the
 * Access Row interface
 * 
 * @author Neil Mackenzie
 *
 */
public class JackcessRow  implements AccessRow{
	
	/**
	 * The JackcessRow
	 */
	Row underlyingRow;

	/**
	 * Constructor taking a Jackcess Row as input
	 *
	 */
	public JackcessRow(Row row)
	{
		underlyingRow = row;
	}
	
	/**
	 * Gets the String value from a particular column in the row
	 * 
	 * @param columnName
	 */
	public String getString(String columnName) {
		
		return underlyingRow.getString(columnName);
	}

	/**
	 * Gets the integer value from a particular column in the row
	 * 
	 * @param columnName
	 */
	public int getInt(String columnName) {
		
		return underlyingRow.getInt(columnName);
	}

	/**
	 * Gets the date value from a particular column in the row
	 * 
	 * @param columnName
	 */
	public Date getDate(String columnName) {
		
		return underlyingRow.getDate(columnName);
	}

	/**
	 * Gets the boolean value from a particular column in the row
	 * 
	 * @param columnName
	 */
	public boolean getBoolean(String columnName) {
		
		return false;
	}

}
