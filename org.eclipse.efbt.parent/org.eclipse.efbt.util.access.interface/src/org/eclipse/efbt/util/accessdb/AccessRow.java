package org.eclipse.efbt.util.accessdb;

import java.util.Date;

/**
 * Interface describing Rows in an Access Database
 * @author Neil Mackenzie
 *
 */
public interface AccessRow {
	
	/**
	 * Gets the String value from a particular column in the row
	 * 
	 * @param columnName
	 * @return
	 */
	public String getString(String columnName);
	
	/**
	 * Gets the integer value from a particular column in the row
	 * 
	 * @param columnName
	 * @return
	 */
	public int getInt(String columnName);

	/**
	 * Gets the date value from a particular column in the row
	 * 
	 * @param columnName
	 * @return
	 */
	public Date getDate(String columnName);

	/**
	 * Gets the boolean value from a particular column in the row
	 * 
	 * @param columnName
	 * @return
	 */
	public boolean getBoolean(String columnName);

}
