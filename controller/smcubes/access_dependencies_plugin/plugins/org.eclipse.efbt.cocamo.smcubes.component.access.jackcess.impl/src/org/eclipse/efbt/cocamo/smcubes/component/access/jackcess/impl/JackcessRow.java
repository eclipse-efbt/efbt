/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *    Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.cocamo.smcubes.component.access.jackcess.impl;

import java.util.Date;

import org.eclipse.efbt.cocamo.smcubes.component.access.api.AccessRow;

import com.healthmarketscience.jackcess.Row;
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
	public Boolean getBoolean(String columnName) {
		
		return underlyingRow.getBoolean(columnName);
	}

}
