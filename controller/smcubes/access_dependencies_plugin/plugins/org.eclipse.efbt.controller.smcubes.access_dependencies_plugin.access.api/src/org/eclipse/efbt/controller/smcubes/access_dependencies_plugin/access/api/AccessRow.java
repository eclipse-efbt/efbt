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
package org.eclipse.efbt.controller.smcubes.access_dependencies_plugin.access.api;

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
	public Boolean getBoolean(String columnName);

}
