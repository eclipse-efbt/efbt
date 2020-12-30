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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.efbt.cocamo.smcubes.component.access.api.AccessRow;
import org.eclipse.efbt.cocamo.smcubes.component.access.api.AccessUtils;
import org.eclipse.efbt.dependencies.access.jackcess.JackcessUtils;

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
