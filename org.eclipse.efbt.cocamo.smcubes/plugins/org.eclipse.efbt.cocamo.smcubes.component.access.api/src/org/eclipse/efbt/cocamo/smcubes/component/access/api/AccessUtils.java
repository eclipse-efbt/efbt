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
package org.eclipse.efbt.cocamo.smcubes.component.access.api;


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
