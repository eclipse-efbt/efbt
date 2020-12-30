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
package org.eclipse.efbt.controller.smcubes.component.access.provider;

import org.eclipse.efbt.controller.smcubes.access_dependencies_plugin.access.api.AccessUtils;
import org.eclipse.efbt.controller.smcubes.access_dependencies_plugin.jackcess.impl.JackcessUtil;


/**
 * Provider for AccessUtils, for now this always provides the Jackcess 
 * implementation of AccessUtils.
 *  
 * @author Neil Mackenzie
 *
 */
public class AccessUtilProvider {
	
	/**
	 * returns the AccessUtils implementation
	 */
	public static AccessUtils getAccessUtils()
	{
		return new JackcessUtil();
	}

}
