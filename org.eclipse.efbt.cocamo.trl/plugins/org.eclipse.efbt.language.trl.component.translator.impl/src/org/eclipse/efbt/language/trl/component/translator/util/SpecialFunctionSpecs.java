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
 *  Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.language.trl.component.translator.util;

import org.eclipse.efbt.cocamo.functions.model.functions.AggregateFunctionSpec;
import org.eclipse.efbt.cocamo.functions.model.functions.BasicFunctionSpec;

/**
 * @author Neil Mackenzie
 *
 */
public class SpecialFunctionSpecs {
  
	/**
	 * the copyColumnSpec
	 */
	public BasicFunctionSpec copyColumnSpec;
	/**
     * the firstColumnSpec
     */
	public AggregateFunctionSpec firstColumnSpec;
	/** 
     * the sumColumnSpec
     */
	public AggregateFunctionSpec sumColumnSpec;	
	/**
     * the equalsSpec
     */
	public BasicFunctionSpec equalsSpec;
	/**
     * the andSpec
     */
	public BasicFunctionSpec andSpec;

}
