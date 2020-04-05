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
package org.eclipse.efbt.component.common.util;

import java.util.Iterator;

import org.eclipse.emf.common.util.EList;

import functions.AggregateFunction;
import functions.BasicFunction;
import functions.CellsParameter;
import functions.MemberParameter;
import functions.Parameter;
import functions.ResolvedCubeColumnParameter;
import functions.SpeculativeCubeColumnParameter;
import functions.ValueParameter;

/**
 * @author Neil Mackenzie
 *
 */
public class FunctionTextUtil {
	
  /**
   * Return a string representation of a BasicFunction.
   * @param function
   * @return
   */
  public static String getStringForFunction(BasicFunction function) {

    String str = "";

    BasicFunction bf = ((BasicFunction) function);

    boolean infix = bf.getFunctionSpec().isIsInfix();
    if (infix)
      str = str + "(" + getParameterString(function.getParameters().get(0)) + " " + bf.getFunctionSpec().getName()
          + " " + getParameterString(function.getParameters().get(1)) + ")";
    else
      str = str + bf.getFunctionSpec().getName() + "(" + stringForParameters(function.getParameters()) + ")";

    return str;
  }
  
  /**
   * Return a string representation of an AggregateFunction.
   * @param function
   * @return
   */
  public static String getStringForAggregateFunction(AggregateFunction function) {

	  
	  String str = "";
      str = str + function.getFunctionSpec().getName() + "(" + stringForParameters(function.getParameters()) + ")";

    return str;
  }


  /**
   * Return a String representation of a Parameter
   * @param param
   * @return
   */
  private static String getParameterString(Parameter param) {
    String str = "";
    if (param instanceof BasicFunction) {
      BasicFunction basicFunction = (BasicFunction) param;
      str = str + getStringForFunction(basicFunction);
    }

    if (param instanceof ValueParameter) {
      ValueParameter vp = (ValueParameter) param;
      str = str + vp.getValue();

    }

    if (param instanceof SpeculativeCubeColumnParameter) {
      SpeculativeCubeColumnParameter sccp = (SpeculativeCubeColumnParameter) param;
      str = str + sccp.getCube().getCube_name();
      str = str + "." + sccp.getColumn().getVariable_id();

    }
    if (param instanceof ResolvedCubeColumnParameter) {
      ResolvedCubeColumnParameter rccp = (ResolvedCubeColumnParameter) param;
      str = str + rccp.getCubeColumn().getCube().getCube_name();
      str = str + "." + rccp.getCubeColumn().getVariable().getVariable_id() ;

    }
    if (param instanceof MemberParameter) {
      MemberParameter mp = (MemberParameter) param;
      str = str + "'" + mp.getParam().getName() + "'";

    }
    if (param instanceof CellsParameter) {
    	CellsParameter cp = (CellsParameter) param;
        str = str + "'" + cp.getCells().getCellID() + "'";

    }

    return str;

  }

  /**
   * Return a String representation of a list of Parameters.
   * 
   * @param parameters
   * @return
   */
  private static String stringForParameters(EList<Parameter> parameters) {
    String str = "";
    Iterator<Parameter> iter = parameters.iterator();
    while (iter.hasNext()) {
      Parameter param = iter.next();
      str = str + getParameterString(param);

      if (iter.hasNext())
        str = str + ",";

    }
    return str;
  }

  /**
   * Get a strin representation of a function which represents a filter function
   * @param function
   * @return
   */
  public static String getFilterText(BasicFunction function) {

    return getStringForFunction(function);
  }

}
