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
package org.eclipse.efbt.model.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EObject;

import column_transformation_logic.AggregateColumnFunction;
import core.VARIABLE;
import functions.BasicFunction;
import functions.BooleanFunction;
import functions.Function;
import functions.MemberParameter;
import functions.Parameter;
import functions.ResolvedCubeColumnParameter;
import functions.ValueParameter;

/**
 * @author Neil Mackenzie
 *
 */
public class ScalaRuntimeUtil {

  /**
   * Returns the text representation of a BasicFunction.
   * 
   * @param calculation
   * @return
   */
  public static String getcalcText(BasicFunction calculation) {

    return getScalaStringForFunction(calculation, false);

  }

  /**
   * Returns the text representation of a BooleanFunction.
   * @param calculation
   * @return
   */
  public static String getRowFunctionText(BooleanFunction calculation) {

    return getScalaStringForFunction(calculation, false);

  }

  /**
   * Returns the set of columns (parameters) required by a BasicFunction.
   * 
   * @param function
   * @return
   */
  public static List<VARIABLE> getRequiredColumns(BasicFunction function) {
    TreeIterator<EObject> contents = function.eAllContents();
    List<VARIABLE> columns = new ArrayList<VARIABLE>();
    while (contents.hasNext()) {
      EObject o = contents.next();
      if (o instanceof ResolvedCubeColumnParameter) {
        VARIABLE param = ((ResolvedCubeColumnParameter) o).getCubeColumn().getVariable();
        columns.add(param);
      }
    }
    return columns;
  }

  /**
   * Returns the column/parameter required by a AggregateColumnFunction.
   * @param function
   * @return
   */
  public static VARIABLE getRequiredColumn(AggregateColumnFunction function) {
    TreeIterator<EObject> contents = function.eAllContents();
    List<VARIABLE> columns = new ArrayList<VARIABLE>();
    while (contents.hasNext()) {
      EObject o = contents.next();
      if (o instanceof ResolvedCubeColumnParameter) {
        VARIABLE param = ((ResolvedCubeColumnParameter) o).getCubeColumn().getVariable();
        columns.add(param);
      }
    }
    return columns.get(0);
  }

  /**
   * Returns the Scala specific String representing the BasicFunction.
   * 
   * @param function
   * @param tempFlag
   * @return
   */
  public static String getScalaStringForFunction(BasicFunction function, boolean tempFlag) {
    String str = "";

    if (function instanceof BasicFunction) {
      BasicFunction bf = ((BasicFunction) function);

      boolean infix = bf.getFunctionSpec().isIsInfix();
      if (infix) {
        if (bf.getFunctionSpec().getFunctionalName().equals("==")) {
          str = str + "(" + getScalaParameterString(function.getParameters().get(1), tempFlag) + ".equals("
              + getScalaParameterString(function.getParameters().get(0), tempFlag) + "))";
        } else if (bf.getFunctionSpec().getFunctionalName().equals("equals")) {
          str = str + "(" + getScalaParameterString(function.getParameters().get(1), tempFlag) + " == "
              + getScalaParameterString(function.getParameters().get(0), tempFlag) + ")";
        } else {
          str = str + "(" + getScalaParameterString(function.getParameters().get(0), tempFlag) + " ";
          if (tempFlag)
            str = str + "funcs.";
          str = str + bf.getFunctionSpec().getFunctionalName() + " "
              + getScalaParameterString(function.getParameters().get(1), tempFlag) + ")";
        }
      } else {
        if (tempFlag)
          str = str + "funcs.";
        if (bf.getFunctionSpec().getName().equals("CopyColumn")) {
          EList<Parameter> theParameters = function.getParameters();
          EList<Parameter> theFirstParameter = new BasicEList<Parameter>();
          theFirstParameter.add(theParameters.get(0));
          str = str + bf.getFunctionSpec().getFunctionalName() + "("
              + scalaStringForParameters(theFirstParameter, tempFlag) + ")";

        } else {
          str = str + bf.getFunctionSpec().getFunctionalName() + "("
              + scalaStringForParameters(function.getParameters(), tempFlag) + ")";
        }
      }
    }

    return str;

  }

  /**
   * Returns the Scala specific String representing the Function.
   * 
   * @param function
   * @param tempFlag
   * @return
   */
  public static String getScalaStringForFunction(Function function, boolean tempFlag) {

    String str = "";

    if (function instanceof BasicFunction) {
      BasicFunction bf = ((BasicFunction) function);

      boolean infix = bf.getFunctionSpec().isIsInfix();
      if (infix) {
        if (bf.getFunctionSpec().getFunctionalName().equals("==")) {
          str = str + "(" + getScalaParameterString(function.getParameters().get(1), tempFlag) + ".equals("
              + getScalaParameterString(function.getParameters().get(0), tempFlag) + "))";
        } else if (bf.getFunctionSpec().getFunctionalName().equals("equals")) {
          str = str + "(" + getScalaParameterString(function.getParameters().get(1), tempFlag) + " == "
              + getScalaParameterString(function.getParameters().get(0), tempFlag) + ")";
        } else {
          str = str + "(" + getScalaParameterString(function.getParameters().get(0), tempFlag) + " ";
          if (tempFlag)
            str = str + "funcs.";
          str = str + bf.getFunctionSpec().getFunctionalName() + " "
              + getScalaParameterString(function.getParameters().get(1), tempFlag) + ")";
        }
      } else {
        if (tempFlag)
          str = str + "funcs.";
        str = str + bf.getFunctionSpec().getFunctionalName() + "("
            + scalaStringForParameters(function.getParameters(), tempFlag) + ")";
      }

    }
    return str;
  }

  /**
   * Returns the Scala specific String representing the parameter.
   *  
   * @param param
   * @param tempFlag
   * @return
   */
  private static String getScalaParameterString(Parameter param, boolean tempFlag) {
    
    String str = "";
    if (param instanceof Function) {
      Function fp = (Function) param;
      str = str + getScalaStringForFunction(fp, tempFlag);
    }
    if (param instanceof ValueParameter) {
      ValueParameter vp = (ValueParameter) param;
      str = str + vp.getValue();

    }
   
    if (param instanceof ResolvedCubeColumnParameter) {
      ResolvedCubeColumnParameter qcp = (ResolvedCubeColumnParameter) param;
      String tableName = null;

      tableName = qcp.getCubeColumn().getCube().getCube_name();

      String colName = qcp.getCubeColumn().getVariable().getVariable_id();

      str = str + tableName + "Def.get" + colName.toLowerCase() + "(" + tableName + ")";

    }
    if (param instanceof MemberParameter) {
      MemberParameter mp = (MemberParameter) param;
      str = str + "\"" + mp.getParam().getName() + "\"";

    }
    return str;

  }

  /**
   * Returns the Scala specific String representing a list of  parameters.
   * @param parameters
   * @param tempFlag
   * @return
   */
  private static String scalaStringForParameters(EList<Parameter> parameters, boolean tempFlag) {
    String str = "";
    Iterator<Parameter> iter = parameters.iterator();
    while (iter.hasNext()) {
      Parameter param = iter.next();
      str = str + getScalaParameterString(param, tempFlag);

      if (iter.hasNext())
        str = str + ",";

    }
    return str;
  }

}
