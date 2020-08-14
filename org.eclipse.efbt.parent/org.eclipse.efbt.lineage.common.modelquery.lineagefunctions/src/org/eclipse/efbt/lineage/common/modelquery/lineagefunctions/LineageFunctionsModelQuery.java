package org.eclipse.efbt.lineage.common.modelquery.lineagefunctions;

import java.util.Iterator;

import org.eclipse.efbt.common.model.functions.AggregateFunction;
import org.eclipse.efbt.common.model.functions.BasicFunction;
import org.eclipse.efbt.common.model.functions.Parameter;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.MemberParameter;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.ResolvedCubeColumnParameter;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter;
import org.eclipse.efbt.lineage.common.model.lineagefunctions.ValueParameter;
//import org.eclipse.efbt.data_structures.smcubes.model.core.VARIABLE;
//import org.eclipse.efbt.data_structures.smcubes.model.core.MEMBER;
import org.eclipse.emf.common.util.EList;



public class LineageFunctionsModelQuery {
	
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

	  //  if (param instanceof SpeculativeCubeColumnParameter) {
	  //    SpeculativeCubeColumnParameter sccp = (SpeculativeCubeColumnParameter) param;
	  //    str = str + sccp.getCube().getName();
	  //    str = str + "." + sccp.getColumn().getVariable_id();

	 //   }
	 //   if (param instanceof ResolvedCubeColumnParameter) {
	 //     ResolvedCubeColumnParameter rccp = (ResolvedCubeColumnParameter) param;
	 //     str = str + rccp.getCubeColumn().getCube().getName();
	//      str = str + "." + rccp.getCubeColumn().getVariable().getVariable_id() ;
//
	//    }
//	    if (param instanceof MemberParameter) {
//	      MemberParameter mp = (MemberParameter) param;
//	      str = str + "'" + mp.getParam().getName() + "'";

//	    }  
	    //need to reinstate below
	  //  if (param instanceof CellsParameter) {
	  //  	CellsParameter cp = (CellsParameter) param;
	   //     str = str + "'" + cp.getCells().getCellID() + "'";
//
	  //  }

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
