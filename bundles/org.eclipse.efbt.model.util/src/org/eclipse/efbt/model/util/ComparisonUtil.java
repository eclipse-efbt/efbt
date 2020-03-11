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

import java.util.Iterator;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;

import attribute_lineage.AttributeLineageModel;
import attribute_lineage.Attribute_lineageFactory;
import column_transformation_logic.AggregateColumnFunction;

import column_transformation_logic.ColumnFunction;

import column_transformation_logic.CubeColumn;
import column_transformation_logic.StandardBasicColumnFunction;
import cube_transformation_logic.RowFunction;
import cubes.TargetCube;
import functions.ResolvedCubeColumnParameter;
import platform_call.CompareAttributeLineageModels;
import platform_call.GetAttributeLineageModel;
import platform_call.IsNotSubset;
import platform_call.IsSubset;
import platform_call.Platform_callFactory;
import row_transformation_logic.BaseRowStructure;
import row_transformation_logic.ColumnFunctionGroup;
import row_transformation_logic.FunctionalRowLogic;


/**
 * @author Neil Mackenzie
 *
 */
public class ComparisonUtil {

	 /**
	   * Compares 2 AttributeLineageModels, the call has a firstModel and secondModel attribute
	   * For for every FunctionalRowLogic, BaseRowStucture and CubeColumn in the first model,
	   * we find it in the resulting model, and set usedInSubSet for those  items only.
	   * 
	   * @param call
	   */
	  public static void compareAttributeLineageModels(CompareAttributeLineageModels call) {
	    // resulting model is a copy of second model
	    AttributeLineageModel resultingModel = EcoreUtil.copy(call.getSecondModel());
	    AttributeLineageModel notIncludedModel = Attribute_lineageFactory.eINSTANCE.createAttributeLineageModel();
	    boolean isSubset = true;
	    
	    // ensure that usedInSubSet is set to false initially for everything on second model    
	    setUsedInSubsetToFalseForAll(resultingModel);
	    // for everything in first model, find it in the resulting model, and set usedInSubSet
	    // to be true for that thing, find may need a special compare method.
	    // if it cannot be found add it to the not included model.
	    EList<FunctionalRowLogic> rowTransformations = call.getFirstModel().getRowTransformations();
	    for (Iterator iterator = rowTransformations.iterator(); iterator.hasNext();) {
	      FunctionalRowLogic functionalRowLogic = (FunctionalRowLogic) iterator.next();
	      FunctionalRowLogic frl2 = AttributeLineageUtil.findFunctionalRowLogicInAttributeModel(functionalRowLogic, resultingModel);
	      FunctionalRowLogic frl2Copy = EcoreUtil.copy(frl2);
	      if (frl2 == null) {
	        notIncludedModel.getRowTransformations().add(EcoreUtil.copy(functionalRowLogic));
	        isSubset = false;
	      } else {
	        frl2.setUsedInSubsets(true);
	        // set useIntrails for columns in resultingModel which are also in first model
	        EList<ColumnFunction> columns = functionalRowLogic.getColumnFunctionGroup().getColumnFunctions();
	        for (Iterator iterator2 = columns.iterator(); iterator2.hasNext();) {
	          ColumnFunction columnFunction = (ColumnFunction) iterator2.next();
	          ColumnFunction columnFunction2 = AttributeLineageUtil.findColumnFunctionInFunctionalRowLogic(columnFunction, frl2);
	          if (columnFunction2 == null) {
	            if (!notIncludedModel.getRowTransformations().contains(frl2Copy)) {

	              frl2Copy.getColumnFunctionGroup().getColumnFunctions().clear();
	              notIncludedModel.getRowTransformations().add(frl2Copy);
	              isSubset = false;
	            }

	            frl2Copy.getColumnFunctionGroup().getColumnFunctions().add(EcoreUtil.copy(columnFunction));

	          } else
	            columnFunction2.setUsedInSubsets(true);

	        }

	      }

	    }

	    EList<BaseRowStructure> baseSchema = call.getFirstModel().getBaseSchemas();
	    for (Iterator iterator = baseSchema.iterator(); iterator.hasNext();) {
	      BaseRowStructure baseRowStructure = (BaseRowStructure) iterator.next();
	      BaseRowStructure baseRowStructure2 = AttributeLineageUtil.findBaseRowStructureInAttributeModel(baseRowStructure, resultingModel);
	      BaseRowStructure baseRowStructure2Copy = EcoreUtil.copy(baseRowStructure2);
	      if (baseRowStructure2 == null) {
	        notIncludedModel.getBaseSchemas().add(EcoreUtil.copy(baseRowStructure));
	        isSubset = false;
	      } else {
	        baseRowStructure2.setUsedInSubsets(true);
	        EList<CubeColumn> columns = baseRowStructure.getColumns();
	        for (Iterator iterator2 = columns.iterator(); iterator2.hasNext();) {
	          CubeColumn column = (CubeColumn) iterator2.next();
	          CubeColumn column2 = AttributeLineageUtil.findColumninSchema(column, baseRowStructure2);
	          if (column2 == null) {
	            if (!notIncludedModel.getBaseSchemas().contains(baseRowStructure2Copy)) {

	              baseRowStructure2Copy.getColumns().clear();
	              notIncludedModel.getBaseSchemas().add(baseRowStructure2Copy);
	              isSubset = false;
	            }

	            baseRowStructure2Copy.getColumns().add(EcoreUtil.copy(column));

	          } else {
	            column2.setUsedInSubsets(true);
	          }

	        }

	      }
	    }
	    if (isSubset) {
	      IsSubset subset = Platform_callFactory.eINSTANCE.createIsSubset();
	      call.setSubsetBoolean(subset);
	    } else {
	      IsNotSubset subset = Platform_callFactory.eINSTANCE.createIsNotSubset();
	      call.setSubsetBoolean(subset);
	    }

	    call.setNotIncludedModel(notIncludedModel);
	    call.setResultingModel(resultingModel);
	  }
	  
	  /**
	   * Set usedInSubset equal to true for every FunctionalRowLogic, BaseRowStucture and CubeColumn
	   * in the  model 
	   * @param model
	   */
	  private static void setUsedInSubsetToFalseForAll(AttributeLineageModel model) {

	    EList<FunctionalRowLogic> rowTransformations = model.getRowTransformations();
	    for (Iterator iterator = rowTransformations.iterator(); iterator.hasNext();) {
	      FunctionalRowLogic functionalRowLogic = (FunctionalRowLogic) iterator.next();
	      functionalRowLogic.setUsedInSubsets(false);
	      EList<ColumnFunction> columns = functionalRowLogic.getColumnFunctionGroup().getColumnFunctions();
	      for (Iterator iterator2 = columns.iterator(); iterator2.hasNext();) {
	        ColumnFunction columnFunction = (ColumnFunction) iterator2.next();
	        columnFunction.setUsedInSubsets(false);
	      }
	    }

	    EList<BaseRowStructure> baseSchema = model.getBaseSchemas();
	    for (Iterator iterator3 = baseSchema.iterator(); iterator3.hasNext();) {
	      BaseRowStructure baseRowStructure = (BaseRowStructure) iterator3.next();
	      baseRowStructure.setUsedInSubsets(false);
	      EList<CubeColumn> columns = baseRowStructure.getColumns();
	      for (Iterator iterator4 = columns.iterator(); iterator4.hasNext();) {
	        CubeColumn column = (CubeColumn) iterator4.next();
	        column.setUsedInSubsets(false);
	      }
	    }
	  }
 
  /**
   * For all dependent items of a TargetColumn, set isUsedInSubset to true.
   * 
   * @param attributeLineageModel
   * @param targetCubes
   * @param call
   */
  static void markAttributeLineageComponentsAsUsedInSubSet(AttributeLineageModel attributeLineageModel, Iterator<TargetCube> targetCubes,
       GetAttributeLineageModel call) {

    while (targetCubes.hasNext()) {
      TargetCube targetCube = targetCubes.next();
      FunctionalRowLogic rowLogic = AttributeLineageUtil.getFunctionalRowLogicForCube(targetCube, attributeLineageModel);

      // get the columns
      Iterator<ColumnFunction> columns = rowLogic.getColumnFunctionGroup().getColumnFunctions().iterator();
      while (columns.hasNext()) {
        ColumnFunction column = columns.next();
        markColumnAsUsedInSubset(column,  attributeLineageModel, call);
      }

    }
  }

  /**
   *  * For all dependent items of a column,   set isUsedInSubset to true.
   * 
   * @param column
   * @param attributeLineageModel
   * @param call
   */
  private static void markColumnAsUsedInSubset(CubeColumn column, 
      AttributeLineageModel attributeLineageModel, GetAttributeLineageModel call) {

    EObject o = column.eContainer();
    column.setUsedInSubsets(true);
    if (o instanceof RowFunction) {
      setDependantColsFromRowFunctionAsUsedInSubset((RowFunction) o, attributeLineageModel, call);

    }
    if (o instanceof ColumnFunctionGroup) {
      FunctionalRowLogic frl = (FunctionalRowLogic) o.eContainer();
      frl.setUsedInSubsets(true);
      setDependantColsFromRowFunctionAsUsedInSubset(frl.getCubeLogic().getRowFunction(), attributeLineageModel, call);
      ColumnFunction func = (ColumnFunction) column;
      if (func instanceof StandardBasicColumnFunction) {

        
          // get all the resolved cube columns and set as used
          TreeIterator<EObject> allContents = func.eAllContents();
          while (allContents.hasNext()) {
            o = allContents.next();
            if (o instanceof ResolvedCubeColumnParameter) {
              markColumnAsUsedInSubset(((ResolvedCubeColumnParameter) o).getCubeColumn(),  attributeLineageModel, call);
            }
          
     
        }
      }
      if (func instanceof AggregateColumnFunction) {

        // get all the resolved cube columns and set as used
        TreeIterator<EObject> allContents = func.eAllContents();
        while (allContents.hasNext()) {
          o = allContents.next();
          if (o instanceof ResolvedCubeColumnParameter) {
            markColumnAsUsedInSubset(((ResolvedCubeColumnParameter) o).getCubeColumn(),  attributeLineageModel, call);
          }

        }

      }
    

    }
    if (o instanceof BaseRowStructure) {
      ((BaseRowStructure) o).setUsedInSubsets(true);
    }

  }
  
  /**
   * Set the DependantCols From a RowFunction As UsedInSubset
   * 
   * @param rowFunction
   * @param attributeLineageModel
   * @param call
   */
  private static void setDependantColsFromRowFunctionAsUsedInSubset(RowFunction rowFunction, 
     
      AttributeLineageModel attributeLineageModel,
      GetAttributeLineageModel call) {
 
    Iterator<ResolvedCubeColumnParameter> dependantCubeCols = ((RowFunction) rowFunction).getDependantCubeColumns().iterator();

    while (dependantCubeCols.hasNext()) {
      ResolvedCubeColumnParameter tc = dependantCubeCols.next();
      markColumnAsUsedInSubset(tc.getCubeColumn(),  attributeLineageModel, call);

    }

  }
  

  

  /**
   * Deletes every FunctionalRowLogic, BaseRowStucture and CubeColumn in the 
   * attributeLineageModel which does not have usedInSubset equal to  true.
   * @param attributeLineageModel
   */
  static void trimComponentsMarkedInTrail(AttributeLineageModel attributeLineageModel) {
    EList<BaseRowStructure> baseRowStructures = attributeLineageModel.getBaseSchemas();
    EList<BaseRowStructure> baseRowStructuresToRemove = new BasicEList<BaseRowStructure>();
    for (Iterator iterator = baseRowStructures.iterator(); iterator.hasNext();) {
      BaseRowStructure baseRowStructure = (BaseRowStructure) iterator.next();
      if (!baseRowStructure.isUsedInSubsets())
        baseRowStructuresToRemove.add(baseRowStructure);
      else {
        EList<CubeColumn> columnsToRemove = new BasicEList<CubeColumn>();
        EList<CubeColumn> baseRowColumns = baseRowStructure.getColumns();
        for (Iterator iterator2 = baseRowColumns.iterator(); iterator2.hasNext();) {
          CubeColumn cubeColumn = (CubeColumn) iterator2.next();
          if (!cubeColumn.isUsedInSubsets())
            columnsToRemove.add(cubeColumn);

        }
        removeColumnsFrowBaseRowStructure(baseRowStructure, columnsToRemove);

      }

    }
    removeBaseRowStructuresFromModel(attributeLineageModel, baseRowStructuresToRemove);

    EList<FunctionalRowLogic> functionalRowLogics = attributeLineageModel.getRowTransformations();
    BasicEList<FunctionalRowLogic> functionalRowLogicsToRemove = new BasicEList<FunctionalRowLogic>();
    for (Iterator iterator = functionalRowLogics.iterator(); iterator.hasNext();) {
      {
        FunctionalRowLogic functionalRowLogic = (FunctionalRowLogic) iterator.next();
        if (!functionalRowLogic.isUsedInSubsets())
          functionalRowLogicsToRemove.add(functionalRowLogic);
        else {
          EList<ColumnFunction> columnFuncsToRemove = new BasicEList<ColumnFunction>();
          EList<ColumnFunction> columnfuncs = functionalRowLogic.getColumnFunctionGroup().getColumnFunctions();
          for (Iterator iterator2 = columnfuncs.iterator(); iterator2.hasNext();) {
            ColumnFunction columnFunction = (ColumnFunction) iterator2.next();
            if (!columnFunction.isUsedInSubsets())
              columnFuncsToRemove.add(columnFunction);

          }
          removeColumnfunctionsFromFunctionalRowLogic(functionalRowLogic, columnFuncsToRemove);
        }
      }
    }
    removeFunctionalRowLogicsFromModel(attributeLineageModel, functionalRowLogicsToRemove);
  }

  /**
   * Remove a FunctionalRowLogic from an AttributeModel.
   * 
   * @param attributeLineageModel
   * @param functionalRowLogicsToRemove
   */
  private static void removeFunctionalRowLogicsFromModel(AttributeLineageModel attributeLineageModel,
      BasicEList<FunctionalRowLogic> functionalRowLogicsToRemove) {

    for (Iterator iterator = functionalRowLogicsToRemove.iterator(); iterator.hasNext();) {
      FunctionalRowLogic functionalRowLogic = (FunctionalRowLogic) iterator.next();
      attributeLineageModel.getRowTransformations().remove(functionalRowLogic);
    }

  }

  /**
   * Remove  a ColumnFunction from a functionalRowLogic item.
   * 
   * @param functionalRowLogic
   * @param columnFuncsToRemove
   */
  private static void removeColumnfunctionsFromFunctionalRowLogic(FunctionalRowLogic functionalRowLogic,
      EList<ColumnFunction> columnFuncsToRemove) {

    for (Iterator iterator = columnFuncsToRemove.iterator(); iterator.hasNext();) {
      ColumnFunction columnFunction = (ColumnFunction) iterator.next();
      functionalRowLogic.getColumnFunctionGroup().getColumnFunctions().remove(columnFunction);

    }

  }

  /**
   * Removes a BaseRowStructure from an AttributeLineageModel.
   * 
   * @param attributeLineageModel
   * @param baseRowStructuresToRemove
   */
  private static void removeBaseRowStructuresFromModel(AttributeLineageModel attributeLineageModel, EList<BaseRowStructure> baseRowStructuresToRemove) {

    for (Iterator iterator = baseRowStructuresToRemove.iterator(); iterator.hasNext();) {
      BaseRowStructure baseRowStructure = (BaseRowStructure) iterator.next();
      attributeLineageModel.getBaseSchemas().remove(baseRowStructure);
    }

  }
  

  /**
   * Removes a Column from a BaseRowStructure.
   * 
   * @param baseRowStructure
   * @param columnsToRemove
   */
  private static void removeColumnsFrowBaseRowStructure(BaseRowStructure baseRowStructure, EList<CubeColumn> columnsToRemove) {

    for (Iterator iterator = columnsToRemove.iterator(); iterator.hasNext();) {
      CubeColumn cubeColumn = (CubeColumn) iterator.next();
      baseRowStructure.getColumns().remove(cubeColumn);
    }

  }


 



}
