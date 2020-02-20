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
 *  */

package org.eclipse.efbt.model.util;

import java.util.Iterator;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;

import attribute_lineage.AttributeLineageModel;
import column_transformation_logic.ColumnFunction;
import column_transformation_logic.CubeColumn;
import cube_transformation_logic.FunctionalCubeLogic;
import cube_transformation_logic.RowFunction;
import cube_transformation_logic.UnionRowFunction;
import cubes.FreeBirdToolsCube;
import cubes.DerivedCube;
import row_transformation_logic.BaseRowStructure;
import row_transformation_logic.FunctionalRowLogic;

/**
 * @author Neil Mackenzie
 *
 */
public class AttributeLineageUtil {

  

  /**
   * This method finds the FunctionalRowLogic within the attributeLineageModel which
   * is associated with a particular cube.
   * 
   * @param cube
   * @param attributeLineageModel
   * @return
   */
  static FunctionalRowLogic getFunctionalRowLogicForCube(FreeBirdToolsCube cube, AttributeLineageModel attributeLineageModel) {
   
    Iterator<FunctionalRowLogic> rowLogicIter = attributeLineageModel.getRowTransformations().iterator();
    FunctionalRowLogic returnFunctionalRowLogic = null;
    while (rowLogicIter.hasNext()) {
      FunctionalRowLogic rowLogic = rowLogicIter.next();
      FreeBirdToolsCube rowlogicCube = rowLogic.getCubeLogic().getCube();
      if (rowlogicCube.equals(cube)) {
        returnFunctionalRowLogic = rowLogic;
      }
    }
    return returnFunctionalRowLogic;
  }

  /**
   * This method finds the BaseRowStructure within the attributeLineageModel which
   * is associated with a particular cube.
   * 
   * @param cube
   * @param attributeLineageModel
   * @return
   */
  private static BaseRowStructure getBaseRowStructureWithTable(FreeBirdToolsCube cube, AttributeLineageModel attributeLineageModel) {

    Iterator<BaseRowStructure> rowStructureIter = attributeLineageModel.getBaseSchemas().iterator();
    BaseRowStructure returnRowStructure = null;
    while (rowStructureIter.hasNext()) {
      BaseRowStructure rowStructure = rowStructureIter.next();
      FreeBirdToolsCube rowStructureCube = rowStructure.getCube();
      if (rowStructureCube.equals(cube)) {
        returnRowStructure = rowStructure;
      }
    }
    return returnRowStructure;
  }
  
  /**
   * Returns the BaseRowStrucures which the functionalRowLogic depends upon.
   * @param functionalRowLogic
   * @return
   */
  public static EList<BaseRowStructure> getTheDependantBaseRowStructures(FunctionalRowLogic functionalRowLogic) {

    BasicEList<BaseRowStructure> dependantBaseRowStructures = new BasicEList<BaseRowStructure>();
    AttributeLineageModel attributeModel = (AttributeLineageModel) functionalRowLogic.eContainer();
   
    FreeBirdToolsCube cube = functionalRowLogic.getCubeLogic().getCube();
    EList<FreeBirdToolsCube> dependentDerivedCubes = ((DerivedCube) cube).getSourceCubes();
    Iterator<FreeBirdToolsCube> dependantDerivedCubesIter = dependentDerivedCubes.iterator();
    while (dependantDerivedCubesIter.hasNext()) {
      FreeBirdToolsCube derivedCube = dependantDerivedCubesIter.next();
      Iterator<BaseRowStructure> baseRowsStructureIter = attributeModel.getBaseSchemas().iterator();
      FreeBirdToolsCube baseCube = null;
      while (baseRowsStructureIter.hasNext()) {
        BaseRowStructure baseRowStructure = baseRowsStructureIter.next();

        baseCube = baseRowStructure.getCube();

        if (baseCube.equals(derivedCube))
          dependantBaseRowStructures.add(baseRowStructure);

      }
    }
    return dependantBaseRowStructures;

  }
  
  /**
   *  Returns the FunctionalRowLogics which the functionalRowLogic depends upon.
   * @param functionalRowLogic
   * @return
   */
  public static EList<FunctionalRowLogic> getTheDependantFunctionalRowLogics(FunctionalRowLogic functionalRowLogic) {
    BasicEList<FunctionalRowLogic> dependantFunctionalRowLogic = new BasicEList<FunctionalRowLogic>();
    AttributeLineageModel attributeLineageModel = (AttributeLineageModel) functionalRowLogic.eContainer();
    boolean isUnionFunction = (functionalRowLogic.getCubeLogic().getRowFunction() instanceof UnionRowFunction);
    boolean oneAdded = false;

    FreeBirdToolsCube derivedCube1 = functionalRowLogic.getCubeLogic().getCube();
    EList<FreeBirdToolsCube> dependantCubes = ((DerivedCube) derivedCube1).getSourceCubes();
    Iterator<FreeBirdToolsCube> dependantCubesIter = dependantCubes.iterator();
    
    while (dependantCubesIter.hasNext()) {
      FreeBirdToolsCube cube2 = dependantCubesIter.next();
      Iterator<FunctionalRowLogic> iter = attributeLineageModel.getRowTransformations().iterator();
      FreeBirdToolsCube cube3 = null;
      while (iter.hasNext()) {
        FunctionalRowLogic rowlogic = iter.next();

        cube3 = rowlogic.getCubeLogic().getCube();

        if (cube3.equals(cube2)) {
          if (isUnionFunction) {
            if (!oneAdded) {
              dependantFunctionalRowLogic.add(rowlogic);
              oneAdded = true;
            }
          } else {
            dependantFunctionalRowLogic.add(rowlogic);
          }

        }

      }
    }

    return dependantFunctionalRowLogic;

  }
  
  /**
   * Returns the list FunctionalRowLogics of an attributeLineageModel, in order
   * such that an item in the list already has all the items it is dependent upon in the
   * list prior to itself.
   * 
   * @param attributeLineageModel
   * @return
   */
  public static EList<FunctionalRowLogic> getOrderedRowTransformations(AttributeLineageModel attributeLineageModel) {
    EList<FunctionalRowLogic> list = attributeLineageModel.getRowTransformations();
    EList<FunctionalRowLogic> orderedList = new BasicEList<FunctionalRowLogic>();
    int numberAdded = -1;
    while (numberAdded != 0) {
      numberAdded = addItemToOrderedListIfAllOfItsChildrenAreInTheList(list, orderedList);

    }

    return orderedList;

  }
  

  /**
   * Adds functionalRowLogics from the list to the end of an ordered list if all its 
   * dependent functionalRowLogis are already in the ordered list.
   * @param list
   * @param orderedList
   * @return the number of functionalrowLogics added to the ordered list.
   */
  private static int addItemToOrderedListIfAllOfItsChildrenAreInTheList(EList<FunctionalRowLogic> list,
      EList<FunctionalRowLogic> orderedList) {
    int added = 0;

    for (Iterator iterator = list.iterator(); iterator.hasNext();) {
      FunctionalRowLogic functionalRowLogic = (FunctionalRowLogic) iterator.next();
      if (!orderedList.contains(functionalRowLogic)) {
        EList<FunctionalRowLogic> children = getTheDependantFunctionalRowLogics(functionalRowLogic);
        boolean allChildrenIncluded = true;

        
        for (Iterator iterator2 = children.iterator(); iterator2.hasNext();) {
          FunctionalRowLogic functionalRowLogic2 = (FunctionalRowLogic) iterator2.next();
          if (!orderedList.contains(functionalRowLogic2)) {
            allChildrenIncluded = false;
          }

        }

        if (allChildrenIncluded) {
          orderedList.add(functionalRowLogic);
          added++;
        }
      }
    }

    return added;

  }
  

  /**
   * Finds a BaseRowStructure in an attributeLineageModel which is equal to the 
   * baseRowStructure passed in based on the checkBaseRowStructureEquality method.
   * 
   * @param baseRowStructure
   * @param model
   * @return
   */
  static BaseRowStructure findBaseRowStructureInAttributeModel(BaseRowStructure baseRowStructure,
      AttributeLineageModel model) {
    EList<BaseRowStructure> baseRowStructures = model.getBaseSchemas();
    BaseRowStructure returnRowStructure = null;
    for (Iterator iterator = baseRowStructures.iterator(); iterator.hasNext();) {
      BaseRowStructure baseRowStructure2 = (BaseRowStructure) iterator.next();
      if (checkBaseRowStructureEquality(baseRowStructure, baseRowStructure2)) {
        returnRowStructure = baseRowStructure2;
      }

    }

    return returnRowStructure;

  }

  /**
   * Checks  2 BaseRowStructures for equality
   * 
   * @param baseRowStructure1
   * @param baseRowStructure2
   * @return
   */
  private static boolean checkBaseRowStructureEquality(BaseRowStructure baseRowStructure1,
      BaseRowStructure baseRowStructure2) {
    boolean equality = false;
    String cubeName1 = baseRowStructure1.getCube().getCube_name();
    String cubeName2 = baseRowStructure2.getCube().getCube_name();
   
    if (cubeName1.equals(cubeName2))
      equality = true;

    return equality;
  }

  /**
   * Finds a ColumnFunction in an attributeLineageModel which is equal to the 
   * ColumnFunction passed in based on the checkColumnFunctionEquality method.
   * 
   * @param columnFunction
   * @param frl
   * @return
   */
  static ColumnFunction findColumnFunctionInFunctionalRowLogic(ColumnFunction columnFunction,
      FunctionalRowLogic frl) {
   
    EList<ColumnFunction> columnFunctions = frl.getColumnFunctionGroup().getColumnFunctions();
    ColumnFunction returnColFunc = null;
    for (Iterator iterator = columnFunctions.iterator(); iterator.hasNext();) {
      ColumnFunction columnFunction2 = (ColumnFunction) iterator.next();
      if (checkColumnFunctionEquality(columnFunction2, columnFunction))
        returnColFunc = columnFunction2;
    }
    return returnColFunc;
  }

  /**
   * Checks  2 ColumnFunctions for equality
   * 
   * @param columnFunction2
   * @param columnFunction1
   * @return
   */
  private static boolean checkColumnFunctionEquality(ColumnFunction columnFunction2, ColumnFunction columnFunction1) {
  
    boolean equality = false;
    String colFuncID1 = columnFunction1.getColumnID();
    String colFuncID2 = columnFunction2.getColumnID();
    if (colFuncID1.equals(colFuncID2))
      equality = true;
    return equality;
  }

  /**
   * Finds a FunctionalRowLogic in an attributeLineageModel which is equal to the 
   * baseRowStructure passed in based on the checkFunctionalRowLogicEquality method.
   * 
   * @param functionalRowLogic
   * @param model
   * @return
   */
  static FunctionalRowLogic findFunctionalRowLogicInAttributeModel(FunctionalRowLogic functionalRowLogic,
      AttributeLineageModel model) {

    EList<FunctionalRowLogic> rowTransformations = model.getRowTransformations();
    FunctionalRowLogic returnFunctionalRowLogic = null;
    for (Iterator iterator = rowTransformations.iterator(); iterator.hasNext();) {
      FunctionalRowLogic functionalRowLogic2 = (FunctionalRowLogic) iterator.next();
      if (checkFunctionalRowLogicEquality(functionalRowLogic2, functionalRowLogic)) {
        returnFunctionalRowLogic = functionalRowLogic2;
      }

    }

    return returnFunctionalRowLogic;

  }

  /**
   * Checks  2 FunctionalRowLogics for equality.
   * 
   * @param functionalRowLogic2
   * @param functionalRowLogic1
   * @return
   */
  private static boolean checkFunctionalRowLogicEquality(FunctionalRowLogic functionalRowLogic2,
      FunctionalRowLogic functionalRowLogic1) {
   
    boolean equality = false;
    String cubeName1 = functionalRowLogic1.getCubeLogic().getCube().getCube_name();
    String cubeName2 = functionalRowLogic2.getCubeLogic().getCube().getCube_name();
    // should check equality of source names of derived cubes also? I dont think it is necessery.
    if (cubeName1.equals(cubeName2))
      equality = true;

    return equality;
  }
  
  /**
   * Finds a FunctionalRowLogic in an attributeLineageModel which is equal to the 
   * baseRowStructure passed in based on the checkColumnEquality method.
   * @param column
   * @param baseRowStructure
   * @return
   */
  static CubeColumn findColumninSchema(CubeColumn column, BaseRowStructure baseRowStructure) {

    EList<CubeColumn> columns = baseRowStructure.getColumns();
    CubeColumn returnCol = null;
    for (Iterator iterator = columns.iterator(); iterator.hasNext();) {
      CubeColumn column2 = (CubeColumn) iterator.next();
      if (checkColumnEquality(column2, column))
        returnCol = column2;
    }
    return returnCol;
  }
  
  
  
  /**
   * Checks  2 CubeColumns for equality.
   * 
   * @param column2
   * @param column
   * @return
   */
  private static boolean checkColumnEquality(CubeColumn column2, CubeColumn column) {

    return (column2.getColumnID().equals(column.getColumnID()));
  }

  /**
   * Gets all columns associated with the FunctionalRowLogic, the make up its
   * column structure.
   * 
   * @param rowLogic
   * @return
   */
  public static EList<CubeColumn> getColumnsFromFunctionalRowLogic(FunctionalRowLogic rowLogic) {
   
    Iterator<ColumnFunction> columnFunctionIter = rowLogic.getColumnFunctionGroup().getColumnFunctions().iterator();
    Iterator<CubeColumn> createdColumnIter = getCreatedColumnsFromFunctionalCubeLogic(rowLogic.getCubeLogic()).iterator();

    
    EList<CubeColumn> returnColumnItems = new BasicEList<CubeColumn>();
    while (columnFunctionIter.hasNext()) {
      returnColumnItems.add(columnFunctionIter.next());
    }
    while (createdColumnIter.hasNext()) {
      returnColumnItems.add(createdColumnIter.next());
    }

    return returnColumnItems;
  }

  /**
   * Returns the created columns of a FunctionalCubeLogic. It is rare that created columns exist.
   * 
   * @param cubeLogic
   * @return
   */
  private static EList<CubeColumn> getCreatedColumnsFromFunctionalCubeLogic(FunctionalCubeLogic cubeLogic) {

    RowFunction rf = cubeLogic.getRowFunction();

    return rf.getCreatedCubeColumns();
  }



}
