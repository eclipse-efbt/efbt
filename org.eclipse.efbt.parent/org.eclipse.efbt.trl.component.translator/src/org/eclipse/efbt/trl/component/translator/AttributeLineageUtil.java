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

package org.eclipse.efbt.trl.component.translator;

import java.util.Iterator;

import org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageModel;
import org.eclipse.efbt.controller.model.platform_call.Platform_callPackage;
import org.eclipse.efbt.almengine.model.attribute_lineage.attribute_lineage.AttributeLineageModel;
import org.eclipse.efbt.almengine.model.attribute_lineage.attribute_lineage.Attribute_lineageFactory;
import org.eclipse.efbt.aorta.aortafunctions.AortafunctionsFactory;
import org.eclipse.efbt.trl.model.trl.transformation.VersionedFunctionalModuleLogic;
import org.eclipse.efbt.trl.model.trl.trl_report_cell_views.ReportCellView;
import org.eclipse.efbt.trl.model.trl.trl_report_cell_views.ReportCellViewModule;
import org.eclipse.efbt.trl.model.trl.transformation.VersionedCubeSchemaModule;
import org.eclipse.efbt.trl.model.trl.trl_sql_views.VersionedSQLViewsModule;
import org.eclipse.efbt.trl.model.trl.trl_sql_views.SQLView;
import org.eclipse.efbt.almengine.model.attribute_lineage.cube_transformation_logic.CubeTransformationLogic;
import org.eclipse.efbt.almengine.model.attribute_lineage.row_transformation_logic.BaseRowStructure;
import org.eclipse.efbt.almengine.model.attribute_lineage.row_transformation_logic.RowCreationApproach;
import org.eclipse.efbt.almengine.model.attribute_lineage.row_transformation_logic.RowCreationApproachForCube;
import org.eclipse.efbt.almengine.model.attribute_lineage.row_transformation_logic.UnionRowCreationApproach;
import org.eclipse.efbt.almengine.model.cubes.cube_schema.CubeSchema;
import org.eclipse.efbt.almengine.model.cubes.cubes.FreeBirdToolsCube;
import org.eclipse.efbt.almengine.model.cubes.cubes.DerivedCube;
import org.eclipse.efbt.almengine.model.functions.column_transformation_logic.ColumnFunction;
import org.eclipse.efbt.almengine.model.functions.functions.CubeColumn;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.util.EcoreUtil;


/**
 * @author Neil Mackenzie
 *
 */
public class AttributeLineageUtil {

  

  /**
   * This method finds the CubeTransformationLogic within the attributeLineageModel which
   * is associated with a particular cube.
   * 
   * @param cube
   * @param attributeLineageModel
   * @return
   */
  static CubeTransformationLogic getFunctionalRowLogicForCube(FreeBirdToolsCube cube, AttributeLineageModel attributeLineageModel) {
   
    Iterator<CubeTransformationLogic> rowLogicIter = attributeLineageModel.getRowTransformations().iterator();
    CubeTransformationLogic returnFunctionalRowLogic = null;
    while (rowLogicIter.hasNext()) {
      CubeTransformationLogic rowLogic = rowLogicIter.next();
      FreeBirdToolsCube rowlogicCube = rowLogic.getRowCreationApproachForCube().getCube();
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
   * Returns the BaseRowStrucures which the cubeTransformationLogic  depends upon.
   * @param cubeTransformationLogic 
   * @return
   */
  public static EList<BaseRowStructure> getTheDependantBaseRowStructures(CubeTransformationLogic cubeTransformationLogic ) {

    BasicEList<BaseRowStructure> dependantBaseRowStructures = new BasicEList<BaseRowStructure>();
    AttributeLineageModel attributeModel = (AttributeLineageModel) cubeTransformationLogic .eContainer();
   
    FreeBirdToolsCube cube = cubeTransformationLogic.getRowCreationApproachForCube().getCube();
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
   *  Returns the FunctionalRowLogics which the cubeTransformationLogic  depends upon.
   * @param cubeTransformationLogic 
   * @return
   */
  public static EList<CubeTransformationLogic> getTheDependantFunctionalRowLogics(CubeTransformationLogic cubeTransformationLogic ) {
    BasicEList<CubeTransformationLogic> dependantFunctionalRowLogic = new BasicEList<CubeTransformationLogic>();
    AttributeLineageModel attributeLineageModel = (AttributeLineageModel) cubeTransformationLogic .eContainer();
    boolean isUnionFunction = (cubeTransformationLogic .getRowCreationApproachForCube().getRowCreationApproach() instanceof UnionRowCreationApproach);
    boolean oneAdded = false;

    FreeBirdToolsCube derivedCube1 = cubeTransformationLogic .getRowCreationApproachForCube().getCube();
    EList<FreeBirdToolsCube> dependantCubes = ((DerivedCube) derivedCube1).getSourceCubes();
    Iterator<FreeBirdToolsCube> dependantCubesIter = dependantCubes.iterator();
    
    while (dependantCubesIter.hasNext()) {
      FreeBirdToolsCube cube2 = dependantCubesIter.next();
      Iterator<CubeTransformationLogic> iter = attributeLineageModel.getRowTransformations().iterator();
      FreeBirdToolsCube cube3 = null;
      while (iter.hasNext()) {
        CubeTransformationLogic rowlogic = iter.next();

        cube3 = rowlogic.getRowCreationApproachForCube().getCube();

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
  public static EList<CubeTransformationLogic> getOrderedRowTransformations(AttributeLineageModel attributeLineageModel) {
    EList<CubeTransformationLogic> list = attributeLineageModel.getRowTransformations();
    EList<CubeTransformationLogic> orderedList = new BasicEList<CubeTransformationLogic>();
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
  private static int addItemToOrderedListIfAllOfItsChildrenAreInTheList(EList<CubeTransformationLogic> list,
      EList<CubeTransformationLogic> orderedList) {
    int added = 0;

    for (Iterator iterator = list.iterator(); iterator.hasNext();) {
      CubeTransformationLogic cubeTransformationLogic  = (CubeTransformationLogic) iterator.next();
      if (!orderedList.contains(cubeTransformationLogic )) {
        EList<CubeTransformationLogic> children = getTheDependantFunctionalRowLogics(cubeTransformationLogic );
        boolean allChildrenIncluded = true;

        
        for (Iterator iterator2 = children.iterator(); iterator2.hasNext();) {
          CubeTransformationLogic functionalRowLogic2 = (CubeTransformationLogic) iterator2.next();
          if (!orderedList.contains(functionalRowLogic2)) {
            allChildrenIncluded = false;
          }

        }

        if (allChildrenIncluded) {
          orderedList.add(cubeTransformationLogic );
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
    String cubeName1 = baseRowStructure1.getCube().getName();
    String cubeName2 = baseRowStructure2.getCube().getName();
   
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
      CubeTransformationLogic frl) {
   
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
   * Finds a CubeTransformationLogic in an attributeLineageModel which is equal to the 
   * baseRowStructure passed in based on the checkFunctionalRowLogicEquality method.
   * 
   * @param cubeTransformationLogic 
   * @param model
   * @return
   */
  static CubeTransformationLogic findFunctionalRowLogicInAttributeModel(CubeTransformationLogic cubeTransformationLogic ,
      AttributeLineageModel model) {

    EList<CubeTransformationLogic> rowTransformations = model.getRowTransformations();
    CubeTransformationLogic returnFunctionalRowLogic = null;
    for (Iterator iterator = rowTransformations.iterator(); iterator.hasNext();) {
      CubeTransformationLogic functionalRowLogic2 = (CubeTransformationLogic) iterator.next();
      if (checkFunctionalRowLogicEquality(functionalRowLogic2, cubeTransformationLogic )) {
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
  private static boolean checkFunctionalRowLogicEquality(CubeTransformationLogic functionalRowLogic2,
      CubeTransformationLogic functionalRowLogic1) {
   
    boolean equality = false;
    String cubeName1 = functionalRowLogic1.getRowCreationApproachForCube().getCube().getName();
    String cubeName2 = functionalRowLogic2.getRowCreationApproachForCube().getCube().getName();
    // should check equality of source names of derived cubes also? I dont think it is necessery.
    if (cubeName1.equals(cubeName2))
      equality = true;

    return equality;
  }
  
  /**
   * Finds a CubeTransformationLogic in an attributeLineageModel which is equal to the 
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
   * Gets all columns associated with the CubeTransformationLogic, the make up its
   * column structure.
   * 
   * @param rowLogic
   * @return
   */
  public static EList<CubeColumn> getColumnsFromFunctionalRowLogic(CubeTransformationLogic rowLogic) {
   
    Iterator<ColumnFunction> columnFunctionIter = rowLogic.getColumnFunctionGroup().getColumnFunctions().iterator();
    Iterator<CubeColumn> createdColumnIter = getCreatedColumnsFromFunctionalCubeLogic(rowLogic.getRowCreationApproachForCube()).iterator();

    
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
   * Returns the created columns of a RowCreationApproachForCube. It is rare that created columns exist.
   * 
   * @param rowCreationApproachForCube
   * @return
   */
  private static EList<CubeColumn> getCreatedColumnsFromFunctionalCubeLogic(RowCreationApproachForCube rowCreationApproachForCube) {

    RowCreationApproach rf = rowCreationApproachForCube.getRowCreationApproach();

    return rf.getCreatedCubeColumns();
  }



/**
 * Creates an attributeLineageModel according to the details of the GetAttributeLineageModel
 * call, this is then set as the resultingModel field of the call.
 * 
 * @param call
 */
public static void createAttributeLineageModel(GetAttributeLineageModel call) {

  // delete the old row logic group and build the new one.
  call.eUnset(call.eClass().getEStructuralFeature(Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL));
  AttributeLineageModel attributeLineageModel = Attribute_lineageFactory.eINSTANCE.createAttributeLineageModel();
  call.setResultingModel(attributeLineageModel);
  // we should create the set of useful functions
  SpecialFunctionSpecs specialFunctions = Util.setSpecialFucntions(attributeLineageModel);

  // loop through each view in each sqlViews modules.

  EList<VersionedFunctionalModuleLogic> functionalModuleLogicList = call.getTransformationContext()
      .getDatasetTransformationModules();
  EList<ReportCellViewModule> reportViewModuleList = call.getTransformationContext().getReportCellViewModules();
  EList<VersionedCubeSchemaModule> cubeSchemaModuleList = call.getTransformationContext().getCubeSchemaModules();

  Iterator<VersionedFunctionalModuleLogic> transformationModuleIter = functionalModuleLogicList.iterator();
  Iterator<ReportCellViewModule> reportViewModuleIter = reportViewModuleList.iterator();
  Iterator<VersionedCubeSchemaModule> CubeSchemaModuleIter = cubeSchemaModuleList.iterator();

  while (transformationModuleIter.hasNext()) {
        VersionedSQLViewsModule viewModules = (VersionedSQLViewsModule) transformationModuleIter.next();
    EList<SQLView> viewList = viewModules.getSqlViews();
    Iterator<SQLView> viewIter = viewList.iterator();
    while (viewIter.hasNext()) {
   
      SQLView view = viewIter.next();
      CubeTransformationLogic rowlogic = TRLUtil.translateViewToFunctionalRowLogic(view, functionalModuleLogicList,
          cubeSchemaModuleList, specialFunctions);
      if (rowlogic.getRowCreationApproachForCube() != null) {

        attributeLineageModel.getRowTransformations().add(rowlogic);
      }
    }

  }
  while (reportViewModuleIter.hasNext()) {
  
    ReportCellViewModule reportViewModules = reportViewModuleIter.next();
    EList<ReportCellView> reportCellViewList = reportViewModules.getReportCellViews();
    Iterator<ReportCellView> reportViewIter = reportCellViewList.iterator();
    while (reportViewIter.hasNext()) {
     
      SQLView view = reportViewIter.next();
      CubeTransformationLogic rowlogic = TRLUtil.translateViewToFunctionalRowLogic(view, functionalModuleLogicList,
          cubeSchemaModuleList, specialFunctions);
      if (rowlogic.getRowCreationApproachForCube() != null) {

        attributeLineageModel.getRowTransformations().add(rowlogic);
      }
    }

  }

  while (CubeSchemaModuleIter.hasNext()) {
   
    VersionedCubeSchemaModule versionedCubeSchemaModule = CubeSchemaModuleIter.next();
    EList<CubeSchema> tableSchemaList = versionedCubeSchemaModule.getSchemas();
    Iterator<CubeSchema> tableSchemaIter = tableSchemaList.iterator();
    while (tableSchemaIter.hasNext()) {
    
      CubeSchema ts = tableSchemaIter.next();
		BaseRowStructure e = Util
				.translateCubeSchemaToBaseRowStructure(ts, functionalModuleLogicList,
          cubeSchemaModuleList);

      attributeLineageModel.getBaseSchemas().add(e);

    }

  }

  Util.replaceSpeculativeColumnReferencesWithResolvedColumnReference(attributeLineageModel);


}





}
