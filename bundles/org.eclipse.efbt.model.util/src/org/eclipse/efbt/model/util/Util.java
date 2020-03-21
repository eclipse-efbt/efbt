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
import efbt_data_definition.CubeModule;
import column_transformation_logic.ColumnFunction;
import column_transformation_logic.Column_transformation_logicFactory;
import column_transformation_logic.CubeColumn;
import core.VARIABLE;
import cube_schema.CubeSchema;
import row_transformation_logic.FilterAndGroupToOneRowCreationApproach;
import row_transformation_logic.RowCreationApproachForCube;
import row_transformation_logic.FilterRowCreationApproach;
import row_transformation_logic.RowCreationApproach;
import row_transformation_logic.GroupByRowCreationApproach;
import row_transformation_logic.RowJoinFunction;
import cubes.BaseCube;
import cubes.FreeBirdToolsCube;
import data_definition.CUBE;
import data_definition.CUBE_STRUCTURE;
import data_definition.CUBE_STRUCTURE_ITEM;
import functions.AggregateFunctionSpec;
import functions.BasicFunctionSpec;
import functions.Function;
import functions.FunctionsFactory;
import functions.ResolvedCubeColumnParameter;
import functions.SpeculativeCubeColumnParameter;
import row_transformation_logic.BaseRowStructure;
import cube_transformation_logic.CubeTransformationLogic;
import row_transformation_logic.Row_transformation_logicFactory;
import transformation.ReportCellCreationTransformationScheme;
import transformation.VersionedComponentsSet;
import transformation.VersionedCubeSchemaModule;
import transformation.VersionedTransformationSchemeLogic;

/**
 * @author Neil Mackenzie
 *
 */
public class Util {

  /**
   * Get the list of VersionedCubeSchemaModule associated with a
   * versionedComponentSet.
   * 
   * @param transformationScheme
   * @param versionedComponentSet
   * @return
   */
  private static EList<VersionedCubeSchemaModule> getCubeSchemaModuleList(
      ReportCellCreationTransformationScheme transformationScheme, VersionedComponentsSet versionedComponentSet) {

    return null; // TODO need to check why this is null.
  }

  /**
   * Set the set of fixed SpecialFunctionSpecs that should be linked to an
   * attributeLineageModel.
   * 
   * @param attributeLineageModel
   * @return
   */
  public static SpecialFunctionSpecs setSpecialFucntions(AttributeLineageModel attributeLineageModel) {
    SpecialFunctionSpecs specialFunctions = new SpecialFunctionSpecs();
    BasicFunctionSpec copyColumnSpec = FunctionsFactory.eINSTANCE.createBasicFunctionSpec();
    copyColumnSpec.setIsInfix(false);
    copyColumnSpec.setName("CopyColumn");
    copyColumnSpec.setFunctionalName("funcs.CopyColumn");
    copyColumnSpec.setSqlName("ID");
    copyColumnSpec.setType(null);
    attributeLineageModel.getSpecialFunctionSpecs().add(copyColumnSpec);
    specialFunctions.copyColumnSpec = copyColumnSpec;

    AggregateFunctionSpec firstColumnSpec = FunctionsFactory.eINSTANCE.createAggregateFunctionSpec();

    firstColumnSpec.setName("First");
    firstColumnSpec.setFunctionalName("funcs.First");
    firstColumnSpec.setSqlName("First");
    firstColumnSpec.setType(null);
    attributeLineageModel.getSpecialFunctionSpecs().add(firstColumnSpec);
    specialFunctions.firstColumnSpec = firstColumnSpec;

    AggregateFunctionSpec sumColumnSpec = FunctionsFactory.eINSTANCE.createAggregateFunctionSpec();

    sumColumnSpec.setName("SUM");
    sumColumnSpec.setFunctionalName("funcs.SUM");
    sumColumnSpec.setSqlName("SUM");
    sumColumnSpec.setType(null);
    attributeLineageModel.getSpecialFunctionSpecs().add(sumColumnSpec);
    specialFunctions.sumColumnSpec = sumColumnSpec;

    BasicFunctionSpec equalsSpec = FunctionsFactory.eINSTANCE.createBasicFunctionSpec();
    equalsSpec.setName("isEqualTo");
    equalsSpec.setFunctionalName("=");
    equalsSpec.setIsInfix(true);
    equalsSpec.setSqlName("=");
    equalsSpec.setFunctionalName("==");
    equalsSpec.setType(null);
    attributeLineageModel.getSpecialFunctionSpecs().add(equalsSpec);
    specialFunctions.equalsSpec = equalsSpec;

    return specialFunctions;
  }

  /**
   * Create  a BaseRowStructure from a cubeSchema in the context of a 
   * transformationSchemeLogicList and a cubeSchemaModuleList.
   * 
   * @param cubeSchema
   * @param transformationSchemeLogicList
   * @param cubeSchemaModuleList
   * @return
   */
  public static BaseRowStructure translateCubeSchemaToBaseRowStructure
     (CubeSchema cubeSchema,
      EList<VersionedTransformationSchemeLogic> transformationSchemeLogicList,
      EList<VersionedCubeSchemaModule> cubeSchemaModuleList) {

    EList<VARIABLE> columnsList = getColumnsFromCubeSchema(cubeSchema);
    Iterator<VARIABLE> columnsIter = columnsList.iterator();

    BaseRowStructure baseRowStructure = Row_transformation_logicFactory.eINSTANCE.createBaseRowStructure();
    baseRowStructure.setCube((BaseCube) cubeSchema.getBaseCube());

    while (columnsIter.hasNext()) {
      VARIABLE colitem = columnsIter.next();
      CubeColumn column = Column_transformation_logicFactory.eINSTANCE.createCubeColumn();
      column.setVariable(colitem);
      column.setCube(cubeSchema.getBaseCube());
      column.setColumnID(cubeSchema.getBaseCube().getCube_name() + ":" + colitem.getVariable_id());
      baseRowStructure.getColumns().add(column);
    }

    return baseRowStructure;

  }

  /**
   * Get the columns associated with a cubeSchema.
   * 
   * @param cubeSchema
   * @return
   */
  static EList<VARIABLE> getColumnsFromCubeSchema(CubeSchema cubeSchema) {
	  
	 EList<CUBE_STRUCTURE_ITEM> csi = TRLUtil.getCubeStructureItemsFromDefaultBIRDModel(cubeSchema);
	  EList<VARIABLE> returnVariables = new BasicEList<VARIABLE>();
	  for (Iterator iterator2 = csi.iterator(); iterator2.hasNext();) {
          CUBE_STRUCTURE_ITEM cube_STRUCTURE_ITEM = (CUBE_STRUCTURE_ITEM) iterator2.next();
        
            returnVariables.add(cube_STRUCTURE_ITEM.getVariable_id());

        }

	  return returnVariables;
   // return getVariablesFromBIRDCube(cubeSchema.getCube());
  }

  /**
   * Replace SpeculativeCubeColumnParameters with ResolvedCubeColumnParameters in an
   * attributeLineageModel.
   * 
   * @param attributeLineageModel
   */
  public static void replaceSpeculativeColumnReferencesWithResolvedColumnReference(
      AttributeLineageModel attributeLineageModel) {

    Iterator<CubeTransformationLogic> rowlogicIter = attributeLineageModel.getRowTransformations().iterator();
    while (rowlogicIter.hasNext()) {
      CubeTransformationLogic rowlogic = rowlogicIter.next();
      replaceSpeculativeTableLogicWithResolvedTableLogic(rowlogic, attributeLineageModel);
      TreeIterator<EObject> iter = rowlogic.eAllContents();

      while (iter.hasNext()) {
        EObject o = iter.next();
        if (o instanceof SpeculativeCubeColumnParameter) {
          SpeculativeCubeColumnParameter qcrp = (SpeculativeCubeColumnParameter) o;
          Function qcrpContainer = (Function) qcrp.eContainer();
          ResolvedCubeColumnParameter linkedColParam = FunctionsFactory.eINSTANCE.createResolvedCubeColumnParameter();
          CubeColumn col = resolveColumnFromRowLogicGroup(qcrp, attributeLineageModel);
          linkedColParam.setCubeColumn(col);
          int index = qcrpContainer.getParameters().indexOf(qcrp);
          qcrpContainer.getParameters().remove(index);
          qcrpContainer.getParameters().add(index, linkedColParam);

        }

      }
    }
  }

  /**
   * @param rowlogic
   * @param attributeLineageModel
   */
  private static void replaceSpeculativeTableLogicWithResolvedTableLogic(CubeTransformationLogic rowlogic,
      AttributeLineageModel attributeLineageModel) {

    RowCreationApproachForCube tableLogic = rowlogic.getRowCreationApproachForCube();
    RowCreationApproach rf = tableLogic.getRowCreationApproach();

    if (rf instanceof GroupByRowCreationApproach) {
    	EList<CubeTransformationLogic> dependantRowLogics = AttributeLineageUtil.getTheDependantFunctionalRowLogics(rowlogic);
        EList<BaseRowStructure> dependantBaseRowStructures = AttributeLineageUtil.getTheDependantBaseRowStructures(rowlogic);
        
        FreeBirdToolsCube dependantTable = null;
        		
        if(dependantRowLogics.size() > 0)
         dependantTable = dependantRowLogics.get(0).getRowCreationApproachForCube().getCube();
        
        if(dependantBaseRowStructures.size() > 0)
            dependantTable = dependantBaseRowStructures.get(0).getCube();
        
        
        
          
      Iterator<VARIABLE> groupByCols = ((GroupByRowCreationApproach) rf).getGroupByColumns().iterator();
      while (groupByCols.hasNext()) {
        VARIABLE col = groupByCols.next();
        CubeColumn tableColumn = resolveColumnFromRowLogicGroup(col, dependantTable, attributeLineageModel);
        ((GroupByRowCreationApproach) rf).getGroupByCubeColumns().add(tableColumn);

        ResolvedCubeColumnParameter linkedColParam = FunctionsFactory.eINSTANCE.createResolvedCubeColumnParameter();
        linkedColParam.setCubeColumn(tableColumn);

        rf.getDependantCubeColumns().add(linkedColParam);
      }

    }
    if (rf instanceof FilterAndGroupToOneRowCreationApproach) {
      TreeIterator<EObject> iter = rf.eAllContents();

      while (iter.hasNext()) {
        EObject o = iter.next();
        if (o instanceof SpeculativeCubeColumnParameter) {
          SpeculativeCubeColumnParameter qcrp = (SpeculativeCubeColumnParameter) o;
          Function qcrpContainer = (Function) qcrp.eContainer();
          ResolvedCubeColumnParameter linkedColParam = FunctionsFactory.eINSTANCE.createResolvedCubeColumnParameter();
          CubeColumn col = resolveColumnFromRowLogicGroup(qcrp, attributeLineageModel);
          linkedColParam.setCubeColumn(col);
          int index = qcrpContainer.getParameters().indexOf(qcrp);
          qcrpContainer.getParameters().remove(index);
          qcrpContainer.getParameters().add(index, linkedColParam);

          rf.getDependantCubeColumns().add(EcoreUtil.copy(linkedColParam));

        }

      }

    }

    if ((rf instanceof RowJoinFunction) || (rf instanceof FilterRowCreationApproach)) {

      // copy the basic functions,replace speculative with resolved, set onthe
      // rowfunctions

      TreeIterator<EObject> iter = rf.eAllContents();

      while (iter.hasNext()) {
        EObject o = iter.next();
        if (o instanceof SpeculativeCubeColumnParameter) {
          SpeculativeCubeColumnParameter qcrp = (SpeculativeCubeColumnParameter) o;
          Function qcrpContainer = (Function) qcrp.eContainer();
          ResolvedCubeColumnParameter linkedColParam = FunctionsFactory.eINSTANCE.createResolvedCubeColumnParameter();
          CubeColumn col = resolveColumnFromRowLogicGroup(qcrp, attributeLineageModel);
          linkedColParam.setCubeColumn(col);
          int index = qcrpContainer.getParameters().indexOf(qcrp);
          qcrpContainer.getParameters().remove(index);
          qcrpContainer.getParameters().add(index, linkedColParam);

          rf.getDependantCubeColumns().add(EcoreUtil.copy(linkedColParam));

        }

      }
    }

  }

  /**
   * @param theColumn
   * @param theCube
   * @param attributeLineageModel
   * @return
   */
  private static CubeColumn resolveColumnFromRowLogicGroup(VARIABLE theColumn, FreeBirdToolsCube theCube,
      AttributeLineageModel attributeLineageModel) {
    EList<CubeTransformationLogic> rowLogicList = attributeLineageModel.getRowTransformations();
    Iterator<CubeTransformationLogic> rowLogicIter = rowLogicList.iterator();
    CubeColumn returnCol = null;
    while (rowLogicIter.hasNext()) {
      CubeTransformationLogic rowLogic = rowLogicIter.next();
      EList<ColumnFunction> columnFuncs = rowLogic.getColumnFunctionGroup().getColumnFunctions();
      Iterator<ColumnFunction> columnFuncsIter = columnFuncs.iterator();

      while (columnFuncsIter.hasNext()) {
        ColumnFunction col = columnFuncsIter.next();

        if ((theColumn.equals(col.getVariable())) && (theCube.equals(col.getCube()))) {
          returnCol = col;
        }

      }

      RowCreationApproachForCube tl = rowLogic.getRowCreationApproachForCube();
      RowCreationApproach rf = tl.getRowCreationApproach();

      Iterator<CubeColumn> createdColumnns = rf.getCreatedCubeColumns().iterator();
      while (createdColumnns.hasNext()) {
        CubeColumn createdColumnn = createdColumnns.next();
        if ((theColumn.equals(createdColumnn.getVariable())) && (theCube.equals(createdColumnn.getCube()))) {
          returnCol = createdColumnn;
        }
      }

    }
    EList<BaseRowStructure> evalschemaList = attributeLineageModel.getBaseSchemas();
    Iterator<BaseRowStructure> evalschemaIter = evalschemaList.iterator();
    while (evalschemaIter.hasNext()) {
      BaseRowStructure evalschema = evalschemaIter.next();
      EList<CubeColumn> cols = evalschema.getColumns();
      Iterator<CubeColumn> colsIter = cols.iterator();

      while (colsIter.hasNext()) {
        CubeColumn col = colsIter.next();

        if ((theColumn.equals(col.getVariable())) && (theCube.equals(col.getCube()))) {
          returnCol = col;
        }

      }

    }

    return returnCol;
  }

  /**
   * @param speculateCubeColumn
   * @param attributeLineageModel
   * @return
   */
  public static CubeColumn resolveColumnFromRowLogicGroup(SpeculativeCubeColumnParameter speculateCubeColumn,
      AttributeLineageModel attributeLineageModel) {

    return resolveColumnFromRowLogicGroup(speculateCubeColumn.getColumn(), speculateCubeColumn.getCube(),
        attributeLineageModel);

  }

  /**
   * @param cube
   * @return
   */
  static EList<VARIABLE> getVariablesFromBIRDCube(CUBE cube) {
    // get the module, then the cube structure, then the items and then the
    // varaibles.
    CubeModule cm = (CubeModule) cube.eContainer();
    EList<VARIABLE> returnVariables = new BasicEList<VARIABLE>();
    EList<CUBE_STRUCTURE> cs = cm.getCubeStructures();
    EList<CUBE_STRUCTURE_ITEM> csi = cm.getCubeStructureItems();
    for (Iterator iterator = cs.iterator(); iterator.hasNext();) {
      CUBE_STRUCTURE cube_STRUCTURE = (CUBE_STRUCTURE) iterator.next();
      if (cube_STRUCTURE.equals(cube.getCube_structure_id())) {
        for (Iterator iterator2 = csi.iterator(); iterator2.hasNext();) {
          CUBE_STRUCTURE_ITEM cube_STRUCTURE_ITEM = (CUBE_STRUCTURE_ITEM) iterator2.next();
          if (cube_STRUCTURE_ITEM.getCube_structure_id().equals(cube_STRUCTURE))
            returnVariables.add(cube_STRUCTURE_ITEM.getVariable_id());

        }
      }

    }
    return returnVariables;
  }

  /**
   * @param cubeSchemaModules
   * @return
   */
  static EList<CubeSchema> getSchemas(EList<VersionedCubeSchemaModule> cubeSchemaModules) {
    EList<CubeSchema> returnList = new BasicEList<CubeSchema>();
    for (Iterator iterator = cubeSchemaModules.iterator(); iterator.hasNext();) {
      VersionedCubeSchemaModule cubeSchemaModule = (VersionedCubeSchemaModule) iterator.next();
      EList<CubeSchema> schemas = ((VersionedCubeSchemaModule) cubeSchemaModule).getSchemas();
      for (Iterator iterator2 = schemas.iterator(); iterator2.hasNext();) {
        CubeSchema schema = (CubeSchema) iterator2.next();
        returnList.add(schema);
      }
    }
    return returnList;
  }

}
