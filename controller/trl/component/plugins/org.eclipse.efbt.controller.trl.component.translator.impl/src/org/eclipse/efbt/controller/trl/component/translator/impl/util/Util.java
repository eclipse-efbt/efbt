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
package org.eclipse.efbt.controller.trl.component.translator.impl.util;

import java.util.Iterator;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.ReportCellCreationFunctionalityModule;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule;
import org.eclipse.efbt.controller.trl.component.translator.api.AttributeLineageUtil;
import org.eclipse.efbt.language.trl.model.transformation.VersionedComponentsSet;
import org.eclipse.efbt.language.trl.model.transformation.VersionedCubeSchemaModule;
import org.eclipse.efbt.language.trl.model.transformation.VersionedFunctionalModuleLogic;
import org.eclipse.efbt.language.trl.model.trl_sql_views.VersionedSQLViewsModule;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_row_transformation_logic.ExplodeArrayOfStructsRowFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.attribute_lineage.AttributeLineageModel;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ColumnFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.Column_transformation_logicFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ResolvedCubeColumnParameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.SpeculativeCubeColumnParameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_transformation_logic.CubeTransformationLogic;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.BaseCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunctionSpec;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunctionSpec;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Function;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionsFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.BaseRowStructure;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.FilterAndGroupToOneRowCreationApproach;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.FilterRowCreationApproach;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.GroupByRowCreationApproach;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.RowCreationApproach;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.RowCreationApproachForCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.RowJoinFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.Row_transformation_logicFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.query.attribute_lineage.AttributeLineageModelQuery;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;



/**
 * @author Neil Mackenzie
 *
 */
public class Util {

  /**
   * Get the list of VersionedCubeSchemaModule associated with a
   * versionedComponentSet.
   * 
   * @param functionalModule
   * @param versionedComponentSet
   * @return
   */
  private static EList<VersionedCubeSchemaModule> getCubeSchemaModuleList(
		  ReportCellCreationFunctionalityModule functionalModule, VersionedComponentsSet versionedComponentSet) {

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
  
    attributeLineageModel.getSpecialFunctionSpecs().add(copyColumnSpec);
    specialFunctions.copyColumnSpec = copyColumnSpec;

    AggregateFunctionSpec firstColumnSpec = FunctionsFactory.eINSTANCE.createAggregateFunctionSpec();

    firstColumnSpec.setName("First");
    firstColumnSpec.setFunctionalName("funcs.First");
    firstColumnSpec.setSqlName("First");
  
    attributeLineageModel.getSpecialFunctionSpecs().add(firstColumnSpec);
    specialFunctions.firstColumnSpec = firstColumnSpec;

    AggregateFunctionSpec sumColumnSpec = FunctionsFactory.eINSTANCE.createAggregateFunctionSpec();

    sumColumnSpec.setName("SUM");
    sumColumnSpec.setFunctionalName("funcs.SUM");
    sumColumnSpec.setSqlName("SUM");
   
    attributeLineageModel.getSpecialFunctionSpecs().add(sumColumnSpec);
    specialFunctions.sumColumnSpec = sumColumnSpec;

    BasicFunctionSpec equalsSpec = FunctionsFactory.eINSTANCE.createBasicFunctionSpec();
    equalsSpec.setName("isEqualTo");
    equalsSpec.setFunctionalName("=");
    equalsSpec.setIsInfix(true);
    equalsSpec.setSqlName("=");
    equalsSpec.setFunctionalName("==");
  
    attributeLineageModel.getSpecialFunctionSpecs().add(equalsSpec);
    specialFunctions.equalsSpec = equalsSpec;
    
    BasicFunctionSpec makeRowSpec= FunctionsFactory.eINSTANCE.createBasicFunctionSpec();
   	makeRowSpec.setIsInfix(false);
   	makeRowSpec.setName("MakeRow");
   	makeRowSpec.setFunctionalName("funcs.MakeRow");
   	makeRowSpec.setSqlName("MakeRow");
   	attributeLineageModel.getSpecialFunctionSpecs().add(makeRowSpec);
   	specialFunctions.makeRowSpec = makeRowSpec;
   	
   	
   	AggregateFunctionSpec makeRowSetSpec = FunctionsFactory.eINSTANCE.createAggregateFunctionSpec();
   	//firstColumnSpec.setInfix(false);
   	makeRowSetSpec.setName("MakeRowSet");
   	makeRowSetSpec.setFunctionalName("funcs.MakeRowSet");
   	makeRowSetSpec.setSqlName("MakeRowSet");

   	attributeLineageModel.getSpecialFunctionSpecs().add(makeRowSetSpec);
   	specialFunctions.makeRowSetSpec = makeRowSetSpec;
   	
   	BasicFunctionSpec getRowNofColY= FunctionsFactory.eINSTANCE.createBasicFunctionSpec();
   	getRowNofColY.setIsInfix(false);
   	getRowNofColY.setName("GetRowNofColY");
   	getRowNofColY.setFunctionalName("funcs.GetRowNofColY");
   	getRowNofColY.setSqlName("GetRowNofColY");
   	attributeLineageModel.getSpecialFunctionSpecs().add(getRowNofColY);
   	specialFunctions.getRowNofColY = getRowNofColY;
   	

    return specialFunctions;
  }

  /**
   * Create  a BaseRowStructure from a cubeSchema in the context of a 
   * functionalModuleLogicList and a cubeSchemaModuleList.
   * 
   * @param cubeSchema
   * @param functionalModuleLogicList
   * @param cubeSchemaModuleList
   * @return
   */
  public static BaseRowStructure translateCubeSchemaToBaseRowStructure
     (CubeSchema cubeSchema,
      EList<VersionedSQLViewsModule> functionalModuleLogicList,
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
      column.setName(cubeSchema.getBaseCube().getName() + ":" + colitem.getVariable_id());
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
          ResolvedCubeColumnParameter linkedColParam = Column_transformation_logicFactory.eINSTANCE.createResolvedCubeColumnParameter();
          CubeColumn col = resolveColumnFromRowLogicGroup(qcrp, attributeLineageModel);
          linkedColParam.setCubeColumn(col);
          int index = qcrpContainer.getParameters().indexOf(qcrp);
          qcrpContainer.getParameters().remove(index);
          qcrpContainer.getParameters().add(index, linkedColParam);

        }
        
        if (o instanceof SpeculativeStructColumnParameter)
		{
			SpeculativeStructColumnParameter qrcrp = (SpeculativeStructColumnParameter) o;
			Function qrcrpContainer = (Function) qrcrp.eContainer();
			ResolvedStructColumnParameter linkedRowColParam = Advanced_variable_lineagefunctionsFactory.eINSTANCE.createResolvedStructColumnParameter();
			CubeColumn col2 = resolveStructColumnFromRowLogicGroup(qrcrp,attributeLineageModel);
			linkedRowColParam.setCubeColumn(col2);
			linkedRowColParam.setColumnInsideStruct(qrcrp.getColumnInsideStruct());
			int index = qrcrpContainer.getParameters().indexOf(qrcrp);
			qrcrpContainer.getParameters().remove(index);
			qrcrpContainer.getParameters().add(index, linkedRowColParam);
			
			
		}

      }
    }
  }
  
  private static CubeColumn resolveStructColumnFromRowLogicGroup(SpeculativeStructColumnParameter speculativeStructColumn, AttributeLineageModel program) {
		return resolveColumnFromRowLogicGroup(speculativeStructColumn.getStructColumn(), speculativeStructColumn.getCube(), program);
		
		
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
    	EList<CubeTransformationLogic> dependantRowLogics = AttributeLineageModelQuery.getTheDependantFunctionalRowLogics(rowlogic);
        EList<BaseRowStructure> dependantBaseRowStructures = AttributeLineageModelQuery.getTheDependantBaseRowStructures(rowlogic);
        
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

        ResolvedCubeColumnParameter linkedColParam = Column_transformation_logicFactory.eINSTANCE.createResolvedCubeColumnParameter();
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
          ResolvedCubeColumnParameter linkedColParam = Column_transformation_logicFactory.eINSTANCE.createResolvedCubeColumnParameter();
          CubeColumn col = resolveColumnFromRowLogicGroup(qcrp, attributeLineageModel);
          linkedColParam.setCubeColumn(col);
          int index = qcrpContainer.getParameters().indexOf(qcrp);
          qcrpContainer.getParameters().remove(index);
          qcrpContainer.getParameters().add(index, linkedColParam);

          rf.getDependantCubeColumns().add(EcoreUtil.copy(linkedColParam));

        }
        
        if (o instanceof SpeculativeStructColumnParameter)
		{
			SpeculativeStructColumnParameter qrcrp = (SpeculativeStructColumnParameter) o;
			Function qrcrpContainer = (Function) qrcrp.eContainer();
			ResolvedStructColumnParameter linkedRowColParam = Advanced_variable_lineagefunctionsFactory.eINSTANCE.createResolvedStructColumnParameter();
			CubeColumn col2 = resolveStructColumnFromRowLogicGroup(qrcrp,attributeLineageModel);
			linkedRowColParam.setCubeColumn(col2);
			linkedRowColParam.setColumnInsideStruct(qrcrp.getColumnInsideStruct());
			int index = qrcrpContainer.getParameters().indexOf(qrcrp);
			qrcrpContainer.getParameters().remove(index);
			qrcrpContainer.getParameters().add(index, linkedRowColParam);
			rf.getDependantStructItemColumns().add(EcoreUtil.copy(linkedRowColParam));
			
			
		}

      }

    }

    if(rf instanceof ExplodeArrayOfStructsRowFunction)
  	{
  		FreeBirdToolsCube dependantTable = AttributeLineageModelQuery.getTheDependantFunctionalRowLogics(rowlogic).get(0).getRowCreationApproachForCube().getCube();
  		//CubeColumn tc2 = Column_transformation_logicFactory.eINSTANCE.createCubeColumn();
  		VARIABLE arraySourceColumn = ((ExplodeArrayOfStructsRowFunction) rf).getArraySourceVariable();
  	  //  tc2.setVariable(arraySourceColumn);
  	   // tc2.setCube(dependantTable);
  		CubeColumn tc2 = resolveColumnFromRowLogicGroup(arraySourceColumn,dependantTable,attributeLineageModel);
  	    // set the table to the source table: 			  tc2.setCube()
  	    //and set the ID too.
  	   // ((ExplodeArrayOfStructsRowFunction) rf).setArraySourceTableColumn(tc2);
  	    
  	    
  	    ResolvedCubeColumnParameter linkedColParam = Column_transformation_logicFactory.eINSTANCE.createResolvedCubeColumnParameter();
  	    linkedColParam.setCubeColumn(tc2);
  	    rf.getDependantCubeColumns().add(linkedColParam);
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
          ResolvedCubeColumnParameter linkedColParam = Column_transformation_logicFactory.eINSTANCE.createResolvedCubeColumnParameter();
          CubeColumn col = resolveColumnFromRowLogicGroup(qcrp, attributeLineageModel);
          linkedColParam.setCubeColumn(col);
          int index = qcrpContainer.getParameters().indexOf(qcrp);
          qcrpContainer.getParameters().remove(index);
          qcrpContainer.getParameters().add(index, linkedColParam);

          rf.getDependantCubeColumns().add(EcoreUtil.copy(linkedColParam));

        }
        
        if (o instanceof SpeculativeStructColumnParameter)
      		{
      			SpeculativeStructColumnParameter qrcrp = (SpeculativeStructColumnParameter) o;
      			Function qrcrpContainer = (Function) qrcrp.eContainer();
      			ResolvedStructColumnParameter linkedRowColParam = Advanced_variable_lineagefunctionsFactory.eINSTANCE.createResolvedStructColumnParameter();
      			CubeColumn col2 = resolveStructColumnFromRowLogicGroup(qrcrp,attributeLineageModel);
      			linkedRowColParam.setCubeColumn(col2);
      			linkedRowColParam.setColumnInsideStruct(qrcrp.getColumnInsideStruct());
      			int index = qrcrpContainer.getParameters().indexOf(qrcrp);
      			qrcrpContainer.getParameters().remove(index);
      			qrcrpContainer.getParameters().add(index, linkedRowColParam);
      			 rf.getDependantStructItemColumns().add(EcoreUtil.copy(linkedRowColParam));
      			
      			
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
