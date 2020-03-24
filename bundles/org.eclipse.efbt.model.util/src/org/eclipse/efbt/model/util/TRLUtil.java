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

import java.io.File;
import java.util.Iterator;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.util.EcoreUtil;
import bird_model.BIRDModel;
import efbt_data_definition.CubeModule;
import efbt_data_definition.MemberModule;
import column_transformation_logic.AggregateColumnFunction;
import column_transformation_logic.BasicColumnFunction;
import column_transformation_logic.ColumnFunction;
import column_transformation_logic.Column_transformation_logicFactory;
import column_transformation_logic.CubeColumn;
import column_transformation_logic.StandardBasicColumnFunction;
import core.DOMAIN;
import core.MEMBER;
import core.VARIABLE;
import cube_schema.CubeSchema;
import row_transformation_logic.FilterAndGroupToOneRowCreationApproach;
import row_transformation_logic.RowCreationApproachForCube;
import row_transformation_logic.OneToOneRowCreationApproach;
import row_transformation_logic.FilterRowCreationApproach;
import row_transformation_logic.GroupByRowCreationApproach;
import row_transformation_logic.RowJoinFunction;
import row_transformation_logic.UnionRowCreationApproach;
import row_transformation_logic.impl.Row_transformation_logicFactoryImpl;
import tags.TransformationSchemeTag;
import cube_transformation_logic.impl.Cube_transformation_logicFactoryImpl;
import cubes.FreeBirdToolsCube;
import cubes.DerivedCube;
import data_definition.COMBINATION;
import data_definition.COMBINATION_ITEM;
import data_definition.CUBE;
import data_definition.CUBE_STRUCTURE;
import data_definition.CUBE_STRUCTURE_ITEM;
import domain_model_mapping.DomainMappingModule;
import domain_model_mapping.LeafModuleToReportCellViewModule;
import domain_model_mapping.LeafModuleToVersionedCubeSchemaModule;
import domain_model_mapping.LeafModuleToVersionedSQLViewsModule;
import functions.AggregateFunction;
import functions.BasicFunction;
import functions.BooleanFunction;
import functions.FunctionsFactory;
import functions.MemberParameter;
import functions.SpeculativeCubeColumnParameter;
import navigation_context.NavigationContext;
import reports.ReportCell;
import column_transformation_logic.ColumnFunctionGroup;
import cube_transformation_logic.CubeTransformationLogic;
import cube_transformation_logic.Cube_transformation_logicFactory;
import row_transformation_logic.Row_transformation_logicFactory;

import transformation.DataSetTransformation;
import functional_module.LeafTransformationScheme;
import functional_module.ReportCellCreationTransformationScheme;

import functional_module.TransformationScheme;
import transformation.VersionedComponentsSet;
import transformation.VersionedCubeSchemaModule;
import transformation.VersionedTransformationSchemeLogic;
import trl_report_cell_views.ReportCellView;
import trl_report_cell_views.ReportCellViewModule;
import trl_sql_views.AggregateEnrichmentView;
import trl_sql_views.CopyView;
import trl_sql_views.EnrichmentView;

import trl_sql_views.FilterByConditionView;
import trl_sql_views.JoinView;
import trl_sql_views.SQLView;
import trl_sql_views.UnionView;
import trl_sql_views.VersionedSQLViewsModule;

/**
 * @author Neil Mackenzie
 *
 */
public class TRLUtil {
 

  /**
   * Create a CubeTransformationLogic object form an SQLView.
   * 
   * @param view
   * @param transformationSchemeLogicList
   * @param cubeSchemaModuleList
   * @param specialFunctions
   * @return
   */
  public static CubeTransformationLogic translateViewToFunctionalRowLogic(SQLView view,
      EList<VersionedTransformationSchemeLogic> transformationSchemeLogicList,
      EList<VersionedCubeSchemaModule> cubeSchemaModuleList, 
      SpecialFunctionSpecs specialFunctions) {


    CubeTransformationLogic cubeTransformationLogic  = Cube_transformation_logicFactory.eINSTANCE.createCubeTransformationLogic();
    ColumnFunctionGroup cfg = Column_transformation_logicFactory.eINSTANCE.createColumnFunctionGroup();
    cubeTransformationLogic .setColumnFunctionGroup(cfg);
   
    
    // columns created by row function
    FreeBirdToolsCube derivedCube = view.getCube();
    setFunctionalCubeLogic(view, cubeTransformationLogic ,
        specialFunctions, transformationSchemeLogicList, cubeSchemaModuleList);

    if (view instanceof EnrichmentView) {

      EList<BasicColumnFunction> calculatedColumns = ((EnrichmentView) view).getFunctions();
      Iterator<BasicColumnFunction> calculatedColumnsIter = calculatedColumns.iterator();
      while (calculatedColumnsIter.hasNext()) {
        BasicColumnFunction calculatedColumn = calculatedColumnsIter.next();
        cubeTransformationLogic .getColumnFunctionGroup().getColumnFunctions().
        add(EcoreUtil.copy(calculatedColumn));

      }
    }
    if (view instanceof AggregateEnrichmentView) {
      EList<AggregateColumnFunction> calculatedColumns =
          ((AggregateEnrichmentView) view).getFunctions();
      Iterator<AggregateColumnFunction> calculatedColumnsIter = calculatedColumns.iterator();
      while (calculatedColumnsIter.hasNext()) {
        AggregateColumnFunction calculatedColumn = calculatedColumnsIter.next();
        cubeTransformationLogic .getColumnFunctionGroup().getColumnFunctions().
        add(EcoreUtil.copy(calculatedColumn));

      }
    }

    if ((view instanceof EnrichmentView) ||
        (view instanceof CopyView) || 
        (view instanceof FilterByConditionView)) {
      // now for each column that is actually available in the source cube
      // create a calculatedColumn using the specialCopycolumn Function.
      // first create the CopyClumn FunctionSpec

      EList<SQLView> dependantViews = getTheDependantViews(view, transformationSchemeLogicList);
      EList<CubeSchema> dependantSourceTables = getAnySourceCubes(view, cubeSchemaModuleList);

      Iterator<SQLView> dependantViewsIter = dependantViews.iterator();
      Iterator<CubeSchema> dependantSourceTablesIter = dependantSourceTables.iterator();
      while (dependantViewsIter.hasNext()) {
        SQLView dependantView = dependantViewsIter.next();
        EList<CubeColumn> columns = 
            getColumnsFromSQLView(dependantView, transformationSchemeLogicList, 
                cubeSchemaModuleList, specialFunctions);
        Iterator<CubeColumn> columnIter = columns.iterator();
        while (columnIter.hasNext()) {
          CubeColumn column = columnIter.next();
          StandardBasicColumnFunction calculatedColumn =
              Column_transformation_logicFactory.eINSTANCE
              .createStandardBasicColumnFunction();
          calculatedColumn.setColumnID(view.getCube().getCube_name() + 
              ":" + column.getVariable().getVariable_id());

          calculatedColumn.setVariable(column.getVariable());
          calculatedColumn.setCube(view.getCube());
          BasicFunction function = FunctionsFactory.eINSTANCE.createBasicFunction();
          function.setFunctionSpec(specialFunctions.copyColumnSpec);
          SpeculativeCubeColumnParameter columnParameter = FunctionsFactory.eINSTANCE
              .createSpeculativeCubeColumnParameter();

          columnParameter.setColumn(column.getVariable());
          columnParameter.setCube(dependantView.getCube());
          function.getParameters().add(columnParameter);
          calculatedColumn.setBasicFunction(function);
          cubeTransformationLogic .getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);
        }
      }

      while (dependantSourceTablesIter.hasNext()) {
        CubeSchema dependantSchema = dependantSourceTablesIter.next();
        EList<VARIABLE> columns = Util.getVariablesFromBIRDCube(dependantSchema.getCube());
        Iterator<VARIABLE> columnIter = columns.iterator();
        while (columnIter.hasNext()) {
          VARIABLE column = columnIter.next();
          StandardBasicColumnFunction calculatedColumn = 
              Column_transformation_logicFactory.eINSTANCE
              .createStandardBasicColumnFunction();
          calculatedColumn.setColumnID(view.getCube().getCube_name() + 
              ":" + column.getVariable_id());
          calculatedColumn.setVariable(column);
          calculatedColumn.setCube(view.getCube());
          BasicFunction function = FunctionsFactory.eINSTANCE.createBasicFunction();
          function.setFunctionSpec(specialFunctions.copyColumnSpec);
          SpeculativeCubeColumnParameter columnParameter = FunctionsFactory.eINSTANCE
              .createSpeculativeCubeColumnParameter();

          columnParameter.setColumn(column);
          columnParameter.setCube(dependantSchema.getBaseCube());
          function.getParameters().add(columnParameter);
          calculatedColumn.setBasicFunction(function);
          cubeTransformationLogic .getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);
        }
      }

    }

    if (view instanceof AggregateEnrichmentView) {
     
      EList<SQLView> dependantViews = getTheDependantViews(view, transformationSchemeLogicList);
      Iterator<SQLView> dependantViewsIter = dependantViews.iterator();
      AggregateEnrichmentView aggView = (AggregateEnrichmentView) view;
      
      while (dependantViewsIter.hasNext()) {
        SQLView dependantView = dependantViewsIter.next();
        // get the aggregates, and the groupbys
        EList<VARIABLE> groupByColumnnTypedItems = 
            ((AggregateEnrichmentView) view).getGroupByClause()
            .getGroupByColumns();

        // from these columns make the calculatedColumns

        Iterator<VARIABLE> groupByColumnnTypedItemsIter = groupByColumnnTypedItems.iterator();

        while (groupByColumnnTypedItemsIter.hasNext()) {
          VARIABLE column = groupByColumnnTypedItemsIter.next();
          AggregateColumnFunction calculatedColumn = Column_transformation_logicFactory.eINSTANCE
              .createAggregateColumnFunction();
          calculatedColumn.setColumnID(view.getCube().getCube_name() +
              ":" + column.getVariable_id());
          calculatedColumn.setVariable(column);
          calculatedColumn.setCube(view.getCube());
          AggregateFunction function = FunctionsFactory.eINSTANCE.createAggregateFunction();
          function.setFunctionSpec(specialFunctions.firstColumnSpec);

          SpeculativeCubeColumnParameter columnParameter = FunctionsFactory.eINSTANCE
              .createSpeculativeCubeColumnParameter();

          columnParameter.setColumn(column);
          columnParameter.setCube(dependantView.getCube());
          function.getParameters().add(columnParameter);
          calculatedColumn.setAggregateFunction(function);
          cubeTransformationLogic .getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);

        }

      }
      EList<CubeSchema> dependantSourceCubes = getAnySourceCubes(view, cubeSchemaModuleList);
      Iterator<CubeSchema> dependantSourceCubesIter = dependantSourceCubes.iterator();
      while (dependantSourceCubesIter.hasNext()) {
          CubeSchema dependantSchema = dependantSourceCubesIter.next();
       // get the aggregates, and the groupbys
          EList<VARIABLE> groupByColumnnTypedItems = 
              ((AggregateEnrichmentView) view).getGroupByClause()
              .getGroupByColumns();

          // from these columns make the calculatedColumns

          Iterator<VARIABLE> groupByColumnnTypedItemsIter = groupByColumnnTypedItems.iterator();

          while (groupByColumnnTypedItemsIter.hasNext()) {
            VARIABLE column = groupByColumnnTypedItemsIter.next();
            AggregateColumnFunction calculatedColumn = Column_transformation_logicFactory.eINSTANCE
                .createAggregateColumnFunction();
            calculatedColumn.setColumnID(view.getCube().getCube_name() +
                ":" + column.getVariable_id());
            calculatedColumn.setVariable(column);
            calculatedColumn.setCube(view.getCube());
            AggregateFunction function = FunctionsFactory.eINSTANCE.createAggregateFunction();
            function.setFunctionSpec(specialFunctions.firstColumnSpec);

            SpeculativeCubeColumnParameter columnParameter = FunctionsFactory.eINSTANCE
                .createSpeculativeCubeColumnParameter();

            columnParameter.setColumn(column);
            columnParameter.setCube(dependantSchema.getBaseCube());
            function.getParameters().add(columnParameter);
            calculatedColumn.setAggregateFunction(function);
            cubeTransformationLogic .getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);

          }
      }
    }
    if (view instanceof JoinView) {
      EList<SQLView> dependantViews = getTheDependantViews(view, transformationSchemeLogicList);
      EList<CubeSchema> dependantSourceCubes = getAnySourceCubes(view, cubeSchemaModuleList);

      Iterator<SQLView> dependantViewsIter = dependantViews.iterator();
      Iterator<CubeSchema> dependantSourceCubesIter = dependantSourceCubes.iterator();
      while (dependantViewsIter.hasNext()) {
        SQLView dependantView = dependantViewsIter.next();
        EList<CubeColumn> columns =
            getColumnsFromSQLView(dependantView, transformationSchemeLogicList, 
                cubeSchemaModuleList, specialFunctions);
        Iterator<CubeColumn> columnIter = columns.iterator();
        while (columnIter.hasNext()) {
          CubeColumn column = columnIter.next();
          // need to check if this is one of the ignored or replaced columns.
          // we could possible do this by reusing the replace function on the main view
          VARIABLE replacedColumn = ReplaceColumnUtil.replaceColumn((JoinView) view, dependantView,
              column.getVariable());
          if (replacedColumn != null) {
            StandardBasicColumnFunction calculatedColumn = 
                Column_transformation_logicFactory.eINSTANCE
                .createStandardBasicColumnFunction();
            calculatedColumn.setColumnID(view.getCube().getCube_name() + 
                ":" + replacedColumn.getVariable_id());

            calculatedColumn.setVariable(replacedColumn);
            calculatedColumn.setCube(view.getCube());
            BasicFunction function = FunctionsFactory.eINSTANCE.createBasicFunction();
            function.setFunctionSpec(specialFunctions.copyColumnSpec);
            SpeculativeCubeColumnParameter columnParameter = FunctionsFactory.eINSTANCE
                .createSpeculativeCubeColumnParameter();

            columnParameter.setColumn(column.getVariable());
            columnParameter.setCube(dependantView.getCube());
            function.getParameters().add(columnParameter);
            calculatedColumn.setBasicFunction(function);
            cubeTransformationLogic .getColumnFunctionGroup().
              getColumnFunctions().add(calculatedColumn);
          }
        }
      }

      while (dependantSourceCubesIter.hasNext()) {
        CubeSchema dependantSchema = dependantSourceCubesIter.next();
        EList<VARIABLE> columns = Util.getColumnsFromCubeSchema(dependantSchema);
        Iterator<VARIABLE> columnIter = columns.iterator();
        while (columnIter.hasNext()) {
          VARIABLE column = columnIter.next();
          VARIABLE replacedColumn = 
              ReplaceColumnUtil.replaceColumn((JoinView) view, dependantSchema.getBaseCube(),
              column);
          if (replacedColumn != null) {
            StandardBasicColumnFunction calculatedColumn =
                Column_transformation_logicFactory.eINSTANCE
                .createStandardBasicColumnFunction();
            calculatedColumn.setColumnID(view.getCube().getCube_name() +
                ":" + replacedColumn.getVariable_id());
            calculatedColumn.setVariable(replacedColumn);
            calculatedColumn.setCube(view.getCube());
            BasicFunction function = FunctionsFactory.eINSTANCE.createBasicFunction();
            function.setFunctionSpec(specialFunctions.copyColumnSpec);
            SpeculativeCubeColumnParameter columnParameter = FunctionsFactory.eINSTANCE
                .createSpeculativeCubeColumnParameter();

            columnParameter.setColumn(column);
            columnParameter.setCube(dependantSchema.getBaseCube());
            function.getParameters().add(columnParameter);
            calculatedColumn.setBasicFunction(function);
            cubeTransformationLogic .getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);
          }
        }
      }

    }

    if (view instanceof UnionView) {
      // now for each column that is actually available in the source cube
      // create a calculatedColumn using the specialCopycolumn Function.
      // first create the CopyClumn FunctionSpec
      // we should complain if they are not all compatible cubes.
      // then just use the first one, for structure, and then create a
      // calculated column for each field,
      // however the we should have a parameter for each dependent cube

      EList<SQLView> dependantViews = getTheDependantViews(view, transformationSchemeLogicList);
      EList<CubeSchema> dependantSourceCubes = getAnySourceCubes(view, cubeSchemaModuleList);

      Iterator<SQLView> dependantViewsIter = dependantViews.iterator();
      Iterator<CubeSchema> dependantSourceTablesIter = dependantSourceCubes.iterator();
      boolean first = true;

      while (dependantViewsIter.hasNext()) {
        SQLView dependantView = dependantViewsIter.next();
        if (first) {
          EList<CubeColumn> theColumns =
              getColumnsFromSQLView(dependantView, transformationSchemeLogicList,
              cubeSchemaModuleList, specialFunctions);
          Iterator<CubeColumn> columnIter = theColumns.iterator();
          while (columnIter.hasNext()) {
            CubeColumn column = columnIter.next();
            StandardBasicColumnFunction calculatedColumn =
                Column_transformation_logicFactory.eINSTANCE
                .createStandardBasicColumnFunction();
            calculatedColumn.setColumnID(view.getCube().getCube_name() +
                ":" + column.getVariable().getVariable_id());

            calculatedColumn.setVariable(column.getVariable());
            calculatedColumn.setCube(view.getCube());
            BasicFunction function = FunctionsFactory.eINSTANCE.createBasicFunction();
            function.setFunctionSpec(specialFunctions.copyColumnSpec);
            calculatedColumn.setBasicFunction(function);
            cubeTransformationLogic .getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);
          }
          first = false;
        }

        Iterator<ColumnFunction> theCalculatedColumns = 
            cubeTransformationLogic .getColumnFunctionGroup().getColumnFunctions()
            .iterator();
        while (theCalculatedColumns.hasNext()) {

          // for each calculated column we need to add a dependant column with this table
          StandardBasicColumnFunction theCalculatedColumn =
              (StandardBasicColumnFunction) theCalculatedColumns.next();
          SpeculativeCubeColumnParameter columnParameter = FunctionsFactory.eINSTANCE
              .createSpeculativeCubeColumnParameter();

          columnParameter.setColumn(theCalculatedColumn.getVariable());
          columnParameter.setCube(dependantView.getCube());
          theCalculatedColumn.getBasicFunction().getParameters().add(columnParameter);

        }

      }

      while (dependantSourceTablesIter.hasNext()) {
        CubeSchema dependantSchema = dependantSourceTablesIter.next();
        if (first) {
          EList<VARIABLE> theColumns = Util.getColumnsFromCubeSchema(dependantSchema);
          Iterator<VARIABLE> columnIter = theColumns.iterator();
          while (columnIter.hasNext()) {
            VARIABLE column = columnIter.next();
            StandardBasicColumnFunction calculatedColumn = 
                Column_transformation_logicFactory.eINSTANCE
                .createStandardBasicColumnFunction();
            calculatedColumn.setColumnID(
                view.getCube().getCube_name() + ":" + column.getVariable_id());

            calculatedColumn.setVariable(column);
            calculatedColumn.setCube(view.getCube());
            BasicFunction function = FunctionsFactory.eINSTANCE.createBasicFunction();
            function.setFunctionSpec(specialFunctions.copyColumnSpec);
            calculatedColumn.setBasicFunction(function);
            cubeTransformationLogic .getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);
          }
          first = false;
        }

        Iterator<ColumnFunction> theCalculatedColumns = 
            cubeTransformationLogic .getColumnFunctionGroup().getColumnFunctions()
            .iterator();
        while (theCalculatedColumns.hasNext()) {

          // for each calculated column we need to add a dependant column with this cube
          StandardBasicColumnFunction theCalculatedColumn = 
              (StandardBasicColumnFunction) theCalculatedColumns.next();
          SpeculativeCubeColumnParameter columnParameter = FunctionsFactory.eINSTANCE
              .createSpeculativeCubeColumnParameter();

          columnParameter.setColumn(theCalculatedColumn.getVariable());
          columnParameter.setCube(dependantSchema.getBaseCube());
          theCalculatedColumn.getBasicFunction().getParameters().add(columnParameter);

        }

      }
    }

    if (view instanceof ReportCellView) {
      // we want the groupbycolumns and the agregate column...
      // we just make a plain assumption that the source is a groupby transformation.
      EList<SQLView> dependantViews = getTheDependantViews(view, transformationSchemeLogicList);
      Iterator<SQLView> dependantViewsIter = dependantViews.iterator();
      ReportCellView dpView = (ReportCellView) view;
      while (dependantViewsIter.hasNext()) {
        SQLView dependantView = dependantViewsIter.next();

        VARIABLE measure = getMeasure(dpView);

        AggregateColumnFunction calculatedColumn = Column_transformation_logicFactory.eINSTANCE
            .createAggregateColumnFunction();
        calculatedColumn.setColumnID(view.getCube().getCube_name() + 
            ":" + measure.getVariable_id());
        calculatedColumn.setVariable(measure);
        calculatedColumn.setCube(view.getCube());
        AggregateFunction function = FunctionsFactory.eINSTANCE.createAggregateFunction();
        function.setFunctionSpec(specialFunctions.sumColumnSpec);

        SpeculativeCubeColumnParameter columnParameter = FunctionsFactory.eINSTANCE
            .createSpeculativeCubeColumnParameter();
        columnParameter.setColumn(measure);
        columnParameter.setCube(dependantView.getCube());
        function.getParameters().add(columnParameter);

        calculatedColumn.setAggregateFunction(function);
        cubeTransformationLogic .getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);

        Iterator<COMBINATION_ITEM> dimensions = getDimensionValues(dpView);
        while (dimensions.hasNext()) {
          COMBINATION_ITEM dimensionVal = dimensions.next();
          VARIABLE dimension = dimensionVal.getVariable_id();
          AggregateColumnFunction calculatedColumnForDimension =
              Column_transformation_logicFactory.eINSTANCE
              .createAggregateColumnFunction();
          calculatedColumnForDimension.setColumnID(view.getCube().getCube_name() + 
              ":" + dimension.getVariable_id());
          calculatedColumnForDimension.setVariable(dimension);
          calculatedColumnForDimension.setCube(view.getCube());
          AggregateFunction functionForDimension =
              FunctionsFactory.eINSTANCE.createAggregateFunction();
          functionForDimension.setFunctionSpec(specialFunctions.firstColumnSpec);
          calculatedColumnForDimension.setAggregateFunction(functionForDimension);

          SpeculativeCubeColumnParameter dimensionColumnParameter = FunctionsFactory.eINSTANCE
              .createSpeculativeCubeColumnParameter();
          dimensionColumnParameter.setColumn(dimension);
          dimensionColumnParameter.setCube(dependantView.getCube());
          functionForDimension.getParameters().add(dimensionColumnParameter);

          cubeTransformationLogic .getColumnFunctionGroup().
            getColumnFunctions().add(calculatedColumnForDimension);
        }

      }
    }

    return cubeTransformationLogic ;
  }

  /**
   * Set the RowCreationApproachForCube of a cubeTransformationLogic .
   * 
   * @param view
   * @param cubeTransformationLogic 
   * @param specialFunctions
   * @param transformationSchemeLogicList
   * @param cubeSchemaModuleList
   */
  private static void setFunctionalCubeLogic(SQLView view, CubeTransformationLogic cubeTransformationLogic ,
      SpecialFunctionSpecs specialFunctions, EList<VersionedTransformationSchemeLogic> transformationSchemeLogicList,
      EList<VersionedCubeSchemaModule> cubeSchemaModuleList) {
    
    if ((view instanceof CopyView) || (view instanceof EnrichmentView)) {
      RowCreationApproachForCube rowCreationApproachForCube = Row_transformation_logicFactoryImpl.eINSTANCE.createRowCreationApproachForCube();
      OneToOneRowCreationApproach oneToOneRowCreationApproach = Row_transformation_logicFactoryImpl.eINSTANCE
          .createOneToOneRowCreationApproach();
      rowCreationApproachForCube.setRowCreationApproach(oneToOneRowCreationApproach);
      rowCreationApproachForCube.setCube(view.getCube());
      rowCreationApproachForCube.setName(view.getName());
      cubeTransformationLogic .setRowCreationApproachForCube(rowCreationApproachForCube);

    }
    if (view instanceof FilterByConditionView) {
      RowCreationApproachForCube rowCreationApproachForCube = Row_transformation_logicFactoryImpl.eINSTANCE.createRowCreationApproachForCube();
      FilterRowCreationApproach rowFilterFunction = Row_transformation_logicFactoryImpl.eINSTANCE.createFilterRowCreationApproach();
      rowFilterFunction
      .setFilterFunction(EcoreUtil.copy(((FilterByConditionView) view).getWhereClause().getFunction()));
      rowCreationApproachForCube.setRowCreationApproach(rowFilterFunction);
      rowCreationApproachForCube.setCube(view.getCube());
      rowCreationApproachForCube.setName(view.getName());
      cubeTransformationLogic .setRowCreationApproachForCube(rowCreationApproachForCube);

    }

    if (view instanceof JoinView) {
      RowCreationApproachForCube rowCreationApproachForCube = Row_transformation_logicFactoryImpl.eINSTANCE.createRowCreationApproachForCube();
      RowJoinFunction rowJoinFunction = Row_transformation_logicFactoryImpl.eINSTANCE.createRowJoinFunction();
      rowJoinFunction.setJoinFunction(EcoreUtil.copy((((JoinView) view).getWhereClause().getFunction())));
      rowCreationApproachForCube.setRowCreationApproach(rowJoinFunction);
      rowCreationApproachForCube.setCube(view.getCube());
      rowCreationApproachForCube.setName(view.getName());
      cubeTransformationLogic .setRowCreationApproachForCube(rowCreationApproachForCube);

    }
    if (view instanceof AggregateEnrichmentView) {
      RowCreationApproachForCube rowCreationApproachForCube = Row_transformation_logicFactoryImpl.eINSTANCE.createRowCreationApproachForCube();
      GroupByRowCreationApproach groupByFunction = Row_transformation_logicFactoryImpl.eINSTANCE.createGroupByRowCreationApproach();
      groupByFunction.getGroupByColumns()
      .addAll((((AggregateEnrichmentView) view).getGroupByClause().getGroupByColumns()));
      rowCreationApproachForCube.setRowCreationApproach(groupByFunction);
      rowCreationApproachForCube.setCube(view.getCube());
      rowCreationApproachForCube.setName(view.getName());
      cubeTransformationLogic .setRowCreationApproachForCube(rowCreationApproachForCube);

    }

    if (view instanceof ReportCellView) {
      // get the dependent view
      SQLView dependantView = getTheDependantViews(view, transformationSchemeLogicList).get(0);
      RowCreationApproachForCube rowCreationApproachForCube = Row_transformation_logicFactoryImpl.eINSTANCE.createRowCreationApproachForCube();
      FilterAndGroupToOneRowCreationApproach rowFilterAndGroupFunction = Row_transformation_logicFactoryImpl.eINSTANCE
          .createFilterAndGroupToOneRowCreationApproach();
     
      BooleanFunction bf = FunctionsFactory.eINSTANCE.createBooleanFunction();
      // this is a hack to make it work for 1 dimension, we need
      // to change to make it work for multiple dimensions
    

      Iterator<COMBINATION_ITEM> dimensionValues = getDimensionValues((ReportCellView) view);
      while (dimensionValues.hasNext()) {
        COMBINATION_ITEM dimValue = dimensionValues.next();
       
        rowFilterAndGroupFunction.getGroupByVariables().add(dimValue.getVariable_id());
        BooleanFunction bf2 = FunctionsFactory.eINSTANCE.createBooleanFunction();
        bf2.setFunctionSpec(specialFunctions.equalsSpec);

        SpeculativeCubeColumnParameter param = FunctionsFactory.eINSTANCE.createSpeculativeCubeColumnParameter();

        param.setColumn(dimValue.getVariable_id());
        param.setCube(dependantView.getCube());
        bf2.getParameters().add(param);

        MemberParameter memberparam = FunctionsFactory.eINSTANCE.createMemberParameter();
        memberparam.setParam(dimValue.getMember_id());
        bf2.getParameters().add(memberparam);

        rowFilterAndGroupFunction.setFilterFunction(bf2);

      }

      rowCreationApproachForCube.setRowCreationApproach(rowFilterAndGroupFunction);
      rowCreationApproachForCube.setCube(view.getCube());
      rowCreationApproachForCube.setName(view.getName());
      cubeTransformationLogic .setRowCreationApproachForCube(rowCreationApproachForCube);

    }

    if (view instanceof UnionView) {
      RowCreationApproachForCube rowCreationApproachForCube = Row_transformation_logicFactoryImpl.eINSTANCE.createRowCreationApproachForCube();
      UnionRowCreationApproach urf = Row_transformation_logicFactoryImpl.eINSTANCE.createUnionRowCreationApproach();

      rowCreationApproachForCube.setRowCreationApproach(urf);
      rowCreationApproachForCube.setCube(view.getCube());
      rowCreationApproachForCube.setName(view.getName());
      cubeTransformationLogic .setRowCreationApproachForCube(rowCreationApproachForCube);

    }

  }

  /**
   * Return the columns associated with an SQLView.
   * @param view
   * @param transformationSchemeLogicList
   * @param cubeSchemaModuleList
   * @param specialFunctions
   * @return
   */
  public static EList<CubeColumn> getColumnsFromSQLView(SQLView view,
      EList<VersionedTransformationSchemeLogic> transformationSchemeLogicList,
      EList<VersionedCubeSchemaModule> cubeSchemaModuleList, SpecialFunctionSpecs specialFunctions) {
    CubeTransformationLogic rowLogic = translateViewToFunctionalRowLogic(view, transformationSchemeLogicList,
        cubeSchemaModuleList, specialFunctions);
    return AttributeLineageUtil.getColumnsFromFunctionalRowLogic(rowLogic);
  }

  /**
   * Return the Views dependant upon a view
   * @param view
   * @param transformationSchemeLogicList
   * @return
   */
  public static EList<SQLView> getTheDependantViews(SQLView view,
      EList<VersionedTransformationSchemeLogic> transformationSchemeLogicList) {

    BasicEList<SQLView> dependantViews = new BasicEList<SQLView>();


    EList<FreeBirdToolsCube> dependantCubes = ((DerivedCube) (view.getCube())).getSourceCubes();
    Iterator<FreeBirdToolsCube> dependantCubesIter = dependantCubes.iterator();

    while (dependantCubesIter.hasNext()) {
      FreeBirdToolsCube dtd = dependantCubesIter.next();
   
      EList<SQLView> viewList = getAllViews(transformationSchemeLogicList);
      Iterator<SQLView> iter = viewList.iterator();
      FreeBirdToolsCube cube = null;
      while (iter.hasNext()) {
        SQLView tableDefinitionItem = iter.next();

        cube = tableDefinitionItem.getCube();

        if (cube.equals(dtd)) {
          dependantViews.add((SQLView) tableDefinitionItem);
        }

      }

    }

    return dependantViews;
  }

  /**
   * et all the SQLViews associated with a  list of VersionedTransformationSchemeLogics
   * @param transformationSchemeLogicList
   * @return
   */
  private static EList<SQLView> getAllViews(EList<VersionedTransformationSchemeLogic> transformationSchemeLogicList) {
    EList<SQLView> returnList = new BasicEList<SQLView>();
    for (Iterator iterator = transformationSchemeLogicList.iterator(); iterator.hasNext();) {
      VersionedTransformationSchemeLogic dataSetTransformationModule = (VersionedTransformationSchemeLogic) iterator
          .next();
      EList<SQLView> views = ((VersionedSQLViewsModule) dataSetTransformationModule).getSqlViews();
      for (Iterator iterator2 = views.iterator(); iterator2.hasNext();) {
        SQLView sqlView = (SQLView) iterator2.next();
        returnList.add(sqlView);
      }
    }
    return returnList;

  }

  /**
   * Get source cubes of a view in the context of a list of cubeSchemaModules.
   * @param view
   * @param cubeSchemaModules
   * @return
   */
  public static EList<CubeSchema> getAnySourceCubes(SQLView view,
      EList<VersionedCubeSchemaModule> cubeSchemaModules) {

    BasicEList<CubeSchema> dependantSourceCube = new BasicEList<CubeSchema>();
    CubeSchema dependantSchema = null;
    EList<FreeBirdToolsCube> sourceCubeList = ((DerivedCube) view.getCube()).getSourceCubes();

    Iterator<FreeBirdToolsCube> dependantCubesIter = sourceCubeList.iterator();

    EList<CubeSchema> cubeSchemas = Util.getSchemas(cubeSchemaModules);
    while (dependantCubesIter.hasNext()) {
      FreeBirdToolsCube dtd = dependantCubesIter.next();
      // loop through the context and get any views which are linked to the cube.
      Iterator<CubeSchema> iter = cubeSchemas.iterator();
      FreeBirdToolsCube hlItem = null;
      while (iter.hasNext()) {
        CubeSchema tableDefinitionItem = iter.next();

        hlItem = ((CubeSchema) tableDefinitionItem).getBaseCube();

        if (hlItem.equals(dtd)) {
          dependantSourceCube.add((CubeSchema) tableDefinitionItem);
        }

      }

    }

    return dependantSourceCube;

  }

  /**
	 * For a TransformationScheme, get the SQLViewsModule associated according to
	 * the DefaultNavigationContext.
	 * 
	 * @param transformationScheme
	 * @return
	 */
	public static VersionedSQLViewsModule getDefaultSQLViewsModuleForTransformation( 
			LeafTransformationScheme transformationScheme) {

		VersionedSQLViewsModule returnModule = null;
		EList<TransformationSchemeTag> returnlist = new BasicEList<TransformationSchemeTag>();
		NavigationContext nc = TagUtil.getDefaultNavigationContext(transformationScheme);
		/**
		 * VersionedComponentsSet context = nc.getTransformationContext();
		 * 
		 * EList<VersionedCubeSchemaModule> sm = context.getCubeSchemaModules();
		 * EList<ReportCellViewModule> rm = context.getReportCellViewModules();
		 * EList<VersionedTransformationSchemeLogic> tm =
		 * context.getDatasetTransformationModules();
		 * 
		 * for (Iterator iterator = tm.iterator(); iterator.hasNext();) {
		 * VersionedTransformationSchemeLogic dataSetTransformationModule =
		 * (VersionedTransformationSchemeLogic) iterator .next(); if
		 * (dataSetTransformationModule.getTransformationScheme().equals(transformationScheme))
		 * { returnModule = (VersionedSQLViewsModule) dataSetTransformationModule; }
		 * 
		 * }
		 **/
		DomainMappingModule domainMapping = nc.getDomainModelMapping();
		EList<LeafModuleToVersionedSQLViewsModule> csm = domainMapping.getLeafModuleToVersionedSQLViewsModules();

		for (LeafModuleToVersionedSQLViewsModule leafModuleToVersionedSQLViewsModule : csm) {
			if (leafModuleToVersionedSQLViewsModule.getScheme().equals(transformationScheme)) {
				returnModule = leafModuleToVersionedSQLViewsModule.getModule();
			}
		}

		return returnModule;
	}
  
  public static EList<CUBE_STRUCTURE_ITEM> getCubeStructureItemsFromDefaultBIRDModel(CubeSchema schema)
  {
	    NavigationContext nc = NavigationContextUtil.getDefaultNavigationContext(schema);
	    BIRDModel birdModel = nc.getBirdModel();
	    EList<CubeModule> cubeModules = birdModel.getCubes();
	    CUBE cube = schema.getCube();
	    CUBE_STRUCTURE cubeStructure = cube.getCube_structure_id();
	    EList<CUBE_STRUCTURE_ITEM> returnlist = new BasicEList<CUBE_STRUCTURE_ITEM>();
	    for (Iterator iterator = cubeModules.iterator(); iterator.hasNext();) {
			CubeModule cubeModule = (CubeModule) iterator.next();
			EList<CUBE_STRUCTURE_ITEM> items = cubeModule.getCubeStructureItems();
			for (Iterator iterator2 = items.iterator(); iterator2.hasNext();) {
				CUBE_STRUCTURE_ITEM cube_STRUCTURE_ITEM = (CUBE_STRUCTURE_ITEM) iterator2.next();
				if(cube_STRUCTURE_ITEM.getCube_structure_id().equals(cubeStructure))
				{
					returnlist.add(cube_STRUCTURE_ITEM);
				}
				
			}
			
		}
	    return returnlist;
  }

 

 

  /**
   * Return the associated reportCellView for a reportCell in the context of a componentSet.
   * @param reportCell
   * @param componentSet
   * @return
   */
  static DataSetTransformation getReportCellViewForCell(ReportCell reportCell, VersionedComponentsSet componentSet) {

    EList<ReportCellViewModule> rcm = componentSet.getReportCellViewModules();
    ReportCellView returnView = null;
    for (Iterator iterator = rcm.iterator(); iterator.hasNext();) {
      ReportCellViewModule reportCellViewModule = (ReportCellViewModule) iterator.next();
      EList<ReportCellView> rcvm = reportCellViewModule.getReportCellViews();
      for (Iterator iterator2 = rcvm.iterator(); iterator2.hasNext();) {
        ReportCellView reportCellView = (ReportCellView) iterator2.next();
        ReportCell cell = reportCellView.getReportCell();
        if (cell.equals(reportCell)) {
          returnView = reportCellView;
        }

      }
    }
    return returnView;

  }

 

  /**
   * Return the measure associated with a reportCellView.
   * 
   * @param reportCellView
   * @return
   */
  private static VARIABLE getMeasure(ReportCellView reportCellView) {

    VARIABLE returnVariable = null;
    COMBINATION comb = reportCellView.getReportCell().getCombination();
  
    EList<COMBINATION_ITEM> items = comb.getCombination_items();
    for (Iterator iterator = items.iterator(); iterator.hasNext();) {
      COMBINATION_ITEM combination_ITEM = (COMBINATION_ITEM) iterator.next();
      boolean isEnumerated = combination_ITEM.getVariable_id().getDomain_id().isIs_enumerated();
      if (  ( (COMBINATION) combination_ITEM.eContainer()  ).equals(comb) && !isEnumerated) {
        returnVariable = combination_ITEM.getVariable_id();
      }
    }
    return returnVariable;
  }

  /**
   * Return the Dimension values associated with a reportCellView.
   * 
   * @param rcView
   * @return
   */
  private static Iterator<COMBINATION_ITEM> getDimensionValues(ReportCellView rcView) {
    EList<COMBINATION_ITEM> returncombitems = new BasicEList<COMBINATION_ITEM>();
    COMBINATION comb = rcView.getReportCell().getCombination();
  
    EList<COMBINATION_ITEM> items = comb.getCombination_items();
    for (Iterator iterator = items.iterator(); iterator.hasNext();) {
      COMBINATION_ITEM combination_ITEM = (COMBINATION_ITEM) iterator.next();
      boolean isEnumerated = combination_ITEM.getVariable_id().getDomain_id().isIs_enumerated();
      if (( (COMBINATION) combination_ITEM.eContainer()  ).equals(comb) && isEnumerated) {
        returncombitems.add(combination_ITEM);
      }

    }
    return returncombitems.iterator();
  }

  /**
	 * Return the VersionedCubeSchemaModule asociated with a
	 * LeafTransformationScheme.
	 * 
	 * @param transformationScheme
	 * @return
	 */
	public static VersionedCubeSchemaModule getDefaultCubeSchemaModuleForTransformationScheme(
			LeafTransformationScheme transformationScheme) {

		VersionedCubeSchemaModule returnModule = null;

		EList<TransformationSchemeTag> returnlist = new BasicEList<TransformationSchemeTag>();
		NavigationContext nc = TagUtil.getDefaultNavigationContext(transformationScheme);

		/**
		 * EList<TransformationSchemeTag> returnlist = new
		 * BasicEList<TransformationSchemeTag>(); NavigationContext nc =
		 * TagUtil.getDefaultNavigationContext(transformationScheme);
		 * VersionedComponentsSet context = nc.getTransformationContext();
		 * 
		 * EList<VersionedCubeSchemaModule> sm = context.getCubeSchemaModules();
		 * EList<ReportCellViewModule> rm = context.getReportCellViewModules();
		 * EList<VersionedTransformationSchemeLogic> tm =
		 * context.getDatasetTransformationModules();
		 * 
		 * for (Iterator iterator = sm.iterator(); iterator.hasNext();) {
		 * VersionedCubeSchemaModule cubeSchemaModule = (VersionedCubeSchemaModule)
		 * iterator.next(); if
		 * (cubeSchemaModule.getTransformationScheme().equals(transformationScheme)) {
		 * returnModule = (VersionedCubeSchemaModule) cubeSchemaModule; }
		 * 
		 * }
		 */

		DomainMappingModule domainMapping = nc.getDomainModelMapping();
		EList<LeafModuleToVersionedCubeSchemaModule> csm = domainMapping.getLeafModuleToVersionedCubeSchemaModules();

		for (LeafModuleToVersionedCubeSchemaModule leafModuleToVersionedCubeSchemaModule : csm) {
			if (leafModuleToVersionedCubeSchemaModule.getScheme().equals(transformationScheme)) {
				returnModule = leafModuleToVersionedCubeSchemaModule.getModule();
			}
		}

		return returnModule;
	}

  
  public static EList<MEMBER> getMembers(
	      DOMAIN domain) {

	   EList<MEMBER> members = new BasicEList<MEMBER>();
	   
	    NavigationContext nc = getDefaultNavigationContext(domain);
	   BIRDModel bm = nc.getBirdModel();
	   EList<MemberModule> birdMemberModules = bm.getMembers();
	   for (Iterator iterator = birdMemberModules.iterator(); iterator.hasNext();) {
		MemberModule memberModule = (MemberModule) iterator.next();
		
		 EList<MEMBER> birdMembers = memberModule.getMembers();
		 for (Iterator iterator2 = birdMembers.iterator(); iterator2.hasNext();) {
			MEMBER member = (MEMBER) iterator2.next();
			
			if (member.getDomain_id().equals(domain))
				members.add(member);
			
		}
		
		
	}
	   
	   return members;
	   
	  }
  
  

  private static NavigationContext getDefaultNavigationContext(DOMAIN domain) {
	
	  ResourceSet rs = domain.eResource().getResourceSet();
	    String tagsXMLFile = domain.eResource().getURI().trimSegments(1)
	        + "/defaultNavigationContext.navigation_context";
	    System.out.println(tagsXMLFile);
	    File file = new File(tagsXMLFile);
	    URI uri = file.isFile() ? URI.createFileURI(file.getAbsolutePath()) : URI.createURI(tagsXMLFile);
	    Resource resource = rs.getResource(uri, true);
	    return (NavigationContext) resource.getContents().get(0);
}



  /**
	 * Find the LeafTransformationScheme associated a reportCell in the context of a
	 * VersionedComponentsSet.
	 * 
	 * @param reportCell
	 * @param context
	 * @return
	 */
	static LeafTransformationScheme findSchemeForReportCellView(ReportCell reportCell, VersionedComponentsSet context, DomainMappingModule mappingModule) {
		// find the scheme with the cell
		EList<ReportCellViewModule> rcm = context.getReportCellViewModules();
		LeafTransformationScheme returnScheme = null;
		for (Iterator iterator = rcm.iterator(); iterator.hasNext();) {
			ReportCellViewModule reportCellViewModule = (ReportCellViewModule) iterator.next();
			EList<ReportCellView> rcvm = reportCellViewModule.getReportCellViews();
			for (Iterator iterator2 = rcvm.iterator(); iterator2.hasNext();) {
				ReportCellView reportCellView = (ReportCellView) iterator2.next();
				ReportCell cell = reportCellView.getReportCell();
				if (cell.equals(reportCell)) {
					
					returnScheme = getTransformationSchemeForReportCellViewModule(reportCellViewModule, mappingModule); 
				}

			}
		}
		return returnScheme;
	}
	
	private static LeafTransformationScheme getTransformationSchemeForReportCellViewModule(
			ReportCellViewModule reportCellViewModule, DomainMappingModule mappingModule) {
		
		LeafTransformationScheme returnLeafTransformationScheme = null;
		  EList<LeafModuleToReportCellViewModule> mappings = mappingModule
				.getLeafModuleToReportCellViewModules();
		for (LeafModuleToReportCellViewModule leafModuleToReportCellViewModule : mappings) {
			if (leafModuleToReportCellViewModule.getModule().getName().equals(reportCellViewModule.getName())) {
				returnLeafTransformationScheme = leafModuleToReportCellViewModule.getScheme();
			}
		}
		return returnLeafTransformationScheme;
	}


}
