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
package org.eclipse.efbt.language.trl.component.translator.util;


import java.util.Iterator;

import org.eclipse.efbt.cocamo.core.model.functionality_module.LeafFunctionalityModule;
import org.eclipse.efbt.cocamo.core.model.module_management.Module;
import org.eclipse.efbt.cocamo.core.model.module_management.ModuleDependency;
import org.eclipse.efbt.cocamo.functions.model.functions.AggregateFunction;
import org.eclipse.efbt.cocamo.functions.model.functions.BasicFunction;
import org.eclipse.efbt.cocamo.functions.model.functions.BasicFunctionSpec;
import org.eclipse.efbt.cocamo.functions.model.functions.BooleanFunction;
import org.eclipse.efbt.cocamo.functions.model.functions.FunctionsFactory;
import org.eclipse.efbt.cocamo.smcubes.model.core.CoreFactory;
import org.eclipse.efbt.cocamo.smcubes.model.core.DOMAIN;
import org.eclipse.efbt.cocamo.smcubes.model.core.FACET_VALUE_TYPE;
import org.eclipse.efbt.cocamo.smcubes.model.core.MEMBER;
import org.eclipse.efbt.cocamo.smcubes.model.core.VARIABLE;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.COMBINATION;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.COMBINATION_ITEM;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.CUBE;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.CUBE_STRUCTURE;
import org.eclipse.efbt.cocamo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.CubeModule;
import org.eclipse.efbt.cocamo.smcubes.model.efbt_data_definition.MemberModule;
import org.eclipse.efbt.cocamo.smcubes.model.reports.ReportCell;
import org.eclipse.efbt.cocamo.smcubes.model.smcubes_model.SmcubesModel;
import org.eclipse.efbt.language.trl.component.translator.impl.AttributeLineageUtilImpl;
import org.eclipse.efbt.language.trl.model.domain_model_mapping.DomainMappingModule;
import org.eclipse.efbt.language.trl.model.domain_model_mapping.LeafModuleToReportCellViewModule;
import org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation;
import org.eclipse.efbt.language.trl.model.transformation.VersionedComponentsSet;
import org.eclipse.efbt.language.trl.model.transformation.VersionedCubeSchemaModule;
import org.eclipse.efbt.language.trl.model.transformation.VersionedFunctionalModuleLogic;
import org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellView;
import org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellViewModule;
import org.eclipse.efbt.language.trl.model.trl_sql_views.AggregateEnrichmentView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.BaseViewIncorporatingDeltas;
import org.eclipse.efbt.language.trl.model.trl_sql_views.CastColumnView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.CopyView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.DeltaAccumulation;
import org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.ExplodeArrayOfStructsView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.ExplodeStructView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.FilterByConditionView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.FilterByStructClassColumnView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.JoinView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.MakeArrayOfStructsView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.MakeStructView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsFactory;
import org.eclipse.efbt.language.trl.model.trl_sql_views.UnionView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.VersionedSQLViewsModule;
import org.eclipse.efbt.language.trl.model.trl_sql_views.WhereClause;
import org.eclipse.efbt.lineage.attributelineage.model.advanced_row_transformation_logic.ExplodeArrayOfStructsRowFunction;
import org.eclipse.efbt.lineage.attributelineage.model.advanced_row_transformation_logic.impl.Advanced_row_transformation_logicFactoryImpl;
import org.eclipse.efbt.lineage.attributelineage.model.cube_transformation_logic.CubeTransformationLogic;
import org.eclipse.efbt.lineage.attributelineage.model.cube_transformation_logic.Cube_transformation_logicFactory;
import org.eclipse.efbt.lineage.attributelineage.model.incremental_row_transformation_logic.BaseViewIncorporatingDeltasRowFunction;
import org.eclipse.efbt.lineage.attributelineage.model.incremental_row_transformation_logic.DeltaAccumulationRowFunction;
import org.eclipse.efbt.lineage.attributelineage.model.incremental_row_transformation_logic.impl.Incremental_row_transformation_logicFactoryImpl;
import org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.FilterAndGroupToOneRowCreationApproach;
import org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.FilterRowCreationApproach;
import org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.GroupByRowCreationApproach;
import org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.OneToOneRowCreationApproach;
import org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.RowCreationApproachForCube;
import org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.RowJoinFunction;
import org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.UnionRowCreationApproach;
import org.eclipse.efbt.lineage.attributelineage.model.row_transformation_logic.impl.Row_transformation_logicFactoryImpl;
import org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsFactory;
import org.eclipse.efbt.lineage.common.model.advanced_variable_lineagefunctions.SpeculativeStructColumnParameter;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.AggregateColumnFunction;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.BasicColumnFunction;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunction;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.ColumnFunctionGroup;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicFactory;
//import org.eclipse.efbt.lineage.common.model.column_transformation_logic.BasicColumnFunction;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cube_schema.CubeSchema;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.BaseCube;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.DerivedCube;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.FreeBirdToolsCube;
import org.eclipse.efbt.lineage.common.model.lineagecubes.efbt_advanced_data_definition.ArrayTypedVariable;
import org.eclipse.efbt.lineage.common.model.lineagecubes.efbt_advanced_data_definition.StructTypedVariable;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.CubeColumn;
import org.eclipse.efbt.lineage.common.model.smcubes_functions.MemberParameter;
import org.eclipse.efbt.lineage.common.model.smcubes_functions.Smcubes_functionsFactory;
import org.eclipse.efbt.lineage.common.model.smcubes_functions.ValueParameter;
import org.eclipse.efbt.lineage.common.model.column_transformation_logic.SpeculativeCubeColumnParameter;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;



/**
 * @author Neil Mackenzie
 *
 */
public class TRLUtil {
 

  /**
   * Create a CubeTransformationLogic object form an SQLView.
   * 
   * @param view
   * @param functionalModuleLogicList
   * @param cubeSchemaModuleList
   * @param specialFunctions
   * @return
   */
  public static CubeTransformationLogic translateViewToFunctionalRowLogic(SQLView view,
      EList<VersionedSQLViewsModule> functionalModuleLogicList,
      EList<VersionedCubeSchemaModule> cubeSchemaModuleList, 
      SpecialFunctionSpecs specialFunctions) {


    CubeTransformationLogic cubeTransformationLogic  = Cube_transformation_logicFactory.eINSTANCE.createCubeTransformationLogic();
    ColumnFunctionGroup cfg = Column_transformation_logicFactory.eINSTANCE.createColumnFunctionGroup();
    cubeTransformationLogic .setColumnFunctionGroup(cfg);
   
    
    // columns created by row function
    FreeBirdToolsCube derivedCube = view.getCube();
    setFunctionalCubeLogic(view, cubeTransformationLogic ,
        specialFunctions, functionalModuleLogicList, cubeSchemaModuleList);

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

      EList<SQLView> dependantViews = getTheDependantViews(view, functionalModuleLogicList);
      EList<CubeSchema> dependantSourceTables = getAnySourceCubes(view, cubeSchemaModuleList);

      Iterator<SQLView> dependantViewsIter = dependantViews.iterator();
      Iterator<CubeSchema> dependantSourceTablesIter = dependantSourceTables.iterator();
      while (dependantViewsIter.hasNext()) {
        SQLView dependantView = dependantViewsIter.next();
        EList<CubeColumn> columns = 
            getColumnsFromSQLView(dependantView, functionalModuleLogicList, 
                cubeSchemaModuleList, specialFunctions);
        Iterator<CubeColumn> columnIter = columns.iterator();
        while (columnIter.hasNext()) {
          CubeColumn column = columnIter.next();
          BasicColumnFunction calculatedColumn =
              Column_transformation_logicFactory.eINSTANCE
              .createBasicColumnFunction();
          calculatedColumn.setName(view.getCube().getName() + 
              ":" + column.getVariable().getVariable_id());

          calculatedColumn.setVariable(column.getVariable());
          calculatedColumn.setCube(view.getCube());
          BasicFunction function = FunctionsFactory.eINSTANCE.createBasicFunction();
          function.setFunctionSpec(specialFunctions.copyColumnSpec);
          SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE
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
          BasicColumnFunction calculatedColumn = 
              Column_transformation_logicFactory.eINSTANCE
              .createBasicColumnFunction();
          calculatedColumn.setName(view.getCube().getName() + 
              ":" + column.getVariable_id());
          calculatedColumn.setVariable(column);
          calculatedColumn.setCube(view.getCube());
          BasicFunction function = FunctionsFactory.eINSTANCE.createBasicFunction();
          function.setFunctionSpec(specialFunctions.copyColumnSpec);
          SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE
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
     
      EList<SQLView> dependantViews = getTheDependantViews(view, functionalModuleLogicList);
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
          calculatedColumn.setName(view.getCube().getName() +
              ":" + column.getVariable_id());
          calculatedColumn.setVariable(column);
          calculatedColumn.setCube(view.getCube());
          AggregateFunction function = FunctionsFactory.eINSTANCE.createAggregateFunction();
          function.setFunctionSpec(specialFunctions.firstColumnSpec);

          SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE
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
            calculatedColumn.setName(view.getCube().getName() +
                ":" + column.getVariable_id());
            calculatedColumn.setVariable(column);
            calculatedColumn.setCube(view.getCube());
            AggregateFunction function = FunctionsFactory.eINSTANCE.createAggregateFunction();
            function.setFunctionSpec(specialFunctions.firstColumnSpec);

            SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE
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
      EList<SQLView> dependantViews = getTheDependantViews(view, functionalModuleLogicList);
      EList<CubeSchema> dependantSourceCubes = getAnySourceCubes(view, cubeSchemaModuleList);

      Iterator<SQLView> dependantViewsIter = dependantViews.iterator();
      Iterator<CubeSchema> dependantSourceCubesIter = dependantSourceCubes.iterator();
      while (dependantViewsIter.hasNext()) {
        SQLView dependantView = dependantViewsIter.next();
        EList<CubeColumn> columns =
            getColumnsFromSQLView(dependantView, functionalModuleLogicList, 
                cubeSchemaModuleList, specialFunctions);
        Iterator<CubeColumn> columnIter = columns.iterator();
        while (columnIter.hasNext()) {
          CubeColumn column = columnIter.next();
          // need to check if this is one of the ignored or replaced columns.
          // we could possible do this by reusing the replace function on the main view
          VARIABLE replacedColumn = ReplaceColumnUtil.replaceColumn((JoinView) view, dependantView,
              column.getVariable());
          if (replacedColumn != null) {
            BasicColumnFunction calculatedColumn = 
                Column_transformation_logicFactory.eINSTANCE
                .createBasicColumnFunction();
            calculatedColumn.setName(view.getCube().getName() + 
                ":" + replacedColumn.getVariable_id());

            calculatedColumn.setVariable(replacedColumn);
            calculatedColumn.setCube(view.getCube());
            BasicFunction function = FunctionsFactory.eINSTANCE.createBasicFunction();
            function.setFunctionSpec(specialFunctions.copyColumnSpec);
            SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE
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
            BasicColumnFunction calculatedColumn =
                Column_transformation_logicFactory.eINSTANCE
                .createBasicColumnFunction();
            calculatedColumn.setName(view.getCube().getName() +
                ":" + replacedColumn.getVariable_id());
            calculatedColumn.setVariable(replacedColumn);
            calculatedColumn.setCube(view.getCube());
            BasicFunction function = FunctionsFactory.eINSTANCE.createBasicFunction();
            function.setFunctionSpec(specialFunctions.copyColumnSpec);
            SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE
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

      EList<SQLView> dependantViews = getTheDependantViews(view, functionalModuleLogicList);
      EList<CubeSchema> dependantSourceCubes = getAnySourceCubes(view, cubeSchemaModuleList);

      Iterator<SQLView> dependantViewsIter = dependantViews.iterator();
      Iterator<CubeSchema> dependantSourceTablesIter = dependantSourceCubes.iterator();
      boolean first = true;

      while (dependantViewsIter.hasNext()) {
        SQLView dependantView = dependantViewsIter.next();
        if (first) {
          EList<CubeColumn> theColumns =
              getColumnsFromSQLView(dependantView, functionalModuleLogicList,
              cubeSchemaModuleList, specialFunctions);
          Iterator<CubeColumn> columnIter = theColumns.iterator();
          while (columnIter.hasNext()) {
            CubeColumn column = columnIter.next();
            BasicColumnFunction calculatedColumn =
                Column_transformation_logicFactory.eINSTANCE
                .createBasicColumnFunction();
            calculatedColumn.setName(view.getCube().getName() +
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
          BasicColumnFunction theCalculatedColumn =
              (BasicColumnFunction) theCalculatedColumns.next();
          SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE
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
            BasicColumnFunction calculatedColumn = 
                Column_transformation_logicFactory.eINSTANCE
                .createBasicColumnFunction();
            calculatedColumn.setName(
                view.getCube().getName() + ":" + column.getVariable_id());

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
          BasicColumnFunction theCalculatedColumn = 
              (BasicColumnFunction) theCalculatedColumns.next();
          SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE
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
      EList<SQLView> dependantViews = getTheDependantViews(view, functionalModuleLogicList);
      Iterator<SQLView> dependantViewsIter = dependantViews.iterator();
      ReportCellView dpView = (ReportCellView) view;
      while (dependantViewsIter.hasNext()) {
        SQLView dependantView = dependantViewsIter.next();

        VARIABLE measure = getMeasure(dpView);

        AggregateColumnFunction calculatedColumn = Column_transformation_logicFactory.eINSTANCE
            .createAggregateColumnFunction();
        calculatedColumn.setName(view.getCube().getName() + 
            ":" + measure.getVariable_id());
        calculatedColumn.setVariable(measure);
        calculatedColumn.setCube(view.getCube());
        AggregateFunction function = FunctionsFactory.eINSTANCE.createAggregateFunction();
        function.setFunctionSpec(specialFunctions.sumColumnSpec);

        SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE
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
          calculatedColumnForDimension.setName(view.getCube().getName() + 
              ":" + dimension.getVariable_id());
          calculatedColumnForDimension.setVariable(dimension);
          calculatedColumnForDimension.setCube(view.getCube());
          AggregateFunction functionForDimension =
              FunctionsFactory.eINSTANCE.createAggregateFunction();
          functionForDimension.setFunctionSpec(specialFunctions.firstColumnSpec);
          calculatedColumnForDimension.setAggregateFunction(functionForDimension);

          SpeculativeCubeColumnParameter dimensionColumnParameter = Column_transformation_logicFactory.eINSTANCE
              .createSpeculativeCubeColumnParameter();
          dimensionColumnParameter.setColumn(dimension);
          dimensionColumnParameter.setCube(dependantView.getCube());
          functionForDimension.getParameters().add(dimensionColumnParameter);

          cubeTransformationLogic .getColumnFunctionGroup().
            getColumnFunctions().add(calculatedColumnForDimension);
        }

      }
    }
    
    if(view instanceof DeltaAccumulation) 
    {
    	
    	EList<CubeSchema> dependantSourceTables =  getAnySourceCubes(view, cubeSchemaModuleList);
		
		Iterator<CubeSchema> dependantSourceTablesIter = dependantSourceTables.iterator();
		boolean first = true;
		
		EList<SQLView> dependantViews = getTheDependantViews(view,functionalModuleLogicList);
		
		
		Iterator<SQLView> dependantViewsIter = dependantViews.iterator();

		/**while (dependantViewsIter.hasNext())
		{
			SQLView dependantView = dependantViewsIter.next();
			if (first)
			{
				EList<CubeColumn> theColumns = getColumnsFromSQLView(dependantView, functionalModuleLogicList,cubeSchemaModuleList, specialFunctions);
				 Iterator<CubeColumn>  columnIter  = theColumns.iterator();
				 while(columnIter.hasNext())
				 {
					 CubeColumn column = columnIter.next();
					 BasicColumnFunction calculatedColumn =  Column_transformation_logicFactory.eINSTANCE.createBasicColumnFunction();
					 calculatedColumn.setName(view.getCube().getCube_name()  +":" + column.getVariable().getVariable_id()  );
					 
					 calculatedColumn.setVariable(column.getVariable());
					 calculatedColumn.setCube(view.getCube());
					 BasicFunction function= FunctionsFactory.eINSTANCE.createBasicFunction();
					 function.setFunctionSpec(specialFunctions.copyColumnSpec);
					 calculatedColumn.setBasicFunction(function);
					 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);	
				 }
				 first = false;
			}
			 
			
			Iterator<ColumnFunction> theCalculatedColumns = cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().iterator();
			while (theCalculatedColumns.hasNext())
			{
				
			//for each calculated column we need to add a dependant column with this table
				BasicColumnFunction theCalculatedColumn = (BasicColumnFunction) theCalculatedColumns.next();
				 SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
		
				 columnParameter.setColumn(theCalculatedColumn.getVariable());
				 columnParameter.setCube(dependantView.getCube());
				 theCalculatedColumn.getBasicFunction().getParameters().add(columnParameter);
				 
					
			 }	
			
		}**/	
		while (dependantSourceTablesIter.hasNext())
		{
			CubeSchema dependantSchema= dependantSourceTablesIter.next();
			if (first)
			{
				EList<VARIABLE> theColumns = Util.getColumnsFromCubeSchema(dependantSchema);
				 Iterator<VARIABLE>  columnIter  = theColumns.iterator();
				 while(columnIter.hasNext())
				 {
					 VARIABLE column = columnIter.next();
					 BasicColumnFunction calculatedColumn = Column_transformation_logicFactory.eINSTANCE.createBasicColumnFunction();
					 calculatedColumn.setName(view.getCube().getName()  +":" + column.getVariable_id()  );
					 
					 calculatedColumn.setVariable(column);
					 calculatedColumn.setCube(view.getCube());
					 BasicFunction function= FunctionsFactory.eINSTANCE.createBasicFunction();
					 function.setFunctionSpec(specialFunctions.copyColumnSpec);
					 calculatedColumn.setBasicFunction(function);
					 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);
				 }
				 first = false;
			}
			 
			Iterator<ColumnFunction> theCalculatedColumns = cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().iterator();
			while (theCalculatedColumns.hasNext())
			{				
			     //for each calculated column we need to add a dependant column with this table
				 BasicColumnFunction theCalculatedColumn = (BasicColumnFunction) theCalculatedColumns.next();
				 SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
		
				 columnParameter.setColumn(theCalculatedColumn.getVariable());
				 columnParameter.setCube(dependantSchema.getBaseCube());
				 theCalculatedColumn.getBasicFunction().getParameters().add(columnParameter);
			 }	
			
		}
		
	}

   	
	if(view instanceof BaseViewIncorporatingDeltas) 
	{
		EList<CubeSchema> dependantSourceTables =  getAnySourceCubes(view, cubeSchemaModuleList);
				
		Iterator<CubeSchema> dependantSourceTablesIter = dependantSourceTables.iterator();
		boolean first = true;
		
		EList<SQLView> dependantViews = getTheDependantViews(view,functionalModuleLogicList);
		
		
		Iterator<SQLView> dependantViewsIter = dependantViews.iterator();

		/**while (dependantViewsIter.hasNext())
		{
			SQLView dependantView = dependantViewsIter.next();
			if (first)
			{
				EList<CubeColumn> theColumns = getColumnsFromSQLView(dependantView, functionalModuleLogicList,cubeSchemaModuleList, specialFunctions);
				 Iterator<CubeColumn>  columnIter  = theColumns.iterator();
				 while(columnIter.hasNext())
				 {
					 CubeColumn column = columnIter.next();
					 BasicColumnFunction calculatedColumn =  Column_transformation_logicFactory.eINSTANCE.createBasicColumnFunction();
					 calculatedColumn.setName(view.getCube().getCube_name()  +":" + column.getVariable().getVariable_id()  );
					 
					 calculatedColumn.setVariable(column.getVariable());
					 calculatedColumn.setCube(view.getCube());
					 BasicFunction function= FunctionsFactory.eINSTANCE.createBasicFunction();
					 function.setFunctionSpec(specialFunctions.copyColumnSpec);
					 calculatedColumn.setBasicFunction(function);
					 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);	
				 }
				 first = false;
			}
			 
			
			Iterator<ColumnFunction> theCalculatedColumns = cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().iterator();
			while (theCalculatedColumns.hasNext())
			{
				
			//for each calculated column we need to add a dependant column with this table
				BasicColumnFunction theCalculatedColumn = (BasicColumnFunction) theCalculatedColumns.next();
				 SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
		
				 columnParameter.setColumn(theCalculatedColumn.getVariable());
				 columnParameter.setCube(dependantView.getCube());
				 theCalculatedColumn.getBasicFunction().getParameters().add(columnParameter);
				 
					
			 }	
			
		}	*/
		while (dependantSourceTablesIter.hasNext())
		{
			CubeSchema dependantSchema= dependantSourceTablesIter.next();
			if (first)
			{
				EList<VARIABLE> theColumns = Util.getColumnsFromCubeSchema(dependantSchema);
				 Iterator<VARIABLE>  columnIter  = theColumns.iterator();
				 while(columnIter.hasNext())
				 {
					 VARIABLE column = columnIter.next();
					 BasicColumnFunction calculatedColumn = Column_transformation_logicFactory.eINSTANCE.createBasicColumnFunction();
					 calculatedColumn.setName(view.getCube().getName()  +":" + column.getVariable_id()  );
					 
					 calculatedColumn.setVariable(column);
					 calculatedColumn.setCube(view.getCube());
					 BasicFunction function= FunctionsFactory.eINSTANCE.createBasicFunction();
					 function.setFunctionSpec(specialFunctions.copyColumnSpec);
					 calculatedColumn.setBasicFunction(function);
					 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);
				 }
				 first = false;
			}
			 
			Iterator<ColumnFunction> theCalculatedColumns = cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().iterator();
			while (theCalculatedColumns.hasNext())
			{				
			     //for each calculated column we need to add a dependant column with this table
				 BasicColumnFunction theCalculatedColumn = (BasicColumnFunction) theCalculatedColumns.next();
				 SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
		
				 columnParameter.setColumn(theCalculatedColumn.getVariable());
				 columnParameter.setCube(dependantSchema.getBaseCube());
				 theCalculatedColumn.getBasicFunction().getParameters().add(columnParameter);
			 }	
			
		}
		
	}
	
	if(view instanceof CastColumnView)
	{
		//mostly copies, but with a change of columnn on one of them,
		
		EList<SQLView> dependantViews = getTheDependantViews(view, functionalModuleLogicList);
		EList<CubeSchema> dependantSourceTables =  getAnySourceCubes(view,   cubeSchemaModuleList);
		
		Iterator<SQLView> dependantViewsIter = dependantViews.iterator();
		Iterator<CubeSchema> dependantSourceTablesIter = dependantSourceTables.iterator();
		 StructTypedVariable sourceColumn = ((CastColumnView) view).getSourceStructVariable();
		 StructTypedVariable targetColumn = ((CastColumnView) view).getTargetStructVariable();
		while (dependantViewsIter.hasNext())
		{
			SQLView dependantView = dependantViewsIter.next();
			 EList<CubeColumn> columns = getColumnsFromSQLView(dependantView,  functionalModuleLogicList, cubeSchemaModuleList,specialFunctions);
			 Iterator<CubeColumn>  columnIter  = columns.iterator();
			 while(columnIter.hasNext())
			 {
				 CubeColumn column = columnIter.next();
				 BasicColumnFunction calculatedColumn = Column_transformation_logicFactory.eINSTANCE.createBasicColumnFunction();
				 if(column.equals(sourceColumn))
				 {
					 calculatedColumn.setVariable(targetColumn);
					 calculatedColumn.setName(view.getCube().getName()  +":" + targetColumn.getVariable_id()  );
					 calculatedColumn.setCube(view.getCube());
				 }
				 else
				 {
					 calculatedColumn.setVariable(column.getVariable());
					 calculatedColumn.setName(view.getCube().getName()  +":" + column.getVariable().getVariable_id()  );						
					 calculatedColumn.setCube(view.getCube());
				 }
				
				 BasicFunction function= FunctionsFactory.eINSTANCE.createBasicFunction();
				 function.setFunctionSpec(specialFunctions.copyColumnSpec);
				 SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
	
				 columnParameter.setColumn(column.getVariable());
				 columnParameter.setCube(dependantView.getCube());
				 function.getParameters().add(columnParameter);
				 calculatedColumn.setBasicFunction(function);
				 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);	
			 }				 			
		}
		
		while (dependantSourceTablesIter.hasNext())
		{
			CubeSchema dependantSchema= dependantSourceTablesIter.next();
			 EList<VARIABLE> columns = Util.getColumnsFromCubeSchema(dependantSchema);
			 Iterator<VARIABLE>  columnIter  = columns.iterator();
			 while(columnIter.hasNext())
			 {
				 VARIABLE column = columnIter.next();
				 BasicColumnFunction calculatedColumn =  Column_transformation_logicFactory.eINSTANCE.createBasicColumnFunction();
				 calculatedColumn.setName(view.getCube().getName()  +":" + column.getVariable_id()  );
				 calculatedColumn.setVariable(column);
				 calculatedColumn.setCube(view.getCube());
				 BasicFunction function= FunctionsFactory.eINSTANCE.createBasicFunction();
				 function.setFunctionSpec(specialFunctions.copyColumnSpec);
				 SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
	
				 columnParameter.setColumn(column);
				 columnParameter.setCube(dependantSchema.getBaseCube());
				 function.getParameters().add(columnParameter);
				 calculatedColumn.setBasicFunction(function);
				 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);	
			 }				 			
		}
		
	}
	
	if(view instanceof MakeStructView)
	{
		FreeBirdToolsCube dependantTable1 =((DerivedCube) (view.getCube())).getSourceCubes().get(0);
		
		CubeSchema dependantSchema1 = null;
		
		SQLView dependantView1 = null;
		
		
		//loop through the context and get any tabledefs which are linked to the high level table 
		//Iterator<EObject> iter1 =  allContextsContents(context);
		Iterator<SQLView> viewsIter = getAllViews(functionalModuleLogicList).iterator();
		Iterator<CubeSchema> schemaIter = Util.getSchemas(cubeSchemaModuleList).iterator();
		FreeBirdToolsCube hlItem = null;
		while (viewsIter.hasNext())
		{
			SQLView tableDefinitionItem = viewsIter.next();
		
				hlItem= ((SQLView) tableDefinitionItem).getCube();
							
				if (hlItem.equals(dependantTable1))
					dependantView1 = (SQLView) tableDefinitionItem;
				
		}
		while (schemaIter.hasNext())
		{
			CubeSchema tableDefinitionItem = schemaIter.next();
			
				hlItem= ((CubeSchema) tableDefinitionItem).getBaseCube();
							
				if (hlItem.equals(dependantTable1))
					dependantSchema1 = (CubeSchema)  tableDefinitionItem;

		}
		
		 StructTypedVariable theRowsCol1 = ((MakeStructView) view ).getTargetVariable();
		 BasicColumnFunction calculatedColumn1 = Column_transformation_logicFactory.eINSTANCE.createBasicColumnFunction();
		 calculatedColumn1.setName(view.getCube().getName()  +":" + theRowsCol1.getVariable_id()   );
		 calculatedColumn1.setVariable(theRowsCol1);
		 calculatedColumn1.setCube(view.getCube());
		 BasicFunction function1= FunctionsFactory.eINSTANCE.createBasicFunction();
		 function1.setFunctionSpec(specialFunctions.makeRowSpec);
		 calculatedColumn1.setBasicFunction(function1);
		 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn1);	
		 
		
		 
		 
		 
		if(dependantView1 !=null)
		{
			EList<CubeColumn> columns = getColumnsFromSQLView(dependantView1,  functionalModuleLogicList, cubeSchemaModuleList,specialFunctions);
			 Iterator<CubeColumn>  columnIter  = columns.iterator();
			
			 while(columnIter.hasNext())
			 {
				 CubeColumn column = columnIter.next();
				 //need to check if this is one of the ignored or replaced columns.
				 //we could possible do this by reusing the replace function on the main view
				 CubeColumn replacedColumn = column;
				 if(replacedColumn != null)
				 {

			
					 SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
					 columnParameter.setColumn(column.getVariable());
					 columnParameter.setCube(dependantView1.getCube());
					 function1.getParameters().add(columnParameter);

				}
			 }	
		}
		
		if(dependantSchema1 !=null)
		{
			 EList<VARIABLE> columns = Util.getColumnsFromCubeSchema(dependantSchema1);
			 Iterator<VARIABLE>  columnIter  = columns.iterator();
			 while(columnIter.hasNext())
			 {
				 VARIABLE column = columnIter.next();
				 VARIABLE replacedColumn = column;
				 if(replacedColumn != null)
				 {
					 
					 SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
				
					 columnParameter.setColumn(column);
					 columnParameter.setCube(dependantSchema1.getBaseCube());
					 function1.getParameters().add(columnParameter);
					
				 }
			 }		
		}
		
		 
	
		
	
		
	}
	
	if(view instanceof FilterByStructClassColumnView)
	{
	
		EList<SQLView> dependantViews = getTheDependantViews(view, functionalModuleLogicList);
		EList<CubeSchema> dependantSourceTables =  getAnySourceCubes(view,  cubeSchemaModuleList);
		
		Iterator<SQLView> dependantViewsIter = dependantViews.iterator();
		Iterator<CubeSchema> dependantSourceTablesIter = dependantSourceTables.iterator();
		StructTypedVariable sourceColumn = ((FilterByStructClassColumnView) view).getStructColumn();
		StructTypedVariable targetColumn = ((FilterByStructClassColumnView) view).getRenameAsStructColumn();
		while (dependantViewsIter.hasNext())
		{
			SQLView dependantView = dependantViewsIter.next();
			 EList<CubeColumn> columns = getColumnsFromSQLView(dependantView,  functionalModuleLogicList, cubeSchemaModuleList,specialFunctions);
			 Iterator<CubeColumn>  columnIter  = columns.iterator();
			 while(columnIter.hasNext())
			 {
				 CubeColumn column = columnIter.next();
				 BasicColumnFunction calculatedColumn = Column_transformation_logicFactory.eINSTANCE.createBasicColumnFunction();
				 if(column.equals(sourceColumn))
				 {
					 calculatedColumn.setVariable(targetColumn);
					 calculatedColumn.setName(view.getCube().getName()  +":" + targetColumn.getVariable_id()  );
					 calculatedColumn.setCube(view.getCube());
				 }
				 else
				 {
					 calculatedColumn.setVariable(column.getVariable());
					 calculatedColumn.setName(view.getCube().getName()  +":" + column.getVariable().getVariable_id()  );						
					 calculatedColumn.setCube(view.getCube());
				 }
				
				 BasicFunction function= FunctionsFactory.eINSTANCE.createBasicFunction();
				 function.setFunctionSpec(specialFunctions.copyColumnSpec);
				 SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
		
				 columnParameter.setColumn(column.getVariable());
				 columnParameter.setCube(dependantView.getCube());
				 function.getParameters().add(columnParameter);
				 calculatedColumn.setBasicFunction(function);
				 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);	
			 }				 			
		}
		
		while (dependantSourceTablesIter.hasNext())
		{
			CubeSchema dependantSchema= dependantSourceTablesIter.next();
			 EList<VARIABLE> columns = Util.getColumnsFromCubeSchema(dependantSchema);
			 Iterator<VARIABLE>  columnIter  = columns.iterator();
			 while(columnIter.hasNext())
			 {
				 VARIABLE column = columnIter.next();
				 BasicColumnFunction calculatedColumn = Column_transformation_logicFactory.eINSTANCE.createBasicColumnFunction();
				 calculatedColumn.setName(view.getCube().getName()  +":" + column.getVariable_id()  );
				 calculatedColumn.setVariable(column);
				 calculatedColumn.setCube(view.getCube());
				 BasicFunction function= FunctionsFactory.eINSTANCE.createBasicFunction();
				 function.setFunctionSpec(specialFunctions.copyColumnSpec);
				 SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
				
				 columnParameter.setColumn(column);
				 columnParameter.setCube(dependantSchema.getBaseCube());
				 function.getParameters().add(columnParameter);
				 calculatedColumn.setBasicFunction(function);
				 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);	
			 }				 			
		}
		
		
	}
	
	
	if(view instanceof MakeArrayOfStructsView)
	{
		FreeBirdToolsCube dependantTable1 =((DerivedCube) (view.getCube())).getSourceCubes().get(0);
		CubeSchema dependantSchema1 = null;
		
		MakeArrayOfStructsView makeRowSetsTransformtaion = (MakeArrayOfStructsView) view;
		StructTypedVariable rowTypeColumn = makeRowSetsTransformtaion.getSourceStructVariable();
		VARIABLE groupingColumn = makeRowSetsTransformtaion.getGroupingVariable();
		
		 ArrayTypedVariable theRowsCol = makeRowSetsTransformtaion.getTargetArrayVariable();
		 AggregateColumnFunction calculatedColumn = Column_transformation_logicFactory.eINSTANCE.createAggregateColumnFunction();
		 
		 calculatedColumn.setName(view.getCube().getName()  +":" + theRowsCol.getVariable_id()   );
		 calculatedColumn.setVariable(theRowsCol);
		 calculatedColumn.setCube(view.getCube());
		 AggregateFunction function= FunctionsFactory.eINSTANCE.createAggregateFunction();
		 function.setFunctionSpec(specialFunctions.makeRowSetSpec);
		 calculatedColumn.setAggregateFunction(function);
		 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);	
		 
		 AggregateColumnFunction calculatedColumn2 = Column_transformation_logicFactory.eINSTANCE.createAggregateColumnFunction();
		
		 calculatedColumn2.setName(view.getCube().getName()  +":" + groupingColumn.getVariable_id()  );
		 calculatedColumn2.setVariable(groupingColumn);
		 calculatedColumn2.setCube(view.getCube());
		 AggregateFunction function2= FunctionsFactory.eINSTANCE.createAggregateFunction();
		 function2.setFunctionSpec(specialFunctions.firstColumnSpec);
		 
		 SpeculativeStructColumnParameter rowColumnParameter = Advanced_variable_lineagefunctionsFactory.eINSTANCE.createSpeculativeStructColumnParameter();
		 rowColumnParameter.setColumnInsideStruct(groupingColumn);
		 rowColumnParameter.setStructColumn(rowTypeColumn);
		 rowColumnParameter.setCube(dependantTable1);
		 
		
		 function2.getParameters().add(rowColumnParameter);
		 calculatedColumn2.setAggregateFunction(function2);
		 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn2);	
		
		
		
	}

	
	if(view instanceof ExplodeArrayOfStructsView)
	{
		// 
		
		EList<SQLView> dependantViews = getTheDependantViews(view, functionalModuleLogicList);
		EList<CubeSchema> dependantSourceTables =  getAnySourceCubes(view,   cubeSchemaModuleList);
		
		Iterator<SQLView> dependantViewsIter = dependantViews.iterator();
		Iterator<CubeSchema> dependantSourceTablesIter = dependantSourceTables.iterator();
		ArrayTypedVariable sourceColumn = ((ExplodeArrayOfStructsView) view).getArraySourceColumn();
		
		while (dependantViewsIter.hasNext())
		{
			SQLView dependantView = dependantViewsIter.next();
			 EList<CubeColumn> columns = getColumnsFromSQLView(dependantView, functionalModuleLogicList,cubeSchemaModuleList, specialFunctions);
			 Iterator<CubeColumn>  columnIter  = columns.iterator();
			 while(columnIter.hasNext())
			 {
				 CubeColumn column = columnIter.next();
				 BasicColumnFunction calculatedColumn = Column_transformation_logicFactory.eINSTANCE.createBasicColumnFunction();
				 if(column.equals(sourceColumn))
				 {
					//do nothing
				 }
				 else
				 {
					 calculatedColumn.setVariable(column.getVariable());
					 calculatedColumn.setName(view.getCube().getName()  +":" + column.getVariable().getVariable_id()  );						
					 calculatedColumn.setCube(view.getCube());
				 }
				
				 BasicFunction function= FunctionsFactory.eINSTANCE.createBasicFunction();
				 function.setFunctionSpec(specialFunctions.copyColumnSpec);
				 SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
		
				 columnParameter.setColumn(column.getVariable());
				 columnParameter.setCube(dependantView.getCube());
				 function.getParameters().add(columnParameter);
				 calculatedColumn.setBasicFunction(function);
				 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);	
			 }	
			 
			//create a calculated column, I guessit should have a seperate name,
				//
				StructTypedVariable targetColumn = ((ExplodeArrayOfStructsView) view).getTargetStructColumn();
				 BasicColumnFunction calculatedColumn = Column_transformation_logicFactory.eINSTANCE.createBasicColumnFunction();
				 calculatedColumn.setVariable(targetColumn);
				 calculatedColumn.setName(view.getCube().getName()  +":" + targetColumn.getVariable_id()  );						
				 calculatedColumn.setCube(view.getCube());
				 BasicFunction function= FunctionsFactory.eINSTANCE.createBasicFunction();
				 function.setFunctionSpec(specialFunctions.getRowNofColY);
				
				 SpeculativeCubeColumnParameter columnParameter = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
			
				 columnParameter.setColumn(sourceColumn);
				 columnParameter.setCube(dependantView.getCube());
				 function.getParameters().add(columnParameter);
				 //need to revisit this use of Index 
				 VARIABLE ci = ((ExplodeArrayOfStructsView) view).getIndexColumn();
				 SpeculativeCubeColumnParameter columnParameter2 = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();
				
				 columnParameter2.setColumn(ci);
				 columnParameter2.setCube(view.getCube());
				 function.getParameters().add(columnParameter2);
				 
				 calculatedColumn.setBasicFunction(function);
				 cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().add(calculatedColumn);	
			 
			 
		}
		
		
		 
		 
		
		
		
		
		
		
		
	}
	if(view instanceof ExplodeStructView)
	{
		// 
		//for each column in the table associated with the row, get the columns
		// for each of those columns make a calculated colum with that name
		// and create copy function which takes a qualified rowtypeColumnparameter.
		// for each other column just create copy function (is there a risk of having the same column twice?)
		// all of this must exist in some kind of data science language l(winding, unwinding ,packing) or
		// mongo aggregation pipeline.
		
	}
	
	

    return cubeTransformationLogic ;
  }

  /**
   * Set the RowCreationApproachForCube of a cubeTransformationLogic .
   * 
   * @param view
   * @param cubeTransformationLogic 
   * @param specialFunctions
   * @param functionalModuleLogicList
   * @param cubeSchemaModuleList
   */
  private static void setFunctionalCubeLogic(SQLView view, CubeTransformationLogic cubeTransformationLogic ,
      SpecialFunctionSpecs specialFunctions, EList<VersionedSQLViewsModule> functionalModuleLogicList,
      EList<VersionedCubeSchemaModule> cubeSchemaModuleList) {
    
    if ((view instanceof CopyView) || (view instanceof EnrichmentView) || (view instanceof ExplodeStructView)  || (view instanceof MakeStructView)) {
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
      SQLView dependantView = getTheDependantViews(view, functionalModuleLogicList).get(0);
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

        SpeculativeCubeColumnParameter param = Column_transformation_logicFactory.eINSTANCE.createSpeculativeCubeColumnParameter();

        param.setColumn(dimValue.getVariable_id());
        param.setCube(dependantView.getCube());
        bf2.getParameters().add(param);

        MemberParameter memberparam = Smcubes_functionsFactory.eINSTANCE.createMemberParameter();
        memberparam.setParam(dimValue.getMember_id());
        bf2.getParameters().add(memberparam);

        rowFilterAndGroupFunction.setFilterFunction(bf2);

      }
           rowCreationApproachForCube.setRowCreationApproach(rowFilterAndGroupFunction);
      rowCreationApproachForCube.setCube(view.getCube());
      rowCreationApproachForCube.setName(view.getName());
      cubeTransformationLogic .setRowCreationApproachForCube(rowCreationApproachForCube);

    }
    if(view instanceof DeltaAccumulation )
	{
		RowCreationApproachForCube tableLogic = Row_transformation_logicFactoryImpl.eINSTANCE.createRowCreationApproachForCube();
		 DeltaAccumulationRowFunction rowDAFunction = Incremental_row_transformation_logicFactoryImpl.eINSTANCE.createDeltaAccumulationRowFunction();
		 rowDAFunction.setUltimateCubeSource(((DeltaAccumulation)view).getUltimateSourceCube());
		
		tableLogic.setRowCreationApproach(rowDAFunction);
		tableLogic.setCube(view.getCube());
	//	tableLogic.setUltimateCubeSource(((DeltaAccumulation)view).getUltimateSourceCube());
		tableLogic.setName(view.getName());
		cubeTransformationLogic.setRowCreationApproachForCube(tableLogic);
		
	}
	if(view instanceof BaseViewIncorporatingDeltas )
	{
		RowCreationApproachForCube tableLogic = Row_transformation_logicFactoryImpl.eINSTANCE.createRowCreationApproachForCube();
		 BaseViewIncorporatingDeltasRowFunction rowbvFunction = Incremental_row_transformation_logicFactoryImpl.eINSTANCE.createBaseViewIncorporatingDeltasRowFunction();
		 rowbvFunction.setUltimateCubeSource((BaseCube) ((BaseViewIncorporatingDeltas)view).getUltimateSourceCube());
		tableLogic.setRowCreationApproach(rowbvFunction);
		tableLogic.setCube(view.getCube());
	//	tableLogic.setUltimateCubeSource(((BaseViewIncorporatingDeltas)view).getUltimateSourceCube());
		tableLogic.setName(view.getName());
		cubeTransformationLogic.setRowCreationApproachForCube(tableLogic);
		
	}


    if (view instanceof UnionView) {
      RowCreationApproachForCube rowCreationApproachForCube = Row_transformation_logicFactoryImpl.eINSTANCE.createRowCreationApproachForCube();
      UnionRowCreationApproach urf = Row_transformation_logicFactoryImpl.eINSTANCE.createUnionRowCreationApproach();

      rowCreationApproachForCube.setRowCreationApproach(urf);
      rowCreationApproachForCube.setCube(view.getCube());
      rowCreationApproachForCube.setName(view.getName());
      cubeTransformationLogic .setRowCreationApproachForCube(rowCreationApproachForCube);

    }
    
    if( view instanceof FilterByStructClassColumnView)
	{
		SQLView dependantView = getTheDependantViews(view, functionalModuleLogicList).get(0);
		RowCreationApproachForCube tableLogic = Row_transformation_logicFactoryImpl.eINSTANCE.createRowCreationApproachForCube();
		 FilterRowCreationApproach rowFilterFunction = Row_transformation_logicFactoryImpl.eINSTANCE.createFilterRowCreationApproach();
		WhereClause whereClause = Trl_sql_viewsFactory.eINSTANCE.createWhereClause();
		 BasicFunction bf = FunctionsFactory.eINSTANCE.createBasicFunction();
		 BasicFunctionSpec bfSpec = specialFunctions.equalsSpec;
		 SpeculativeStructColumnParameter param1 = Advanced_variable_lineagefunctionsFactory.eINSTANCE.createSpeculativeStructColumnParameter();
		 
		  VARIABLE classColumn = CoreFactory.eINSTANCE.createVARIABLE();
		 classColumn.setName("class");
		param1.setColumnInsideStruct(classColumn ); //class
		 param1.setStructColumn(((FilterByStructClassColumnView)view ).getStructColumn());
		 //should be source dependant table surely?
		 param1.setCube(dependantView.getCube());
		 
		ValueParameter param2 = Smcubes_functionsFactory.eINSTANCE.createValueParameter();		
		// ValueParameter tv = FunctionsFactory.eINSTANCE.createValueParameter();
		param2.setValue(((FilterByStructClassColumnView) view).getStructClass().getName());
		param2.setDataType(FACET_VALUE_TYPE.STRING);
		
		
		bf.setFunctionSpec(bfSpec);
		bf.getParameters().add(param1);
		bf.getParameters().add(param2);
		
		//create the function which is a where on the class struct member
		rowFilterFunction.setFilterFunction(whereClause.getFunction());
		//rowFilterFunction.setFilterFunction();
		tableLogic.setRowCreationApproach(rowFilterFunction);
		tableLogic.setCube(view.getCube());
		tableLogic.setName(view.getName());
		cubeTransformationLogic.setRowCreationApproachForCube(tableLogic);
		
	}
	if( view instanceof ExplodeArrayOfStructsView)
	{
		RowCreationApproachForCube tableLogic = Row_transformation_logicFactoryImpl.eINSTANCE.createRowCreationApproachForCube();
		  ExplodeArrayOfStructsRowFunction rf =  Advanced_row_transformation_logicFactoryImpl.eINSTANCE.createExplodeArrayOfStructsRowFunction();
		 
		   ArrayTypedVariable arraySourceColumn = ((ExplodeArrayOfStructsView)view ).getArraySourceColumn();
		  rf.setArraySourceVariable(arraySourceColumn);
		  
		  
		  
		 
	
		  CubeColumn tc = Column_transformation_logicFactory.eINSTANCE.createCubeColumn();
		  VARIABLE indexColumn = ((ExplodeArrayOfStructsView)view ).getIndexColumn();
		  tc.setVariable(indexColumn);
		  tc.setCube(view.getCube());
		  tc.setName(indexColumn.getVariable_id() + ":" + view.getCube().getName());
		 // rf.setIndexColumn(tc);
		  rf.getCreatedCubeColumns().add(tc);
		 
		  tableLogic.setRowCreationApproach(rf);
			tableLogic.setCube(view.getCube());
			tableLogic.setName(view.getName());
			cubeTransformationLogic.setRowCreationApproachForCube(tableLogic);
			
		
	}
	if(view instanceof MakeArrayOfStructsView )
	{
		RowCreationApproachForCube tableLogic = Row_transformation_logicFactoryImpl.eINSTANCE.createRowCreationApproachForCube();
		 GroupByRowCreationApproach groupByFunction = Row_transformation_logicFactoryImpl.eINSTANCE.createGroupByRowCreationApproach();
		 //needs to change we need our own row function for this which knows struct column and columnInStruct.
		 groupByFunction.getGroupByColumns().add((((MakeArrayOfStructsView)view).getSourceStructVariable()));
		tableLogic.setRowCreationApproach(groupByFunction);
		tableLogic.setCube(view.getCube());
		tableLogic.setName(view.getName());
		cubeTransformationLogic.setRowCreationApproachForCube(tableLogic);
		
	}

  }

  /**
   * Return the columns associated with an SQLView.
   * @param view
   * @param functionalModuleLogicList
   * @param cubeSchemaModuleList
   * @param specialFunctions
   * @return
   */
  public static EList<CubeColumn> getColumnsFromSQLView(SQLView view,
      EList<VersionedSQLViewsModule> functionalModuleLogicList,
      EList<VersionedCubeSchemaModule> cubeSchemaModuleList, SpecialFunctionSpecs specialFunctions) {
    CubeTransformationLogic rowLogic = translateViewToFunctionalRowLogic(view, functionalModuleLogicList,
        cubeSchemaModuleList, specialFunctions);
    return AttributeLineageUtilImpl.getColumnsFromFunctionalRowLogic(rowLogic);
  }

  /**
   * Return the Views dependant upon a view
   * @param view
   * @param functionalModuleLogicList
   * @return
   */
  public static EList<SQLView> getTheDependantViews(SQLView view,
      EList<VersionedSQLViewsModule> functionalModuleLogicList) {

    BasicEList<SQLView> dependantViews = new BasicEList<SQLView>();


    EList<FreeBirdToolsCube> dependantCubes = ((DerivedCube) (view.getCube())).getSourceCubes();
    Iterator<FreeBirdToolsCube> dependantCubesIter = dependantCubes.iterator();

    while (dependantCubesIter.hasNext()) {
      FreeBirdToolsCube dtd = dependantCubesIter.next();
   
      EList<SQLView> viewList = getAllViews(functionalModuleLogicList);
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
   * et all the SQLViews associated with a  list of VersionedFunctionalModuleLogics
   * @param functionalModuleLogicList
   * @return
   */
  private static EList<SQLView> getAllViews(EList<VersionedSQLViewsModule> functionalModuleLogicList) {
    EList<SQLView> returnList = new BasicEList<SQLView>();
    for (Iterator iterator = functionalModuleLogicList.iterator(); iterator.hasNext();) {
      VersionedFunctionalModuleLogic dataSetTransformationModule = (VersionedFunctionalModuleLogic) iterator
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
	   * Returns the SMCubesModel
	   * @param o
	   * @return
	   */
	  public static EList<ModuleDependency> getDependantModules( EObject o) {
	    return  ((Module) o.eContainer()).getDependencies().getTheModules();

	  }
  
  public static EList<CUBE_STRUCTURE_ITEM> getCubeStructureItemsFromDefaultBIRDModel(CubeSchema schema)
  {
	  CUBE cube = schema.getCube();
	 
	    
	    CUBE_STRUCTURE cubeStructure = cube.getCube_structure_id();
	    EList<ModuleDependency> dependantModules = getDependantModules(cubeStructure);
	     
	    EList<CUBE_STRUCTURE_ITEM> returnlist = new BasicEList<CUBE_STRUCTURE_ITEM>();
	    for (ModuleDependency moduleDependency : dependantModules) {
	    	Module module = moduleDependency.getTheModule();
	    	if (module instanceof CubeModule)
	    	{
	    		CubeModule cubeModule = (CubeModule) module;
				EList<CUBE_STRUCTURE_ITEM> items = cubeModule.getCubeStructureItems();
				for (Iterator iterator2 = items.iterator(); iterator2.hasNext();) {
					CUBE_STRUCTURE_ITEM cube_STRUCTURE_ITEM = (CUBE_STRUCTURE_ITEM) iterator2.next();
					if(cube_STRUCTURE_ITEM.getCube_structure_id().equals(cubeStructure))
					{
						returnlist.add(cube_STRUCTURE_ITEM);
					}
					
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
  public static DataSetTransformation getReportCellViewForCell(ReportCell reportCell, VersionedComponentsSet componentSet) {

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
	 * LeafFunctionalityModule.
	 * 
	 * @param functionalModule
	 * @return
	 */
	public static VersionedCubeSchemaModule getDefaultCubeSchemaModuleForFunctionalityModule(
			LeafFunctionalityModule functionalModule) {

		VersionedCubeSchemaModule returnModule = null;

		/**EList<FunctionalityModuleTag> returnlist = new BasicEList<FunctionalityModuleTag>();
		NavigationContext nc = getDefaultNavigationContext(functionalModule);

		

		DomainMappingModule domainMapping = nc.getDomainModelMapping();
		EList<LeafModuleToVersionedCubeSchemaModule> csm = domainMapping.getLeafModuleToVersionedCubeSchemaModules();

		for (LeafModuleToVersionedCubeSchemaModule leafModuleToVersionedCubeSchemaModule : csm) {
			if (leafModuleToVersionedCubeSchemaModule.getScheme().equals(functionalModule)) {
				returnModule = leafModuleToVersionedCubeSchemaModule.getModule();
			}
		}*/

		return returnModule;
	}

  
  public static EList<MEMBER> getMembers(
	      DOMAIN domain) {

	   EList<MEMBER> members = new BasicEList<MEMBER>();
	   
	  
	   EList<ModuleDependency> dependentModules = getDependantModules(domain);
	   
	  
	for (ModuleDependency moduleDependency : dependentModules) {
		
		   Module module = moduleDependency.getTheModule();
		   if(module instanceof MemberModule)
		   {
			   MemberModule memberModule = (MemberModule) module; 
			  
			   EList<MEMBER> birdMembers = memberModule.getMembers();
			   
			   for (Iterator iterator2 = birdMembers.iterator(); iterator2.hasNext();) {
					MEMBER member = (MEMBER) iterator2.next();
					
					if (member.getDomain_id().equals(domain))
						members.add(member);
					
				}
		   }
	
	   }
	    
	  
	   
	   return members;
	   
	  }
  
  

 
/**  public static EList<FunctionalityModuleTag> getFunctionalityModuleTagsFromDefualtTagGroup(
	      FunctionalityModule functionalModule) {
	    EList<FunctionalityModuleTag> returnlist = new BasicEList<FunctionalityModuleTag>();
	    NavigationContext nc = getDefaultNavigationContext(functionalModule);
	    TagGroup tg = nc.getTags();
	    EList<Tag> tags = tg.getTags();
	    for (Iterator iterator = tags.iterator(); iterator.hasNext();) {
	      Tag tag = (Tag) iterator.next();

	      if (tag instanceof FunctionalityModuleTag) {
	        if (((FunctionalityModuleTag) tag).getTransformation().getName().equals(functionalModule.getName())) {
	          returnlist.add((FunctionalityModuleTag) tag);
	        }

	      }

	    }*/


  /**
	 * Find the LeafFunctionalityModule associated a reportCell in the context of a
	 * VersionedComponentsSet.
	 * 
	 * @param reportCell
	 * @param context
	 * @return
	 */
	static LeafFunctionalityModule findSchemeForReportCellView(ReportCell reportCell, VersionedComponentsSet context, DomainMappingModule mappingModule) {
		// find the scheme with the cell
		EList<ReportCellViewModule> rcm = context.getReportCellViewModules();
		LeafFunctionalityModule returnScheme = null;
		for (Iterator iterator = rcm.iterator(); iterator.hasNext();) {
			ReportCellViewModule reportCellViewModule = (ReportCellViewModule) iterator.next();
			EList<ReportCellView> rcvm = reportCellViewModule.getReportCellViews();
			for (Iterator iterator2 = rcvm.iterator(); iterator2.hasNext();) {
				ReportCellView reportCellView = (ReportCellView) iterator2.next();
				ReportCell cell = reportCellView.getReportCell();
				if (cell.equals(reportCell)) {
					
					returnScheme = getFunctionalityModuleForReportCellViewModule(reportCellViewModule, mappingModule); 
				}

			}
		}
		return returnScheme;
	}
	
	private static LeafFunctionalityModule getFunctionalityModuleForReportCellViewModule(
			ReportCellViewModule reportCellViewModule, DomainMappingModule mappingModule) {
		
		LeafFunctionalityModule returnLeafFunctionalityModule = null;
		  EList<LeafModuleToReportCellViewModule> mappings = mappingModule
				.getLeafModuleToReportCellViewModules();
		for (LeafModuleToReportCellViewModule leafModuleToReportCellViewModule : mappings) {
			if (leafModuleToReportCellViewModule.getModule().getName().equals(reportCellViewModule.getName())) {
				returnLeafFunctionalityModule = leafModuleToReportCellViewModule.getScheme();
			}
		}
		return returnLeafFunctionalityModule;
	}


}
