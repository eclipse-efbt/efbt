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
package org.eclipse.efbt.controller.trl.component.translator.impl.util;

import java.util.Iterator;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;
import org.eclipse.efbt.controller.trl.component.translator.api.AttributeLineageUtil;
import org.eclipse.efbt.controller.trl.component.translator.impl.AttributeLineageUtilImpl;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.ResolvedStructColumnParameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.attribute_lineage.AttributeLineageModel;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.attribute_lineage.Attribute_lineageFactory;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.attribute_lineage.IsNotSubset;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.attribute_lineage.IsSubset;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.AggregateColumnFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.BasicColumnFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ColumnFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ColumnFunctionGroup;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.CubeColumn;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.ResolvedCubeColumnParameter;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_transformation_logic.CubeTransformationLogic;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationshipModule;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.TargetCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariable;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.BaseRowStructure;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.RowCreationApproach;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;

/**
 * @author Neil Mackenzie
 *
 */
public class ComparisonUtil {

	/**
	 * Compares 2 AttributeLineageModels, the call has a firstModel and secondModel
	 * attribute For for every CubeTransformationLogic, BaseRowStucture and
	 * CubeColumn in the first model, we find it in the resulting model, and set
	 * usedInSubSet for those items only.
	 * 
	 * @param call
	 */
	public static ALMComparisonTuple compareAttributeLineageModels(AttributeLineageModel firstModel,
			AttributeLineageModel secondModel) {
		// resulting model is a copy of second model
		AttributeLineageModel resultingModel = EcoreUtil.copy(secondModel);
		AttributeLineageModel notIncludedModel = Attribute_lineageFactory.eINSTANCE.createAttributeLineageModel();
		boolean isSubset = true;

		// ensure that usedInSubSet is set to false initially for everything on second
		// model
		setUsedInSubsetToFalseForAll(resultingModel);
		// for everything in first model, find it in the resulting model, and set
		// usedInSubSet
		// to be true for that thing, find may need a special compare method.
		// if it cannot be found add it to the not included model.
		EList<CubeTransformationLogic> rowTransformations = firstModel.getRowTransformations();
		for (Iterator iterator = rowTransformations.iterator(); iterator.hasNext();) {
			CubeTransformationLogic cubeTransformationLogic = (CubeTransformationLogic) iterator.next();
			CubeTransformationLogic frl2 = AttributeLineageUtilImpl
					.findFunctionalRowLogicInAttributeModel(cubeTransformationLogic, resultingModel);
			CubeTransformationLogic frl2Copy = EcoreUtil.copy(frl2);
			if (frl2 == null) {
				notIncludedModel.getRowTransformations().add(EcoreUtil.copy(cubeTransformationLogic));
				isSubset = false;
			} else {
				frl2.setUsedInSubsets(true);
				// set useIntrails for columns in resultingModel which are also in first model
				EList<ColumnFunction> columns = cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions();
				for (Iterator iterator2 = columns.iterator(); iterator2.hasNext();) {
					ColumnFunction columnFunction = (ColumnFunction) iterator2.next();
					ColumnFunction columnFunction2 = AttributeLineageUtilImpl
							.findColumnFunctionInFunctionalRowLogic(columnFunction, frl2);
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

		EList<BaseRowStructure> baseSchema = firstModel.getBaseSchemas();
		for (Iterator iterator = baseSchema.iterator(); iterator.hasNext();) {
			BaseRowStructure baseRowStructure = (BaseRowStructure) iterator.next();
			BaseRowStructure baseRowStructure2 = AttributeLineageUtilImpl
					.findBaseRowStructureInAttributeModel(baseRowStructure, resultingModel);
			BaseRowStructure baseRowStructure2Copy = EcoreUtil.copy(baseRowStructure2);
			if (baseRowStructure2 == null) {
				notIncludedModel.getBaseSchemas().add(EcoreUtil.copy(baseRowStructure));
				isSubset = false;
			} else {
				baseRowStructure2.setUsedInSubsets(true);
				EList<CubeColumn> columns = baseRowStructure.getColumns();
				for (Iterator iterator2 = columns.iterator(); iterator2.hasNext();) {
					CubeColumn column = (CubeColumn) iterator2.next();
					CubeColumn column2 = AttributeLineageUtilImpl.findColumninSchema(column, baseRowStructure2);
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

		ALMComparisonTuple tuple = new ALMComparisonTuple();
		if (isSubset) {
			IsSubset subset = Attribute_lineageFactory.eINSTANCE.createIsSubset();
			tuple.subsetBoolean = subset;
		} else {
			IsNotSubset subset = Attribute_lineageFactory.eINSTANCE.createIsNotSubset();
			tuple.subsetBoolean = subset;
		}

		tuple.notIncludedModel = notIncludedModel;
		tuple.resultingALM = resultingModel;
		return tuple;

	}

	/**
	 * Set usedInSubset equal to true for every CubeTransformationLogic,
	 * BaseRowStucture and CubeColumn in the model
	 * 
	 * @param model
	 */
	private static void setUsedInSubsetToFalseForAll(AttributeLineageModel model) {

		EList<CubeTransformationLogic> rowTransformations = model.getRowTransformations();
		for (Iterator iterator = rowTransformations.iterator(); iterator.hasNext();) {
			CubeTransformationLogic cubeTransformationLogic = (CubeTransformationLogic) iterator.next();
			cubeTransformationLogic.setUsedInSubsets(false);
			EList<ColumnFunction> columns = cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions();
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
	public static void markAttributeLineageComponentsAsUsedInSubSet(AttributeLineageModel attributeLineageModel,
			Iterator<TargetCube> targetCubes, FreeBIRDToolsCubeHierarchyRelationshipModule hmodule) {

		while (targetCubes.hasNext()) {
			TargetCube targetCube = targetCubes.next();
			CubeTransformationLogic rowLogic = AttributeLineageUtilImpl.getFunctionalRowLogicForCube(targetCube,
					attributeLineageModel);

			// get the columns
			Iterator<ColumnFunction> columns = rowLogic.getColumnFunctionGroup().getColumnFunctions().iterator();
			while (columns.hasNext()) {
				ColumnFunction column = columns.next();
				markColumnAsUsedInSubset(column, attributeLineageModel, hmodule);
			}

		}
	}

	/**
	 * * For all dependent items of a column, set isUsedInSubset to true.
	 * 
	 * @param column
	 * @param attributeLineageModel
	 * @param call
	 */
	private static void markColumnAsUsedInSubset(CubeColumn column, AttributeLineageModel attributeLineageModel,
			FreeBIRDToolsCubeHierarchyRelationshipModule hmodule) {

		EObject o = column.eContainer();
		column.setUsedInSubsets(true);
		if (o instanceof RowCreationApproach) {
			setDependantColsFromRowCreationApproachAsUsedInSubset((RowCreationApproach) o, attributeLineageModel,
					hmodule);

		}
		if (o instanceof ColumnFunctionGroup) {
			CubeTransformationLogic frl = (CubeTransformationLogic) o.eContainer();
			frl.setUsedInSubsets(true);
			setDependantColsFromRowCreationApproachAsUsedInSubset(
					frl.getRowCreationApproachForCube().getRowCreationApproach(), attributeLineageModel, hmodule);
			ColumnFunction func = (ColumnFunction) column;
			if (func instanceof BasicColumnFunction) {

				if (((BasicColumnFunction) func).getBasicFunction().getFunctionSpec().getName()
						.equals("MakeRow")) {
					// do nothing yet..note there might be a weird case where no columns are needed.
				} else {
					// get all the resolved cube columns and set as used
					TreeIterator<EObject> allContents = func.eAllContents();
					while (allContents.hasNext()) {
						o = allContents.next();
						if (o instanceof ResolvedCubeColumnParameter) {
							markColumnAsUsedInSubset(((ResolvedCubeColumnParameter) o).getCubeColumn(),
									attributeLineageModel, hmodule);
						}
						if (o instanceof ResolvedStructColumnParameter) {
							ResolvedStructColumnParameter sc = (ResolvedStructColumnParameter) o;
							markColumnAsUsedInSubset(((ResolvedStructColumnParameter) o).getCubeColumn(),
									attributeLineageModel, hmodule);
							markColumnFromStructAsUsed(
									((StructTypedVariable) sc.getCubeColumn().getVariable()).getCubeType(),
									((ResolvedStructColumnParameter) o).getColumnInsideStruct(), attributeLineageModel,
									hmodule);
						}

					}
				}
			}
			if (func instanceof AggregateColumnFunction) {

				// get all the resolved cube columns and set as used
				TreeIterator<EObject> allContents = func.eAllContents();
				while (allContents.hasNext()) {
					o = allContents.next();
					if (o instanceof ResolvedCubeColumnParameter) {
						markColumnAsUsedInSubset(((ResolvedCubeColumnParameter) o).getCubeColumn(),
								attributeLineageModel, hmodule);
					}
					if (o instanceof ResolvedStructColumnParameter) {
						ResolvedStructColumnParameter sc = (ResolvedStructColumnParameter) o;
						markColumnAsUsedInSubset(((ResolvedStructColumnParameter) o).getCubeColumn(),
								attributeLineageModel, hmodule);
						markColumnFromStructAsUsed(
								((StructTypedVariable) sc.getCubeColumn().getVariable()).getCubeType(),
								((ResolvedStructColumnParameter) o).getColumnInsideStruct(), attributeLineageModel,
								hmodule);
					}

				}

			}

		}
		if (o instanceof BaseRowStructure) {
			((BaseRowStructure) o).setUsedInSubsets(true);
		}

	}

	private static void markColumnFromStructAsUsed(FreeBirdToolsCube cube, VARIABLE column,
			AttributeLineageModel program, FreeBIRDToolsCubeHierarchyRelationshipModule hmodule) {

		Iterator<BaseRowStructure> sourceTables = getBaseRowStructuresWithTable(cube, program, hmodule);

		while (sourceTables.hasNext()) {

			BaseRowStructure sourceTable = sourceTables.next();
			sourceTable.setUsedInSubsets(true);
			// get the cols, and set the correct one as used
			Iterator<CubeColumn> colIter = sourceTable.getColumns().iterator();
			while (colIter.hasNext()) {
				CubeColumn col = colIter.next();
				if (col.getVariable().equals(column)) {
					col.setUsedInSubsets(true);
				}

			}
		}

	}

	private static Iterator<BaseRowStructure> getBaseRowStructuresWithTable(FreeBirdToolsCube cube,
			AttributeLineageModel program, FreeBIRDToolsCubeHierarchyRelationshipModule hmodule) {
		// use the hierarchy to get all possible source table
		EList<BaseRowStructure> retunSchemas = new BasicEList<BaseRowStructure>();
		BaseRowStructure srs = getBaseRowStructureWithTable(cube, program);
		if (srs != null)
			retunSchemas.add(srs);

		// FreeBIRDToolsCubeHierarchyRelationshipModule hmodule =
		// call.getTransformationContext().getCube_hierarchy_module();
		Iterator<FreeBIRDToolsCubeHierarchyRelationship> relIter = hmodule.getHierarchyRelationships().iterator();
		while (relIter.hasNext()) {
			FreeBIRDToolsCubeHierarchyRelationship rel = relIter.next();
			FreeBirdToolsCube supCube = rel.getSuperCube();
			FreeBirdToolsCube subCube = rel.getSubCube();
			// this should be ameded for multiple level hierarchies
			if (supCube.equals(cube)) {
				BaseRowStructure subschema = getBaseRowStructureWithTable(subCube, program);
				if (subschema != null)
					retunSchemas.add(subschema);
			}

		}
		return retunSchemas.iterator();

	}

	private static BaseRowStructure getBaseRowStructureWithTable(FreeBirdToolsCube table,
			AttributeLineageModel program) {
		// TODO Auto-generated method stub
		Iterator<BaseRowStructure> rowSchemaIter = program.getBaseSchemas().iterator();
		BaseRowStructure returnRowSchema = null;
		while (rowSchemaIter.hasNext()) {
			BaseRowStructure rowSchema = rowSchemaIter.next();
			FreeBirdToolsCube rowSchemaTable = rowSchema.getCube();
			if (rowSchemaTable.equals(table)) {
				returnRowSchema = rowSchema;
			}
		}
		return returnRowSchema;
	}

	/**
	 * Set the DependantCols From a RowCreationApproach As UsedInSubset
	 * 
	 * @param rowCreationApproach
	 * @param attributeLineageModel
	 * @param call
	 */
	private static void setDependantColsFromRowCreationApproachAsUsedInSubset(RowCreationApproach rowCreationApproach,
			AttributeLineageModel attributeLineageModel, FreeBIRDToolsCubeHierarchyRelationshipModule hmodule) {

		Iterator<ResolvedCubeColumnParameter> dependantCubeCols = ((RowCreationApproach) rowCreationApproach)
				.getDependantCubeColumns().iterator();

		while (dependantCubeCols.hasNext()) {
			ResolvedCubeColumnParameter tc = dependantCubeCols.next();
			markColumnAsUsedInSubset(tc.getCubeColumn(), attributeLineageModel, hmodule);

		}

		Iterator<ResolvedStructColumnParameter> dependantStructCols = ((RowCreationApproach) rowCreationApproach)
				.getDependantStructItemColumns().iterator();

		while (dependantStructCols.hasNext()) {
			ResolvedStructColumnParameter sc = dependantStructCols.next();
			markColumnAsUsedInSubset(sc.getCubeColumn(), attributeLineageModel, hmodule);
			markColumnFromStructAsUsed(((StructTypedVariable) sc.getCubeColumn().getVariable()).getCubeType(),
					sc.getColumnInsideStruct(), attributeLineageModel, hmodule);

		}

	}

	/**
	 * Deletes every CubeTransformationLogic, BaseRowStucture and CubeColumn in the
	 * attributeLineageModel which does not have usedInSubset equal to true.
	 * 
	 * @param attributeLineageModel
	 */
	public static void trimComponentsMarkedInTrail(AttributeLineageModel attributeLineageModel) {
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

		EList<CubeTransformationLogic> functionalRowLogics = attributeLineageModel.getRowTransformations();
		BasicEList<CubeTransformationLogic> functionalRowLogicsToRemove = new BasicEList<CubeTransformationLogic>();
		for (Iterator iterator = functionalRowLogics.iterator(); iterator.hasNext();) {
			{
				CubeTransformationLogic cubeTransformationLogic = (CubeTransformationLogic) iterator.next();
				if (!cubeTransformationLogic.isUsedInSubsets())
					functionalRowLogicsToRemove.add(cubeTransformationLogic);
				else {
					EList<ColumnFunction> columnFuncsToRemove = new BasicEList<ColumnFunction>();
					EList<ColumnFunction> columnfuncs = cubeTransformationLogic.getColumnFunctionGroup()
							.getColumnFunctions();
					for (Iterator iterator2 = columnfuncs.iterator(); iterator2.hasNext();) {
						ColumnFunction columnFunction = (ColumnFunction) iterator2.next();
						if (!columnFunction.isUsedInSubsets())
							columnFuncsToRemove.add(columnFunction);

					}
					removeColumnfunctionsFromFunctionalRowLogic(cubeTransformationLogic, columnFuncsToRemove);
				}
			}
		}
		removeFunctionalRowLogicsFromModel(attributeLineageModel, functionalRowLogicsToRemove);
	}

	/**
	 * Remove a CubeTransformationLogic from an AttributeModel.
	 * 
	 * @param attributeLineageModel
	 * @param functionalRowLogicsToRemove
	 */
	private static void removeFunctionalRowLogicsFromModel(AttributeLineageModel attributeLineageModel,
			BasicEList<CubeTransformationLogic> functionalRowLogicsToRemove) {

		for (Iterator iterator = functionalRowLogicsToRemove.iterator(); iterator.hasNext();) {
			CubeTransformationLogic cubeTransformationLogic = (CubeTransformationLogic) iterator.next();
			attributeLineageModel.getRowTransformations().remove(cubeTransformationLogic);
		}

	}

	/**
	 * Remove a ColumnFunction from a cubeTransformationLogic item.
	 * 
	 * @param cubeTransformationLogic
	 * @param columnFuncsToRemove
	 */
	private static void removeColumnfunctionsFromFunctionalRowLogic(CubeTransformationLogic cubeTransformationLogic,
			EList<ColumnFunction> columnFuncsToRemove) {

		for (Iterator iterator = columnFuncsToRemove.iterator(); iterator.hasNext();) {
			ColumnFunction columnFunction = (ColumnFunction) iterator.next();
			cubeTransformationLogic.getColumnFunctionGroup().getColumnFunctions().remove(columnFunction);

		}

	}

	/**
	 * Removes a BaseRowStructure from an AttributeLineageModel.
	 * 
	 * @param attributeLineageModel
	 * @param baseRowStructuresToRemove
	 */
	private static void removeBaseRowStructuresFromModel(AttributeLineageModel attributeLineageModel,
			EList<BaseRowStructure> baseRowStructuresToRemove) {

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
	private static void removeColumnsFrowBaseRowStructure(BaseRowStructure baseRowStructure,
			EList<CubeColumn> columnsToRemove) {

		for (Iterator iterator = columnsToRemove.iterator(); iterator.hasNext();) {
			CubeColumn cubeColumn = (CubeColumn) iterator.next();
			baseRowStructure.getColumns().remove(cubeColumn);
		}

	}

}
