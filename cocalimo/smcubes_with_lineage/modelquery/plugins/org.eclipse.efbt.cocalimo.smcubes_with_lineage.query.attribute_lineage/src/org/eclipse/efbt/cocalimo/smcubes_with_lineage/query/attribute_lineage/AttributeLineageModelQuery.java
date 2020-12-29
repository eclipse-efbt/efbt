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
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.query.attribute_lineage;

import java.util.Iterator;


import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.attribute_lineage.AttributeLineageModel;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_transformation_logic.CubeTransformationLogic;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.DerivedCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.BaseRowStructure;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.UnionRowCreationApproach;



public class AttributeLineageModelQuery {

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
}
