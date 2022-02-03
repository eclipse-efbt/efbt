package org.eclipse.efbt.cocalimo.computation.query.computation_lineage;

import java.util.Iterator;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.efbt.cocalimo.computation.model.attribute_lineage.AttributeLineageModel;
import org.eclipse.efbt.cocalimo.computation.model.cube_transformation_logic.CubeTransformationLogic;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity;
import org.eclipse.efbt.cocalimo.computation.model.row_transformation_logic.UnionRowCreationApproach;

public class AttributeLineageModelQuery {

	/**
	   * Returns the BaseRowStrucures which the cubeTransformationLogic  depends upon.
	   * @param cubeTransformationLogic 
	   * @return
	   */
	  public static EList<EClass> getTheDependantEClasss(CubeTransformationLogic cubeTransformationLogic ) {

	    BasicEList<EClass> dependantEClasss = new BasicEList<EClass>();
	    AttributeLineageModel attributeModel = (AttributeLineageModel) cubeTransformationLogic .eContainer();
	   
	    EClass cube = cubeTransformationLogic.getRowCreationApproachForentity().getEntity();
	    EList<EClass> dependentDerivedCubes = ((DerivedEntity) cube).getSourceEntities();
	    Iterator<EClass> dependantDerivedCubesIter = dependentDerivedCubes.iterator();
	    while (dependantDerivedCubesIter.hasNext()) {
	      EClass derivedCube = dependantDerivedCubesIter.next();
	      Iterator<BaseEntity> baseRowsStructureIter = attributeModel.getBaseSchemas().iterator();
	      while (baseRowsStructureIter.hasNext()) {
	    	  BaseEntity baseEntity = baseRowsStructureIter.next();

	        if (baseEntity.equals(derivedCube))
	          dependantEClasss.add(baseEntity);

	      }
	    }
	    return dependantEClasss;

	  }
	  
	  /**
	   *  Returns the FunctionalRowLogics which the cubeTransformationLogic  depends upon.
	   * @param cubeTransformationLogic 
	   * @return
	   */
	  public static EList<CubeTransformationLogic> getTheDependantFunctionalRowLogics(CubeTransformationLogic cubeTransformationLogic ) {
	    BasicEList<CubeTransformationLogic> dependantFunctionalRowLogic = new BasicEList<CubeTransformationLogic>();
	    AttributeLineageModel attributeLineageModel = (AttributeLineageModel) cubeTransformationLogic .eContainer();
	    boolean isUnionFunction = (cubeTransformationLogic .getRowCreationApproachForentity().getRowCreationApproach() instanceof UnionRowCreationApproach);
	    boolean oneAdded = false;

	    EClass derivedCube1 = cubeTransformationLogic .getRowCreationApproachForentity().getEntity();
	    EList<EClass> dependantCubes = ((DerivedEntity) derivedCube1).getSourceEntities();
	    Iterator<EClass> dependantCubesIter = dependantCubes.iterator();
	    
	    while (dependantCubesIter.hasNext()) {
	      EClass cube2 = dependantCubesIter.next();
	      Iterator<CubeTransformationLogic> iter = attributeLineageModel.getRowTransformations().iterator();
	      EClass cube3 = null;
	      while (iter.hasNext()) {
	        CubeTransformationLogic rowlogic = iter.next();

	        cube3 = rowlogic.getRowCreationApproachForentity().getEntity();

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
