/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.DerivedCube;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Delta Accumulation Cube</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * DeltaAccumulationCube accumulates a set of Deltas. It is a kind of DerivedCube, its source cubes will always be iether a single BaseDeltaCube, or A BaseDeltaCube and anotherDeltaAccumulationCube.
 * By following the trail of source DeltaAccumulationCubes, we can see the set of compounded BaseDeltas that are to be applied to a BaseCube. The BaseViewCube will represents the application of these compounded Deltas to a BaseCube.
 * 
 * <!-- end-model-doc -->
 *
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesPackage#getDeltaAccumulationCube()
 * @model
 * @generated
 */
public interface DeltaAccumulationCube extends DerivedCube
{
} // DeltaAccumulationCube
