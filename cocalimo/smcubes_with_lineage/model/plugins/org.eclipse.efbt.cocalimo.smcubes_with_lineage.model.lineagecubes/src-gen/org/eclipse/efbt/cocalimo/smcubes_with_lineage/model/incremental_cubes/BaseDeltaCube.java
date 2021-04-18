/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.BaseCube;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base Delta Cube</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * Base Delta Cube represetns a cube holding Delta information, this is rows to be added, or deleted or amended to a cube.
 * It has an associated BaseCube to show the type of cube it is amending.
 * Multiple Deltas are accumulated using a Delta Accumulation Cube
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseDeltaCube#getUltimateBaseCube <em>Ultimate Base Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesPackage#getBaseDeltaCube()
 * @model
 * @generated
 */
public interface BaseDeltaCube extends BaseCube
{
	/**
	 * Returns the value of the '<em><b>Ultimate Base Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The type of cube that is being amended
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Ultimate Base Cube</em>' reference.
	 * @see #setUltimateBaseCube(BaseCube)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesPackage#getBaseDeltaCube_UltimateBaseCube()
	 * @model
	 * @generated
	 */
	BaseCube getUltimateBaseCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseDeltaCube#getUltimateBaseCube <em>Ultimate Base Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Ultimate Base Cube</em>' reference.
	 * @see #getUltimateBaseCube()
	 * @generated
	 */
	void setUltimateBaseCube(BaseCube value);

} // BaseDeltaCube
