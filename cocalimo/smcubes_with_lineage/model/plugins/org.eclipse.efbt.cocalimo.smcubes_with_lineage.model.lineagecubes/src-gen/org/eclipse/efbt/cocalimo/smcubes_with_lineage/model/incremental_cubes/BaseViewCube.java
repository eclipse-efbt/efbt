/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.BaseCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.DerivedCube;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base View Cube</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * BaseViewCube will represents the application of compounded Deltas to a BaseCube.
 * Its structure will be the same as the associated BaseCube.
 * BaseViewCube is a kind of DerivedCube. Th source cubes of a BaseViewCube are OneBaseCube, and one DeltaAccumulationCube.
 * BaseViewCube is the source of further Derived Cubes which apply the relevant busines logic for useful calculations, all detla accumulation for the BaseCube will already have been done in the source of the BaseViewCube.
 * 
 * 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseViewCube#getUltimateBaseCube <em>Ultimate Base Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesPackage#getBaseViewCube()
 * @model
 * @generated
 */
public interface BaseViewCube extends DerivedCube
{
	/**
	 * Returns the value of the '<em><b>Ultimate Base Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The related BaseCube
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Ultimate Base Cube</em>' reference.
	 * @see #setUltimateBaseCube(BaseCube)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesPackage#getBaseViewCube_UltimateBaseCube()
	 * @model
	 * @generated
	 */
	BaseCube getUltimateBaseCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.BaseViewCube#getUltimateBaseCube <em>Ultimate Base Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Ultimate Base Cube</em>' reference.
	 * @see #getUltimateBaseCube()
	 * @generated
	 */
	void setUltimateBaseCube(BaseCube value);

} // BaseViewCube
