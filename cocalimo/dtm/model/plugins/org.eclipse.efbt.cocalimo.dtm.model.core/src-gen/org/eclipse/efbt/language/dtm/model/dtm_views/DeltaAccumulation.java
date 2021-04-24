/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Delta Accumulation</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.DeltaAccumulation#getUltimateSourceCube <em>Ultimate Source Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getDeltaAccumulation()
 * @model
 * @generated
 */
public interface DeltaAccumulation extends DTMView {
	/**
	 * Returns the value of the '<em><b>Ultimate Source Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Ultimate Source Cube</em>' reference.
	 * @see #setUltimateSourceCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getDeltaAccumulation_UltimateSourceCube()
	 * @model
	 * @generated
	 */
	FreeBirdToolsCube getUltimateSourceCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.DeltaAccumulation#getUltimateSourceCube <em>Ultimate Source Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Ultimate Source Cube</em>' reference.
	 * @see #getUltimateSourceCube()
	 * @generated
	 */
	void setUltimateSourceCube(FreeBirdToolsCube value);

} // DeltaAccumulation
