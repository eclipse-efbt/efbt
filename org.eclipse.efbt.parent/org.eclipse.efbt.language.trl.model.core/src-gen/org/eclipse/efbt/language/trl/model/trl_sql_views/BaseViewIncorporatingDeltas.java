/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views;

import org.eclipse.efbt.lineage.common.model.cubes.FreeBirdToolsCube;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base View Incorporating Deltas</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.BaseViewIncorporatingDeltas#getUltimateSourceCube <em>Ultimate Source Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getBaseViewIncorporatingDeltas()
 * @model
 * @generated
 */
public interface BaseViewIncorporatingDeltas extends SQLView
{
	/**
	 * Returns the value of the '<em><b>Ultimate Source Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Ultimate Source Cube</em>' reference.
	 * @see #setUltimateSourceCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getBaseViewIncorporatingDeltas_UltimateSourceCube()
	 * @model
	 * @generated
	 */
	FreeBirdToolsCube getUltimateSourceCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.BaseViewIncorporatingDeltas#getUltimateSourceCube <em>Ultimate Source Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Ultimate Source Cube</em>' reference.
	 * @see #getUltimateSourceCube()
	 * @generated
	 */
	void setUltimateSourceCube(FreeBirdToolsCube value);

} // BaseViewIncorporatingDeltas
