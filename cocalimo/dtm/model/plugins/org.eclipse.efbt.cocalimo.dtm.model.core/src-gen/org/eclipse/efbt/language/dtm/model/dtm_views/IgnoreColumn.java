/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Ignore Column</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 *  Column to Ignore. Used in a JoinView if we dont want all the columns from the joined cubes to be in the resulting Views cube structure.
 *       
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn#getColumnToIgnore <em>Column To Ignore</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn#getCube <em>Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getIgnoreColumn()
 * @model extendedMetaData="name='IgnoreColumn' kind='empty'"
 * @generated
 */
public interface IgnoreColumn extends EObject {
	/**
	 * Returns the value of the '<em><b>Column To Ignore</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the column to Ignore
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Column To Ignore</em>' reference.
	 * @see #setColumnToIgnore(VARIABLE)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getIgnoreColumn_ColumnToIgnore()
	 * @model extendedMetaData="kind='attribute' name='columnToIgnore'"
	 * @generated
	 */
	VARIABLE getColumnToIgnore();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn#getColumnToIgnore <em>Column To Ignore</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column To Ignore</em>' reference.
	 * @see #getColumnToIgnore()
	 * @generated
	 */
	void setColumnToIgnore(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the cube with the column to ignore.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage#getIgnoreColumn_Cube()
	 * @model extendedMetaData="kind='attribute' name='cube'"
	 * @generated
	 */
	FreeBirdToolsCube getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(FreeBirdToolsCube value);

} // IgnoreColumn
