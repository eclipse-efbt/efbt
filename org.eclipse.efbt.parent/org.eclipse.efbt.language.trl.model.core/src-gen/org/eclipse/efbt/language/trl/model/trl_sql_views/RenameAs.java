/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views;

import org.eclipse.efbt.data_structures.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.lineage.common.model.cubes.FreeBirdToolsCube;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Rename As</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * Column to rename in the resulting View. similar to AS in SQL. Particularly useful if a Join View would result in 2 columns with the same name
 *       
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs#getFromCube <em>From Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs#getFromVariable <em>From Variable</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs#getToVariable <em>To Variable</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getRenameAs()
 * @model extendedMetaData="name='RenameAs' kind='empty'"
 * @generated
 */
public interface RenameAs extends EObject
{
	/**
	 * Returns the value of the '<em><b>From Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The source cube containing the Column
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>From Cube</em>' reference.
	 * @see #setFromCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getRenameAs_FromCube()
	 * @model extendedMetaData="kind='attribute' name='fromCube'"
	 * @generated
	 */
	FreeBirdToolsCube getFromCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs#getFromCube <em>From Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>From Cube</em>' reference.
	 * @see #getFromCube()
	 * @generated
	 */
	void setFromCube(FreeBirdToolsCube value);

	/**
	 * Returns the value of the '<em><b>From Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the Column from the source cube that we choose  to Rename in the resulting View
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>From Variable</em>' reference.
	 * @see #setFromVariable(VARIABLE)
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getRenameAs_FromVariable()
	 * @model extendedMetaData="kind='attribute' name='fromVariable'"
	 * @generated
	 */
	VARIABLE getFromVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs#getFromVariable <em>From Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>From Variable</em>' reference.
	 * @see #getFromVariable()
	 * @generated
	 */
	void setFromVariable(VARIABLE value);

	/**
	 * Returns the value of the '<em><b>To Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * What Column would we like to use instead.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>To Variable</em>' reference.
	 * @see #setToVariable(VARIABLE)
	 * @see org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage#getRenameAs_ToVariable()
	 * @model extendedMetaData="kind='attribute' name='toVariable'"
	 * @generated
	 */
	VARIABLE getToVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs#getToVariable <em>To Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>To Variable</em>' reference.
	 * @see #getToVariable()
	 * @generated
	 */
	void setToVariable(VARIABLE value);

} // RenameAs
