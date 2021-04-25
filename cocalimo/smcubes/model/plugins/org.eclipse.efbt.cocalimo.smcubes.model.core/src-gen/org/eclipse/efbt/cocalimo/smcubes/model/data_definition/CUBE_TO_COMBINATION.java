/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>CUBE TO COMBINATION</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION#getCombination_id <em>Combination id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION#getCube_id <em>Cube id</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_TO_COMBINATION()
 * @model extendedMetaData="name='CUBE_TO_COMBINATION' kind='empty'"
 * @generated
 */
public interface CUBE_TO_COMBINATION extends EObject
{
	/**
	 * Returns the value of the '<em><b>Combination id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Combination id</em>' reference.
	 * @see #setCombination_id(COMBINATION)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_TO_COMBINATION_Combination_id()
	 * @model extendedMetaData="kind='attribute' name='combination_id'"
	 * @generated
	 */
	COMBINATION getCombination_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION#getCombination_id <em>Combination id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Combination id</em>' reference.
	 * @see #getCombination_id()
	 * @generated
	 */
	void setCombination_id(COMBINATION value);

	/**
	 * Returns the value of the '<em><b>Cube id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube id</em>' reference.
	 * @see #setCube_id(CUBE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage#getCUBE_TO_COMBINATION_Cube_id()
	 * @model extendedMetaData="kind='attribute' name='cube_id'"
	 * @generated
	 */
	CUBE getCube_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_TO_COMBINATION#getCube_id <em>Cube id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube id</em>' reference.
	 * @see #getCube_id()
	 * @generated
	 */
	void setCube_id(CUBE value);

} // CUBE_TO_COMBINATION
