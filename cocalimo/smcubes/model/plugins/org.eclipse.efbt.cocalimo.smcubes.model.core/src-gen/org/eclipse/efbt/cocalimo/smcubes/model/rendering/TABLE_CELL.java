/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.rendering;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.COMBINATION;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>TABLE CELL</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getCell_id <em>Cell id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getCombination_id <em>Combination id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getIs_shaded <em>Is shaded</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getTable_id <em>Table id</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getTABLE_CELL()
 * @model extendedMetaData="name='TABLE_CELL' kind='empty'"
 * @generated
 */
public interface TABLE_CELL extends EObject
{
	/**
	 * Returns the value of the '<em><b>Cell id</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cell id</em>' attribute.
	 * @see #setCell_id(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getTABLE_CELL_Cell_id()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='cell_id'"
	 * @generated
	 */
	String getCell_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getCell_id <em>Cell id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cell id</em>' attribute.
	 * @see #getCell_id()
	 * @generated
	 */
	void setCell_id(String value);

	/**
	 * Returns the value of the '<em><b>Combination id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Combination id</em>' reference.
	 * @see #setCombination_id(COMBINATION)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getTABLE_CELL_Combination_id()
	 * @model extendedMetaData="kind='attribute' name='combination_id'"
	 * @generated
	 */
	COMBINATION getCombination_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getCombination_id <em>Combination id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Combination id</em>' reference.
	 * @see #getCombination_id()
	 * @generated
	 */
	void setCombination_id(COMBINATION value);

	/**
	 * Returns the value of the '<em><b>Is shaded</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is shaded</em>' attribute.
	 * @see #setIs_shaded(Boolean)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getTABLE_CELL_Is_shaded()
	 * @model extendedMetaData="kind='attribute' name='is_shaded'"
	 * @generated
	 */
	Boolean getIs_shaded();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getIs_shaded <em>Is shaded</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is shaded</em>' attribute.
	 * @see #getIs_shaded()
	 * @generated
	 */
	void setIs_shaded(Boolean value);

	/**
	 * Returns the value of the '<em><b>Table id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Table id</em>' reference.
	 * @see #setTable_id(TABLE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getTABLE_CELL_Table_id()
	 * @model extendedMetaData="kind='attribute' name='table_id'"
	 * @generated
	 */
	TABLE getTable_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE_CELL#getTable_id <em>Table id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Table id</em>' reference.
	 * @see #getTable_id()
	 * @generated
	 */
	void setTable_id(TABLE value);

} // TABLE_CELL
