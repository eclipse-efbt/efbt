/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base Cell With Enumerated Value</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * Base Cell data which contains one member of a list of enumerated values (e.g. a currency)
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCellWithEnumeratedValue#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getBaseCellWithEnumeratedValue()
 * @model
 * @generated
 */
public interface BaseCellWithEnumeratedValue extends BaseCell
{
	/**
	 * Returns the value of the '<em><b>Value</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The member
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Value</em>' reference.
	 * @see #setValue(MEMBER)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getBaseCellWithEnumeratedValue_Value()
	 * @model
	 * @generated
	 */
	MEMBER getValue();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseCellWithEnumeratedValue#getValue <em>Value</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Value</em>' reference.
	 * @see #getValue()
	 * @generated
	 */
	void setValue(MEMBER value);

} // BaseCellWithEnumeratedValue
