/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Base Cell With Value</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * Base Cell data which contains a value (e.g. price)
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseCellWithValue#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getBaseCellWithValue()
 * @model
 * @generated
 */
public interface BaseCellWithValue extends BaseCell
{
	/**
	 * Returns the value of the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The value of the cell represented as a String
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Value</em>' attribute.
	 * @see #setValue(String)
	 * @see org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getBaseCellWithValue_Value()
	 * @model extendedMetaData="kind='attribute' name='value'"
	 * @generated
	 */
	String getValue();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseCellWithValue#getValue <em>Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Value</em>' attribute.
	 * @see #getValue()
	 * @generated
	 */
	void setValue(String value);

} // BaseCellWithValue
