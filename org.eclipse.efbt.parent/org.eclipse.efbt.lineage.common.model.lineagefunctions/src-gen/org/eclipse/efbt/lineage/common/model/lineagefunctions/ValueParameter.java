/**
 */
package org.eclipse.efbt.lineage.common.model.lineagefunctions;

import org.eclipse.efbt.common.model.functions.Parameter;

import org.eclipse.efbt.data_structures.smcubes.model.core.FACET_VALUE_TYPE;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Value Parameter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Parameter which is a value 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.ValueParameter#getDataType <em>Data Type</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.ValueParameter#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getValueParameter()
 * @model extendedMetaData="name='ValueParameter' kind='empty'"
 * @generated
 */
public interface ValueParameter extends Parameter
{
	/**
	 * Returns the value of the '<em><b>Data Type</b></em>' attribute.
	 * The literals are from the enumeration {@link org.eclipse.efbt.data_structures.smcubes.model.core.FACET_VALUE_TYPE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * the values datatype
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Data Type</em>' attribute.
	 * @see org.eclipse.efbt.data_structures.smcubes.model.core.FACET_VALUE_TYPE
	 * @see #setDataType(FACET_VALUE_TYPE)
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getValueParameter_DataType()
	 * @model extendedMetaData="kind='attribute' name='dataType'"
	 * @generated
	 */
	FACET_VALUE_TYPE getDataType();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.ValueParameter#getDataType <em>Data Type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Data Type</em>' attribute.
	 * @see org.eclipse.efbt.data_structures.smcubes.model.core.FACET_VALUE_TYPE
	 * @see #getDataType()
	 * @generated
	 */
	void setDataType(FACET_VALUE_TYPE value);

	/**
	 * Returns the value of the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The value as a string representation
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Value</em>' attribute.
	 * @see #setValue(String)
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getValueParameter_Value()
	 * @model extendedMetaData="kind='attribute' name='value'"
	 * @generated
	 */
	String getValue();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.ValueParameter#getValue <em>Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Value</em>' attribute.
	 * @see #getValue()
	 * @generated
	 */
	void setValue(String value);

} // ValueParameter
