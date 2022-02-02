/**
 */
package entity_functions;

import functions.Parameter;

import org.eclipse.emf.ecore.EDataType;

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
 *   <li>{@link entity_functions.ValueParameter#getValue <em>Value</em>}</li>
 *   <li>{@link entity_functions.ValueParameter#getDataType <em>Data Type</em>}</li>
 * </ul>
 *
 * @see entity_functions.Entity_functionsPackage#getValueParameter()
 * @model extendedMetaData="name='ValueParameter' kind='empty'"
 * @generated
 */
public interface ValueParameter extends Parameter {
	/**
	 * Returns the value of the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The value as a string representation
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Value</em>' attribute.
	 * @see #setValue(String)
	 * @see entity_functions.Entity_functionsPackage#getValueParameter_Value()
	 * @model extendedMetaData="kind='attribute' name='value'"
	 * @generated
	 */
	String getValue();

	/**
	 * Sets the value of the '{@link entity_functions.ValueParameter#getValue <em>Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Value</em>' attribute.
	 * @see #getValue()
	 * @generated
	 */
	void setValue(String value);

	/**
	 * Returns the value of the '<em><b>Data Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Data Type</em>' reference.
	 * @see #setDataType(EDataType)
	 * @see entity_functions.Entity_functionsPackage#getValueParameter_DataType()
	 * @model
	 * @generated
	 */
	EDataType getDataType();

	/**
	 * Sets the value of the '{@link entity_functions.ValueParameter#getDataType <em>Data Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Data Type</em>' reference.
	 * @see #getDataType()
	 * @generated
	 */
	void setDataType(EDataType value);

} // ValueParameter
