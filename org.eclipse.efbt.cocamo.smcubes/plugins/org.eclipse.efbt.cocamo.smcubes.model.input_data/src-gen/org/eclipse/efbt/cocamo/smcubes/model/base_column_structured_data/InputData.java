/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Input Data</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.InputData#getSourceTableData <em>Source Table Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.InputData#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getInputData()
 * @model
 * @generated
 */
public interface InputData extends EObject
{
	/**
	 * Returns the value of the '<em><b>Source Table Data</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.BaseColumnStructuredData}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of pupulated BaseCubes that represent the input data.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Source Table Data</em>' containment reference list.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getInputData_SourceTableData()
	 * @model containment="true" ordered="false"
	 *        extendedMetaData="kind='element' name='sourceTableData'"
	 * @generated
	 */
	EList<BaseColumnStructuredData> getSourceTableData();

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name associated with the input data.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getInputData_Name()
	 * @model extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.InputData#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // InputData
