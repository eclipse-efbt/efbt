/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Input Data Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A module containeing a set of input data
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.InputDataModule#getInputData <em>Input Data</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getInputDataModule()
 * @model extendedMetaData="name='InputDataModule' kind='elementOnly'"
 * @generated
 */
public interface InputDataModule extends org.eclipse.efbt.cocamo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Input Data</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.InputData}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The input data that makes up the module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Input Data</em>' containment reference list.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.base_column_structured_data.Base_column_structured_dataPackage#getInputDataModule_InputData()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='inputData'"
	 * @generated
	 */
	EList<InputData> getInputData();

} // InputDataModule
