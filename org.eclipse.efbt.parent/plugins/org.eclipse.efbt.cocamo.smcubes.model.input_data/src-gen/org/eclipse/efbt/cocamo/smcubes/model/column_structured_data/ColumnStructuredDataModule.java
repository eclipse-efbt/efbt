/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.column_structured_data;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Column Structured Data Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.ColumnStructuredDataModule#getCubeData <em>Cube Data</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.Column_structured_dataPackage#getColumnStructuredDataModule()
 * @model extendedMetaData="name='ColumnStructuredDataModule' kind='elementOnly'"
 * @generated
 */
public interface ColumnStructuredDataModule extends org.eclipse.efbt.cocamo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Cube Data</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.ColumnStructuredData}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of ColumnStructuredData
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Data</em>' containment reference list.
	 * @see org.eclipse.efbt.cocamo.smcubes.model.column_structured_data.Column_structured_dataPackage#getColumnStructuredDataModule_CubeData()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='cubeData'"
	 * @generated
	 */
	EList<ColumnStructuredData> getCubeData();

} // ColumnStructuredDataModule
