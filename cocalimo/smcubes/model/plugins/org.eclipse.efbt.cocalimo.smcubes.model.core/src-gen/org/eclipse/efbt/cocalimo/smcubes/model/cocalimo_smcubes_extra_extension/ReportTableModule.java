/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Report Table Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing a set of ReportTables
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.ReportTableModule#getReportTables <em>Report Tables</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.Cocalimo_smcubes_extra_extensionPackage#getReportTableModule()
 * @model extendedMetaData="name='ReportTableModule' kind='elementOnly'"
 * @generated
 */
public interface ReportTableModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Report Tables</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.TABLE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of ReportTables
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Report Tables</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extra_extension.Cocalimo_smcubes_extra_extensionPackage#getReportTableModule_ReportTables()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='reportTables'"
	 * @generated
	 */
	EList<TABLE> getReportTables();

} // ReportTableModule
