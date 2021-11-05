/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Axis Ordinate Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing a set of  AxisOrdinates
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.AxisOrdinateModule#getAxis_ordinates <em>Axis ordinates</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getAxisOrdinateModule()
 * @model extendedMetaData="name='AxisOrdinateModule' kind='elementOnly'"
 * @generated
 */
public interface AxisOrdinateModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Axis ordinates</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of AxisOrdinates
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Axis ordinates</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getAxisOrdinateModule_Axis_ordinates()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='axis_ordinates'"
	 * @generated
	 */
	EList<AXIS_ORDINATE> getAxis_ordinates();

} // AxisOrdinateModule
