/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.reports;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Axis Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing a set of  Axis 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.reports.AxisModule#getAxes <em>Axes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getAxisModule()
 * @model extendedMetaData="name='AxisModule' kind='elementOnly'"
 * @generated
 */
public interface AxisModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Axes</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of Axes
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Axes</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.reports.ReportsPackage#getAxisModule_Axes()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='axes'"
	 * @generated
	 */
	EList<AXIS> getAxes();

} // AxisModule
