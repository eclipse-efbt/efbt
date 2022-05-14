/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension;

import org.eclipse.efbt.openregspecs.model.core.VARIABLE;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Variable Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing a list of variables
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.VariableModule#getVariables <em>Variables</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getVariableModule()
 * @model extendedMetaData="name='VariableModule' kind='elementOnly'"
 * @generated
 */
public interface VariableModule extends org.eclipse.efbt.openregspecs.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Variables</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.core.VARIABLE}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of Variables  contained in the Module
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Variables</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage#getVariableModule_Variables()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='variables'"
	 * @generated
	 */
	EList<VARIABLE> getVariables();

} // VariableModule
