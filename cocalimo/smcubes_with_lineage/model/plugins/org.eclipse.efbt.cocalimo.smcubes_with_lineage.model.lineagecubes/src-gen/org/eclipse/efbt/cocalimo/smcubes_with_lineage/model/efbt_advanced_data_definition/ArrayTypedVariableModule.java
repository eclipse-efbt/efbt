/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Array Typed Variable Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Module containing a set of ArrayTypeVariables
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariableModule#getArrayTypedVariables <em>Array Typed Variables</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage#getArrayTypedVariableModule()
 * @model
 * @generated
 */
public interface ArrayTypedVariableModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Array Typed Variables</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of ArrayTypeVariables
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Array Typed Variables</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage#getArrayTypedVariableModule_ArrayTypedVariables()
	 * @model containment="true"
	 * @generated
	 */
	EList<ArrayTypedVariable> getArrayTypedVariables();

} // ArrayTypedVariableModule
