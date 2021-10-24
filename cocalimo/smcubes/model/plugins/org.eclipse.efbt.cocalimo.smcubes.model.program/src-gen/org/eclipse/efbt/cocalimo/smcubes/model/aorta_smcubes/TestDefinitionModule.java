/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Test Definition Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinitionModule#getTestDefinitions <em>Test Definitions</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTestDefinitionModule()
 * @model
 * @generated
 */
public interface TestDefinitionModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Test Definitions</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Test Definitions</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTestDefinitionModule_TestDefinitions()
	 * @model containment="true"
	 * @generated
	 */
	EList<TestDefinition> getTestDefinitions();

} // TestDefinitionModule
