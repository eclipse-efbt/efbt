/**
 */
package org.eclipse.efbt.cocalimo.core.model.logical_transformations;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Test Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.TestModule#getTests <em>Tests</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage#getTestModule()
 * @model
 * @generated
 */
public interface TestModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Tests</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Tests</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage#getTestModule_Tests()
	 * @model containment="true"
	 * @generated
	 */
	EList<Test> getTests();

} // TestModule
