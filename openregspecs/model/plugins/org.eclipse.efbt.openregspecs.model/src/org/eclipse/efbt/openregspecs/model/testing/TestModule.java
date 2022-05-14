/**
 */
package org.eclipse.efbt.openregspecs.model.testing;

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
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.TestModule#getTests <em>Tests</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getTestModule()
 * @model
 * @generated
 */
public interface TestModule extends org.eclipse.efbt.openregspecs.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>Tests</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.testing.Test}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Tests</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getTestModule_Tests()
	 * @model containment="true"
	 * @generated
	 */
	EList<Test> getTests();

} // TestModule
