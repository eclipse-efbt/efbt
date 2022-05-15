/**
 */
package org.eclipse.efbt.openregspecs.model.testing;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Unit Test Scope</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.UnitTestScope#getScenarios <em>Scenarios</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getUnitTestScope()
 * @model
 * @generated
 */
public interface UnitTestScope extends TestScope {
	/**
	 * Returns the value of the '<em><b>Scenarios</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenarios</em>' reference.
	 * @see #setScenarios(Scenario)
	 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage#getUnitTestScope_Scenarios()
	 * @model
	 * @generated
	 */
	Scenario getScenarios();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.testing.UnitTestScope#getScenarios <em>Scenarios</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenarios</em>' reference.
	 * @see #getScenarios()
	 * @generated
	 */
	void setScenarios(Scenario value);

} // UnitTestScope
