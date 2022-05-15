/**
 */
package org.eclipse.efbt.openregspecs.model.testing;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.testing.TestingPackage
 * @generated
 */
public interface TestingFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TestingFactory eINSTANCE = org.eclipse.efbt.openregspecs.model.testing.impl.TestingFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Logical Transformation Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Logical Transformation Module</em>'.
	 * @generated
	 */
	LogicalTransformationModule createLogicalTransformationModule();

	/**
	 * Returns a new object of class '<em>Activity Tag</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Activity Tag</em>'.
	 * @generated
	 */
	ActivityTag createActivityTag();

	/**
	 * Returns a new object of class '<em>Scenario</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Scenario</em>'.
	 * @generated
	 */
	Scenario createScenario();

	/**
	 * Returns a new object of class '<em>Scenario Tag</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Scenario Tag</em>'.
	 * @generated
	 */
	ScenarioTag createScenarioTag();

	/**
	 * Returns a new object of class '<em>Test</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Test</em>'.
	 * @generated
	 */
	Test createTest();

	/**
	 * Returns a new object of class '<em>Test Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Test Module</em>'.
	 * @generated
	 */
	TestModule createTestModule();

	/**
	 * Returns a new object of class '<em>Data Constraint</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Data Constraint</em>'.
	 * @generated
	 */
	DataConstraint createDataConstraint();

	/**
	 * Returns a new object of class '<em>Selection Layer</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Selection Layer</em>'.
	 * @generated
	 */
	SelectionLayer createSelectionLayer();

	/**
	 * Returns a new object of class '<em>Test Scope</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Test Scope</em>'.
	 * @generated
	 */
	TestScope createTestScope();

	/**
	 * Returns a new object of class '<em>Unit Test Scope</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Unit Test Scope</em>'.
	 * @generated
	 */
	UnitTestScope createUnitTestScope();

	/**
	 * Returns a new object of class '<em>E2E Test Scope</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>E2E Test Scope</em>'.
	 * @generated
	 */
	E2ETestScope createE2ETestScope();

	/**
	 * Returns a new object of class '<em>CSV File</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>CSV File</em>'.
	 * @generated
	 */
	CSVFile createCSVFile();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	TestingPackage getTestingPackage();

} //TestingFactory
