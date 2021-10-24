/**
 */
package org.eclipse.efbt.cocalimo.core.model.scenarios;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage
 * @generated
 */
public interface ScenariosFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	ScenariosFactory eINSTANCE = org.eclipse.efbt.cocalimo.core.model.scenarios.impl.ScenariosFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Scenario</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Scenario</em>'.
	 * @generated
	 */
	Scenario createScenario();

	/**
	 * Returns a new object of class '<em>Scenario Set</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Scenario Set</em>'.
	 * @generated
	 */
	ScenarioSet createScenarioSet();

	/**
	 * Returns a new object of class '<em>Scenario Tag</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Scenario Tag</em>'.
	 * @generated
	 */
	ScenarioTag createScenarioTag();

	/**
	 * Returns a new object of class '<em>Scenario Set Tag</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Scenario Set Tag</em>'.
	 * @generated
	 */
	ScenarioSetTag createScenarioSetTag();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	ScenariosPackage getScenariosPackage();

} //ScenariosFactory
