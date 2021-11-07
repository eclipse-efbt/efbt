/**
 */
package org.eclipse.efbt.cocalimo.core.model.logical_transformations;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsPackage
 * @generated
 */
public interface Logical_transformationsFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Logical_transformationsFactory eINSTANCE = org.eclipse.efbt.cocalimo.core.model.logical_transformations.impl.Logical_transformationsFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Logical Transformation Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Logical Transformation Module</em>'.
	 * @generated
	 */
	LogicalTransformationModule createLogicalTransformationModule();

	/**
	 * Returns a new object of class '<em>Task Tag</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Task Tag</em>'.
	 * @generated
	 */
	TaskTag createTaskTag();

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
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Logical_transformationsPackage getLogical_transformationsPackage();

} //Logical_transformationsFactory
