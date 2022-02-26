/**
 */
package org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.Bpmn_litePackage
 * @generated
 */
public interface Bpmn_liteFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Bpmn_liteFactory eINSTANCE = org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.impl.Bpmn_liteFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Sequence Flow</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Sequence Flow</em>'.
	 * @generated
	 */
	SequenceFlow createSequenceFlow();

	/**
	 * Returns a new object of class '<em>Task</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Task</em>'.
	 * @generated
	 */
	Task createTask();

	/**
	 * Returns a new object of class '<em>Exclusive Gateway</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Exclusive Gateway</em>'.
	 * @generated
	 */
	ExclusiveGateway createExclusiveGateway();

	/**
	 * Returns a new object of class '<em>Inclusive Gateway</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Inclusive Gateway</em>'.
	 * @generated
	 */
	InclusiveGateway createInclusiveGateway();

	/**
	 * Returns a new object of class '<em>Parallel Gateway</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Parallel Gateway</em>'.
	 * @generated
	 */
	ParallelGateway createParallelGateway();

	/**
	 * Returns a new object of class '<em>Service Task</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Service Task</em>'.
	 * @generated
	 */
	ServiceTask createServiceTask();

	/**
	 * Returns a new object of class '<em>Sub Process</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Sub Process</em>'.
	 * @generated
	 */
	SubProcess createSubProcess();

	/**
	 * Returns a new object of class '<em>Script Task</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Script Task</em>'.
	 * @generated
	 */
	ScriptTask createScriptTask();

	/**
	 * Returns a new object of class '<em>User Task</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>User Task</em>'.
	 * @generated
	 */
	UserTask createUserTask();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Bpmn_litePackage getBpmn_litePackage();

} //Bpmn_liteFactory
