/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.task;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.task.TaskPackage
 * @generated
 */
public interface TaskFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	TaskFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.task.impl.TaskFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Data Processing Task</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Data Processing Task</em>'.
	 * @generated
	 */
	DataProcessingTask createDataProcessingTask();

	/**
	 * Returns a new object of class '<em>Leaf Task</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Leaf Task</em>'.
	 * @generated
	 */
	LeafTask createLeafTask();

	/**
	 * Returns a new object of class '<em>Parallel Task</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Parallel Task</em>'.
	 * @generated
	 */
	ParallelTask createParallelTask();

	/**
	 * Returns a new object of class '<em>Serial Task</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Serial Task</em>'.
	 * @generated
	 */
	SerialTask createSerialTask();

	/**
	 * Returns a new object of class '<em>Set</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Set</em>'.
	 * @generated
	 */
	TaskSet createTaskSet();

	/**
	 * Returns a new object of class '<em>Read Data Source Task</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Read Data Source Task</em>'.
	 * @generated
	 */
	ReadDataSourceTask createReadDataSourceTask();

	/**
	 * Returns a new object of class '<em>Report Cell Creation Task</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report Cell Creation Task</em>'.
	 * @generated
	 */
	ReportCellCreationTask createReportCellCreationTask();

	/**
	 * Returns a new object of class '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Module</em>'.
	 * @generated
	 */
	TaskModule createTaskModule();

	/**
	 * Returns a new object of class '<em>Tag</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Tag</em>'.
	 * @generated
	 */
	TaskTag createTaskTag();

	/**
	 * Returns a new object of class '<em>Parallel Choice Task</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Parallel Choice Task</em>'.
	 * @generated
	 */
	ParallelChoiceTask createParallelChoiceTask();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	TaskPackage getTaskPackage();

} //TaskFactory
