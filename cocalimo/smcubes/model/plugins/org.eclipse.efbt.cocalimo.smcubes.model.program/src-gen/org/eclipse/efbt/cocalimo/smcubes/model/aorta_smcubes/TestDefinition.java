/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes;

import org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario;

import org.eclipse.efbt.cocalimo.core.model.task.DataProcessingTask;
import org.eclipse.efbt.cocalimo.core.model.task.ReadDataSourceTask;
import org.eclipse.efbt.cocalimo.core.model.task.ReportCellCreationTask;
import org.eclipse.efbt.cocalimo.core.model.task.TaskSet;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Test Definition</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getReadTasks <em>Read Tasks</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getProcessingTasks <em>Processing Tasks</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getReportCellCreationTasks <em>Report Cell Creation Tasks</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getScenarios <em>Scenarios</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getPicture <em>Picture</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTestDefinition()
 * @model
 * @generated
 */
public interface TestDefinition extends EObject {
	/**
	 * Returns the value of the '<em><b>Read Tasks</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.task.ReadDataSourceTask}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Read Tasks</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTestDefinition_ReadTasks()
	 * @model
	 * @generated
	 */
	EList<ReadDataSourceTask> getReadTasks();

	/**
	 * Returns the value of the '<em><b>Processing Tasks</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.task.DataProcessingTask}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Processing Tasks</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTestDefinition_ProcessingTasks()
	 * @model
	 * @generated
	 */
	EList<DataProcessingTask> getProcessingTasks();

	/**
	 * Returns the value of the '<em><b>Report Cell Creation Tasks</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.task.ReportCellCreationTask}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Report Cell Creation Tasks</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTestDefinition_ReportCellCreationTasks()
	 * @model
	 * @generated
	 */
	EList<ReportCellCreationTask> getReportCellCreationTasks();

	/**
	 * Returns the value of the '<em><b>Scenarios</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenarios</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTestDefinition_Scenarios()
	 * @model
	 * @generated
	 */
	EList<Scenario> getScenarios();

	/**
	 * Returns the value of the '<em><b>Picture</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Picture</em>' reference.
	 * @see #setPicture(TaskSet)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTestDefinition_Picture()
	 * @model
	 * @generated
	 */
	TaskSet getPicture();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getPicture <em>Picture</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Picture</em>' reference.
	 * @see #getPicture()
	 * @generated
	 */
	void setPicture(TaskSet value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getTestDefinition_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // TestDefinition
