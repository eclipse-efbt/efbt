/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition;

import org.eclipse.efbt.cocalimo.core.model.task.Task;
import org.eclipse.efbt.cocalimo.core.model.task.TaskSet;
import org.eclipse.efbt.cocalimo.core.model.task.ReadDataSourceTask;
import org.eclipse.efbt.cocalimo.core.model.task.ReportCellCreationTask;

import org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Reg Functionality Test Definition</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getInputModules <em>Input Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getProcessingModules <em>Processing Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getReportCellCreationModules <em>Report Cell Creation Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getScenarios <em>Scenarios</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getPicture <em>Picture</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getRegFunctionalityTestDefinition()
 * @model
 * @generated
 */
public interface RegFunctionalityTestDefinition extends TestDefinition
{
	/**
	 * Returns the value of the '<em><b>Input Modules</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.task.ReadDataSourceTask}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Input Modules</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getRegFunctionalityTestDefinition_InputModules()
	 * @model
	 * @generated
	 */
	EList<ReadDataSourceTask> getInputModules();

	/**
	 * Returns the value of the '<em><b>Processing Modules</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.task.Task}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Processing Modules</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getRegFunctionalityTestDefinition_ProcessingModules()
	 * @model
	 * @generated
	 */
	EList<Task> getProcessingModules();

	/**
	 * Returns the value of the '<em><b>Report Cell Creation Modules</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.task.ReportCellCreationTask}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Report Cell Creation Modules</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getRegFunctionalityTestDefinition_ReportCellCreationModules()
	 * @model
	 * @generated
	 */
	EList<ReportCellCreationTask> getReportCellCreationModules();

	/**
	 * Returns the value of the '<em><b>Scenarios</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenarios</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getRegFunctionalityTestDefinition_Scenarios()
	 * @model
	 * @generated
	 */
	EList<Scenario> getScenarios();

	/**
	 * Returns the value of the '<em><b>Picture</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Picture</em>' containment reference.
	 * @see #setPicture(TaskSet)
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getRegFunctionalityTestDefinition_Picture()
	 * @model containment="true"
	 * @generated
	 */
	TaskSet getPicture();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition#getPicture <em>Picture</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Picture</em>' containment reference.
	 * @see #getPicture()
	 * @generated
	 */
	void setPicture(TaskSet value);

} // RegFunctionalityTestDefinition
