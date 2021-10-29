/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsModule;

import org.eclipse.efbt.cocalimo.smcubes.model.logical_transformations.LogicalTransformationModule;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Aorta SM Cubes Model</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A grouping of SMCubes data definations alongside the tasks, and the tests for those tasks
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getSmCubesModel <em>Sm Cubes Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getTests <em>Tests</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getTaskModules <em>Task Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getTestDefinitions <em>Test Definitions</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getAortaSMCubesModel()
 * @model
 * @generated
 */
public interface AortaSMCubesModel extends EObject {
	/**
	 * Returns the value of the '<em><b>Sm Cubes Model</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The SMCubes data Model for cubes/varaibles etc
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Sm Cubes Model</em>' reference.
	 * @see #setSmCubesModel(SmcubesModel)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getAortaSMCubesModel_SmCubesModel()
	 * @model
	 * @generated
	 */
	SmcubesModel getSmCubesModel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getSmCubesModel <em>Sm Cubes Model</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Sm Cubes Model</em>' reference.
	 * @see #getSmCubesModel()
	 * @generated
	 */
	void setSmCubesModel(SmcubesModel value);

	/**
	 * Returns the value of the '<em><b>Requirements</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated requirements, these will be referenced by Tasks
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Requirements</em>' reference.
	 * @see #setRequirements(RequirementsModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getAortaSMCubesModel_Requirements()
	 * @model
	 * @generated
	 */
	RequirementsModule getRequirements();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getRequirements <em>Requirements</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Requirements</em>' reference.
	 * @see #getRequirements()
	 * @generated
	 */
	void setRequirements(RequirementsModule value);

	/**
	 * Returns the value of the '<em><b>Tests</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated Tests, these will be refrence by Tasks
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Tests</em>' reference.
	 * @see #setTests(TestModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getAortaSMCubesModel_Tests()
	 * @model
	 * @generated
	 */
	TestModule getTests();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getTests <em>Tests</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Tests</em>' reference.
	 * @see #getTests()
	 * @generated
	 */
	void setTests(TestModule value);

	/**
	 * Returns the value of the '<em><b>Task Modules</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated Tasks, and their workflow
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Task Modules</em>' reference.
	 * @see #setTaskModules(LogicalTransformationModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getAortaSMCubesModel_TaskModules()
	 * @model
	 * @generated
	 */
	LogicalTransformationModule getTaskModules();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getTaskModules <em>Task Modules</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Task Modules</em>' reference.
	 * @see #getTaskModules()
	 * @generated
	 */
	void setTaskModules(LogicalTransformationModule value);

	/**
	 * Returns the value of the '<em><b>Test Definitions</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The test definitions, each test is associated with a test definition which describes the test case
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Test Definitions</em>' reference.
	 * @see #setTestDefinitions(TestDefinitionModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getAortaSMCubesModel_TestDefinitions()
	 * @model
	 * @generated
	 */
	TestDefinitionModule getTestDefinitions();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getTestDefinitions <em>Test Definitions</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Test Definitions</em>' reference.
	 * @see #getTestDefinitions()
	 * @generated
	 */
	void setTestDefinitions(TestDefinitionModule value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the test definition
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage#getAortaSMCubesModel_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.AortaSMCubesModel#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // AortaSMCubesModel
