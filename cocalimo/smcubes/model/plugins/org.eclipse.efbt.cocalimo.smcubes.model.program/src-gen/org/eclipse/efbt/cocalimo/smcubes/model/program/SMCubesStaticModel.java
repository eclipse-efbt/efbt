/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.program;

import org.eclipse.efbt.cocalimo.core.model.task.TaskModule;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.RequirementsModule;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinitionModule;
import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplateModule;
import org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinitionModule;

import org.eclipse.efbt.cocalimo.smcubes.model.smcubes_model.SmcubesModel;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>SM Cubes Static Model</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * An SMCubesStaticModel  relates to the group of artifacts, and we allow the contents of  a SMCubesStaticModel  to reference other SMCubesStaticModels.
 * 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getSmCubesModel <em>Sm Cubes Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getBddTestDefinitions <em>Bdd Test Definitions</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTests <em>Tests</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTestTemplates <em>Test Templates</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTestConstriants <em>Test Constriants</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTasks <em>Functionality Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getRegFunctionalityTests <em>Reg Functionality Tests</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getRegFunctionalityTestDefinitions <em>Reg Functionality Test Definitions</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#getSMCubesStaticModel()
 * @model
 * @generated
 */
public interface SMCubesStaticModel extends StaticModel {
	/**
	 * Returns the value of the '<em><b>Sm Cubes Model</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The SMCubes Model associated with the Program
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Sm Cubes Model</em>' reference.
	 * @see #setSmCubesModel(SmcubesModel)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#getSMCubesStaticModel_SmCubesModel()
	 * @model
	 * @generated
	 */
	SmcubesModel getSmCubesModel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getSmCubesModel <em>Sm Cubes Model</em>}' reference.
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
	 * The requirements associated with the program, these will be refrence by Tasks
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Requirements</em>' reference.
	 * @see #setRequirements(RequirementsModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#getSMCubesStaticModel_Requirements()
	 * @model
	 * @generated
	 */
	RequirementsModule getRequirements();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getRequirements <em>Requirements</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Requirements</em>' reference.
	 * @see #getRequirements()
	 * @generated
	 */
	void setRequirements(RequirementsModule value);

	/**
	 * Returns the value of the '<em><b>Bdd Test Definitions</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The TestDefinitions associated with the program, these will be refrence by Tasks
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Bdd Test Definitions</em>' reference.
	 * @see #setBddTestDefinitions(BDDTestDefinitionModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#getSMCubesStaticModel_BddTestDefinitions()
	 * @model
	 * @generated
	 */
	BDDTestDefinitionModule getBddTestDefinitions();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getBddTestDefinitions <em>Bdd Test Definitions</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Bdd Test Definitions</em>' reference.
	 * @see #getBddTestDefinitions()
	 * @generated
	 */
	void setBddTestDefinitions(BDDTestDefinitionModule value);

	/**
	 * Returns the value of the '<em><b>Tests</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Tests associated with the program, these will be refrence by Tasks
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Tests</em>' reference.
	 * @see #setTests(SMCubesBDDTestModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#getSMCubesStaticModel_Tests()
	 * @model
	 * @generated
	 */
	SMCubesBDDTestModule getTests();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTests <em>Tests</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Tests</em>' reference.
	 * @see #getTests()
	 * @generated
	 */
	void setTests(SMCubesBDDTestModule value);

	/**
	 * Returns the value of the '<em><b>Test Templates</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The TestTemplates associated with the program, these will be refrence by Tasks
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Test Templates</em>' reference.
	 * @see #setTestTemplates(BDDTestTemplateModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#getSMCubesStaticModel_TestTemplates()
	 * @model
	 * @generated
	 */
	BDDTestTemplateModule getTestTemplates();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTestTemplates <em>Test Templates</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Test Templates</em>' reference.
	 * @see #getTestTemplates()
	 * @generated
	 */
	void setTestTemplates(BDDTestTemplateModule value);

	/**
	 * Returns the value of the '<em><b>Test Constriants</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The TestConstraints associated with the program, these will be refrence by Tasks
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Test Constriants</em>' reference.
	 * @see #setTestConstriants(BDDTestContraints)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#getSMCubesStaticModel_TestConstriants()
	 * @model
	 * @generated
	 */
	BDDTestContraints getTestConstriants();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTestConstriants <em>Test Constriants</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Test Constriants</em>' reference.
	 * @see #getTestConstriants()
	 * @generated
	 */
	void setTestConstriants(BDDTestContraints value);

	/**
	 * Returns the value of the '<em><b>Functionality Modules</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The FunctionaltyModules associated with the program, these will be refrence by Tasks
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Functionality Modules</em>' reference.
	 * @see #setTasks(TaskModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#getSMCubesStaticModel_Tasks()
	 * @model
	 * @generated
	 */
	TaskModule getTasks();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getTasks <em>Functionality Modules</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Functionality Modules</em>' reference.
	 * @see #getTasks()
	 * @generated
	 */
	void setTasks(TaskModule value);

	/**
	 * Returns the value of the '<em><b>Reg Functionality Tests</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Tests associated with the program, these will be refrence by Tasks
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Reg Functionality Tests</em>' reference.
	 * @see #setRegFunctionalityTests(SMCubesRegFunctionalityTestModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#getSMCubesStaticModel_RegFunctionalityTests()
	 * @model
	 * @generated
	 */
	SMCubesRegFunctionalityTestModule getRegFunctionalityTests();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getRegFunctionalityTests <em>Reg Functionality Tests</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Reg Functionality Tests</em>' reference.
	 * @see #getRegFunctionalityTests()
	 * @generated
	 */
	void setRegFunctionalityTests(SMCubesRegFunctionalityTestModule value);

	/**
	 * Returns the value of the '<em><b>Reg Functionality Test Definitions</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The TestDefinitions associated with the program, these will be refrence by Tasks
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Reg Functionality Test Definitions</em>' reference.
	 * @see #setRegFunctionalityTestDefinitions(RegFunctionalityTestDefinitionModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage#getSMCubesStaticModel_RegFunctionalityTestDefinitions()
	 * @model
	 * @generated
	 */
	RegFunctionalityTestDefinitionModule getRegFunctionalityTestDefinitions();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel#getRegFunctionalityTestDefinitions <em>Reg Functionality Test Definitions</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Reg Functionality Test Definitions</em>' reference.
	 * @see #getRegFunctionalityTestDefinitions()
	 * @generated
	 */
	void setRegFunctionalityTestDefinitions(RegFunctionalityTestDefinitionModule value);

} // SMCubesStaticModel
