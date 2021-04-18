/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocamo;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getSmCubesModel <em>Sm Cubes Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getBddTestDefinitions <em>Bdd Test Definitions</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getTests <em>Tests</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getTestTemplates <em>Test Templates</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getTestConstriants <em>Test Constriants</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getFunctionalityModules <em>Functionality Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getRegFunctionalityTests <em>Reg Functionality Tests</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getRegFunctionalityTestDefinitions <em>Reg Functionality Test Definitions</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesStaticModel()
 * @model
 * @generated
 */
public interface SMCubesStaticModel extends StaticModel
{
	/**
	 * Returns the value of the '<em><b>Sm Cubes Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The SMCubes Model associated with the Program
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Sm Cubes Model</em>' containment reference.
	 * @see #setSmCubesModel(SmcubesModel)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesStaticModel_SmCubesModel()
	 * @model containment="true"
	 * @generated
	 */
	SmcubesModel getSmCubesModel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getSmCubesModel <em>Sm Cubes Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Sm Cubes Model</em>' containment reference.
	 * @see #getSmCubesModel()
	 * @generated
	 */
	void setSmCubesModel(SmcubesModel value);

	/**
	 * Returns the value of the '<em><b>Requirements</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The requirements associated with the program, these will be refrence by FunctionalityModules
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Requirements</em>' containment reference.
	 * @see #setRequirements(RequirementsModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesStaticModel_Requirements()
	 * @model containment="true"
	 * @generated
	 */
	RequirementsModule getRequirements();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getRequirements <em>Requirements</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Requirements</em>' containment reference.
	 * @see #getRequirements()
	 * @generated
	 */
	void setRequirements(RequirementsModule value);

	/**
	 * Returns the value of the '<em><b>Bdd Test Definitions</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The TestDefinitions associated with the program, these will be refrence by FunctionalityModules
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Bdd Test Definitions</em>' containment reference.
	 * @see #setBddTestDefinitions(BDDTestDefinitionModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesStaticModel_BddTestDefinitions()
	 * @model containment="true"
	 * @generated
	 */
	BDDTestDefinitionModule getBddTestDefinitions();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getBddTestDefinitions <em>Bdd Test Definitions</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Bdd Test Definitions</em>' containment reference.
	 * @see #getBddTestDefinitions()
	 * @generated
	 */
	void setBddTestDefinitions(BDDTestDefinitionModule value);

	/**
	 * Returns the value of the '<em><b>Tests</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Tests associated with the program, these will be refrence by FunctionalityModules
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Tests</em>' containment reference.
	 * @see #setTests(SMCubesBDDTestModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesStaticModel_Tests()
	 * @model containment="true"
	 * @generated
	 */
	SMCubesBDDTestModule getTests();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getTests <em>Tests</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Tests</em>' containment reference.
	 * @see #getTests()
	 * @generated
	 */
	void setTests(SMCubesBDDTestModule value);

	/**
	 * Returns the value of the '<em><b>Test Templates</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The TestTemplates associated with the program, these will be refrence by FunctionalityModules
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Test Templates</em>' containment reference.
	 * @see #setTestTemplates(BDDTestTemplateModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesStaticModel_TestTemplates()
	 * @model containment="true"
	 * @generated
	 */
	BDDTestTemplateModule getTestTemplates();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getTestTemplates <em>Test Templates</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Test Templates</em>' containment reference.
	 * @see #getTestTemplates()
	 * @generated
	 */
	void setTestTemplates(BDDTestTemplateModule value);

	/**
	 * Returns the value of the '<em><b>Test Constriants</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The TestConstraints associated with the program, these will be refrence by FunctionalityModules
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Test Constriants</em>' containment reference.
	 * @see #setTestConstriants(BDDTestContraints)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesStaticModel_TestConstriants()
	 * @model containment="true"
	 * @generated
	 */
	BDDTestContraints getTestConstriants();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getTestConstriants <em>Test Constriants</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Test Constriants</em>' containment reference.
	 * @see #getTestConstriants()
	 * @generated
	 */
	void setTestConstriants(BDDTestContraints value);

	/**
	 * Returns the value of the '<em><b>Functionality Modules</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The FunctionaltyModules associated with the program, these will be refrence by FunctionalityModules
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Functionality Modules</em>' containment reference.
	 * @see #setFunctionalityModules(FunctionalityModuleModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesStaticModel_FunctionalityModules()
	 * @model containment="true"
	 * @generated
	 */
	FunctionalityModuleModule getFunctionalityModules();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getFunctionalityModules <em>Functionality Modules</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Functionality Modules</em>' containment reference.
	 * @see #getFunctionalityModules()
	 * @generated
	 */
	void setFunctionalityModules(FunctionalityModuleModule value);

	/**
	 * Returns the value of the '<em><b>Reg Functionality Tests</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Tests associated with the program, these will be refrence by FunctionalityModules
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Reg Functionality Tests</em>' containment reference.
	 * @see #setRegFunctionalityTests(SMCubesRegFunctionalityTestModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesStaticModel_RegFunctionalityTests()
	 * @model containment="true"
	 * @generated
	 */
	SMCubesRegFunctionalityTestModule getRegFunctionalityTests();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getRegFunctionalityTests <em>Reg Functionality Tests</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Reg Functionality Tests</em>' containment reference.
	 * @see #getRegFunctionalityTests()
	 * @generated
	 */
	void setRegFunctionalityTests(SMCubesRegFunctionalityTestModule value);

	/**
	 * Returns the value of the '<em><b>Reg Functionality Test Definitions</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The TestDefinitions associated with the program, these will be refrence by FunctionalityModules
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Reg Functionality Test Definitions</em>' containment reference.
	 * @see #setRegFunctionalityTestDefinitions(RegFunctionalityTestDefinitionModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesStaticModel_RegFunctionalityTestDefinitions()
	 * @model containment="true"
	 * @generated
	 */
	RegFunctionalityTestDefinitionModule getRegFunctionalityTestDefinitions();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesStaticModel#getRegFunctionalityTestDefinitions <em>Reg Functionality Test Definitions</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Reg Functionality Test Definitions</em>' containment reference.
	 * @see #getRegFunctionalityTestDefinitions()
	 * @generated
	 */
	void setRegFunctionalityTestDefinitions(RegFunctionalityTestDefinitionModule value);

} // SMCubesStaticModel
