/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module;

import org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario;
import org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Functionality Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * FunctionalityModule is the super class of ReportCellFunctionalityModule,ReadDataSourceFunctionalityModule, and DataSetTransformationFunctionalityModule.
 * It represents a set of processing.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getUsedModule <em>Used Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getScenario <em>Scenario</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getScenarioSet <em>Scenario Set</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModule()
 * @model abstract="true"
 *        extendedMetaData="name='FunctionalityModule' kind='empty'"
 * @generated
 */
public interface FunctionalityModule extends EObject
{
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the FunctionalityModule
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModule_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Used Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * We can specify a dependency upon another Functional Module here if we wish. Normally the dependencies are not set but can be derived from where the Functionality Module exists within a set of Serial and Parallel FunctionalityModuleSets
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Used Module</em>' reference.
	 * @see #setUsedModule(FunctionalityModule)
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModule_UsedModule()
	 * @model
	 * @generated
	 */
	FunctionalityModule getUsedModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getUsedModule <em>Used Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Used Module</em>' reference.
	 * @see #getUsedModule()
	 * @generated
	 */
	void setUsedModule(FunctionalityModule value);

	/**
	 * Returns the value of the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * A Particular Scenario if the FunctionalityModule is assocuiated with a particular Scenario.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Scenario</em>' reference.
	 * @see #setScenario(Scenario)
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModule_Scenario()
	 * @model
	 * @generated
	 */
	Scenario getScenario();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getScenario <em>Scenario</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenario</em>' reference.
	 * @see #getScenario()
	 * @generated
	 */
	void setScenario(Scenario value);

	/**
	 * Returns the value of the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * Any ScenarioSets associated with the  FunctionalityModule. 
	 * The individual Scenarios in such a ScenarioSet are associated with different behaviour in the FunctionalityModule that we would like to test seperately. 
	 * Scenarios can themsleves be linked to parts of the regulatory rulebook, allowing an annotation of the behaviour we wish to test.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Scenario Set</em>' containment reference.
	 * @see #setScenarioSet(ScenarioSet)
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getFunctionalityModule_ScenarioSet()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='scenarioSet'"
	 * @generated
	 */
	ScenarioSet getScenarioSet();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule#getScenarioSet <em>Scenario Set</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenario Set</em>' containment reference.
	 * @see #getScenarioSet()
	 * @generated
	 */
	void setScenarioSet(ScenarioSet value);

} // FunctionalityModule
