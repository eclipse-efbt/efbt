/**
 */
package org.eclipse.efbt.language.trl.model.domain_model_mapping;

import org.eclipse.efbt.functional_design.core.model.scenarios.Scenario;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Scenario To Code Conditions</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.ScenarioToCodeConditions#getScenario <em>Scenario</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.ScenarioToCodeConditions#getCodeConditions <em>Code Conditions</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage#getScenarioToCodeConditions()
 * @model
 * @generated
 */
public interface ScenarioToCodeConditions extends EObject
{
	/**
	 * Returns the value of the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenario</em>' reference.
	 * @see #setScenario(Scenario)
	 * @see org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage#getScenarioToCodeConditions_Scenario()
	 * @model
	 * @generated
	 */
	Scenario getScenario();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.ScenarioToCodeConditions#getScenario <em>Scenario</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenario</em>' reference.
	 * @see #getScenario()
	 * @generated
	 */
	void setScenario(Scenario value);

	/**
	 * Returns the value of the '<em><b>Code Conditions</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.trl.model.domain_model_mapping.CodeCondition}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Code Conditions</em>' containment reference list.
	 * @see org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage#getScenarioToCodeConditions_CodeConditions()
	 * @model containment="true"
	 * @generated
	 */
	EList<CodeCondition> getCodeConditions();

} // ScenarioToCodeConditions
