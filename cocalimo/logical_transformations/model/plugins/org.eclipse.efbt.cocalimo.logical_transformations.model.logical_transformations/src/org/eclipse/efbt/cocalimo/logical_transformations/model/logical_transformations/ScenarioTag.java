/**
 */
package org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Scenario Tag</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Tag which tags a Scenario in order to link it with requirements.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.ScenarioTag#getScenario <em>Scenario</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.Logical_transformationsPackage#getScenarioTag()
 * @model
 * @generated
 */
public interface ScenarioTag extends Tag {
	/**
	 * Returns the value of the '<em><b>Scenario</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The tagged Scenario.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Scenario</em>' reference.
	 * @see #setScenario(Scenario)
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.Logical_transformationsPackage#getScenarioTag_Scenario()
	 * @model
	 * @generated
	 */
	Scenario getScenario();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.ScenarioTag#getScenario <em>Scenario</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenario</em>' reference.
	 * @see #getScenario()
	 * @generated
	 */
	void setScenario(Scenario value);

} // ScenarioTag
