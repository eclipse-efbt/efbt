/**
 */
package org.eclipse.efbt.cocalimo.core.model.scenarios;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Scenario Set</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Set of Scenarios
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet#getScenarios <em>Scenarios</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage#getScenarioSet()
 * @model extendedMetaData="name='ScenarioSet' kind='elementOnly'"
 * @generated
 */
public interface ScenarioSet extends EObject
{
	/**
	 * Returns the value of the '<em><b>Scenarios</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Scenarios in the ScenarioSet
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Scenarios</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage#getScenarioSet_Scenarios()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='scenarios'"
	 * @generated
	 */
	EList<Scenario> getScenarios();

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the ScenarioSet
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage#getScenarioSet_Name()
	 * @model extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.ScenarioSet#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // ScenarioSet
