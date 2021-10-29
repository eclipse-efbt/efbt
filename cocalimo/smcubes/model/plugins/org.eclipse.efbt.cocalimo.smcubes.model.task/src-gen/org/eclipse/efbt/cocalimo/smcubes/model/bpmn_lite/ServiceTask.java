/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;

import org.eclipse.efbt.cocalimo.smcubes.model.scenarios.ScenarioSet;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Service Task</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.ServiceTask#getRequiredAttributes <em>Required Attributes</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.ServiceTask#getEnrichedAttribute <em>Enriched Attribute</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.ServiceTask#getScenarioSet <em>Scenario Set</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage#getServiceTask()
 * @model
 * @generated
 */
public interface ServiceTask extends Task {
	/**
	 * Returns the value of the '<em><b>Required Attributes</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Required Attributes</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage#getServiceTask_RequiredAttributes()
	 * @model
	 * @generated
	 */
	EList<CUBE_STRUCTURE_ITEM> getRequiredAttributes();

	/**
	 * Returns the value of the '<em><b>Enriched Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Enriched Attribute</em>' reference.
	 * @see #setEnrichedAttribute(CUBE_STRUCTURE_ITEM)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage#getServiceTask_EnrichedAttribute()
	 * @model
	 * @generated
	 */
	CUBE_STRUCTURE_ITEM getEnrichedAttribute();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.ServiceTask#getEnrichedAttribute <em>Enriched Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Enriched Attribute</em>' reference.
	 * @see #getEnrichedAttribute()
	 * @generated
	 */
	void setEnrichedAttribute(CUBE_STRUCTURE_ITEM value);

	/**
	 * Returns the value of the '<em><b>Scenario Set</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenario Set</em>' containment reference.
	 * @see #setScenarioSet(ScenarioSet)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage#getServiceTask_ScenarioSet()
	 * @model containment="true"
	 * @generated
	 */
	ScenarioSet getScenarioSet();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.ServiceTask#getScenarioSet <em>Scenario Set</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Scenario Set</em>' containment reference.
	 * @see #getScenarioSet()
	 * @generated
	 */
	void setScenarioSet(ScenarioSet value);

} // ServiceTask
