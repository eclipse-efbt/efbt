/**
 */
package org.eclipse.efbt.openregspecs.model.bpmn_lite;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute;

import org.eclipse.efbt.openregspecs.model.testing.Scenario;

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
 *   <li>{@link org.eclipse.efbt.openregspecs.model.bpmn_lite.ServiceTask#getEnrichedAttribute <em>Enriched Attribute</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.bpmn_lite.ServiceTask#getScenarios <em>Scenarios</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.bpmn_lite.ServiceTask#getSecondAttribute <em>Second Attribute</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage#getServiceTask()
 * @model
 * @generated
 */
public interface ServiceTask extends Task {
	/**
	 * Returns the value of the '<em><b>Enriched Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Enriched Attribute</em>' reference.
	 * @see #setEnrichedAttribute(Attribute)
	 * @see org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage#getServiceTask_EnrichedAttribute()
	 * @model
	 * @generated
	 */
	Attribute getEnrichedAttribute();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.bpmn_lite.ServiceTask#getEnrichedAttribute <em>Enriched Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Enriched Attribute</em>' reference.
	 * @see #getEnrichedAttribute()
	 * @generated
	 */
	void setEnrichedAttribute(Attribute value);

	/**
	 * Returns the value of the '<em><b>Scenarios</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.openregspecs.model.testing.Scenario}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenarios</em>' containment reference list.
	 * @see org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage#getServiceTask_Scenarios()
	 * @model containment="true"
	 * @generated
	 */
	EList<Scenario> getScenarios();

	/**
	 * Returns the value of the '<em><b>Second Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Second Attribute</em>' reference.
	 * @see #setSecondAttribute(Attribute)
	 * @see org.eclipse.efbt.openregspecs.model.bpmn_lite.Bpmn_litePackage#getServiceTask_SecondAttribute()
	 * @model
	 * @generated
	 */
	Attribute getSecondAttribute();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.openregspecs.model.bpmn_lite.ServiceTask#getSecondAttribute <em>Second Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Second Attribute</em>' reference.
	 * @see #getSecondAttribute()
	 * @generated
	 */
	void setSecondAttribute(Attribute value);

} // ServiceTask
