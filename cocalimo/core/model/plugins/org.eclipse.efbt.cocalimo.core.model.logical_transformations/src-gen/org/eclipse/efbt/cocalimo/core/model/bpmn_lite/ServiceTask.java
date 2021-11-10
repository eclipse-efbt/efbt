/**
 */
package org.eclipse.efbt.cocalimo.core.model.bpmn_lite;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;
import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EAttribute;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Service Task</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ServiceTask#getEnrichedAttribute <em>Enriched Attribute</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ServiceTask#getScenarios <em>Scenarios</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ServiceTask#getLinkedAttributes <em>Linked Attributes</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage#getServiceTask()
 * @model
 * @generated
 */
public interface ServiceTask extends Task {
	/**
	 * Returns the value of the '<em><b>Enriched Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Enriched Attribute</em>' reference.
	 * @see #setEnrichedAttribute(EAttribute)
	 * @see org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage#getServiceTask_EnrichedAttribute()
	 * @model
	 * @generated
	 */
	EAttribute getEnrichedAttribute();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ServiceTask#getEnrichedAttribute <em>Enriched Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Enriched Attribute</em>' reference.
	 * @see #getEnrichedAttribute()
	 * @generated
	 */
	void setEnrichedAttribute(EAttribute value);

	/**
	 * Returns the value of the '<em><b>Scenarios</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Scenarios</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage#getServiceTask_Scenarios()
	 * @model containment="true"
	 * @generated
	 */
	EList<Scenario> getScenarios();

	/**
	 * Returns the value of the '<em><b>Linked Attributes</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.emf.ecore.EAttribute}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Linked Attributes</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage#getServiceTask_LinkedAttributes()
	 * @model
	 * @generated
	 */
	EList<EAttribute> getLinkedAttributes();

} // ServiceTask
