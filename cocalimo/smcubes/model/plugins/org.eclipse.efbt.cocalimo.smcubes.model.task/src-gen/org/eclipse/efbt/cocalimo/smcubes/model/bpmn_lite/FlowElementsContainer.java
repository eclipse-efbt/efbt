/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Flow Elements Container</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.FlowElementsContainer#getFlowElements <em>Flow Elements</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage#getFlowElementsContainer()
 * @model abstract="true"
 * @generated
 */
public interface FlowElementsContainer extends BaseElement {
	/**
	 * Returns the value of the '<em><b>Flow Elements</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.FlowElement}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Flow Elements</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage#getFlowElementsContainer_FlowElements()
	 * @model containment="true" ordered="false"
	 * @generated
	 */
	EList<FlowElement> getFlowElements();

} // FlowElementsContainer
