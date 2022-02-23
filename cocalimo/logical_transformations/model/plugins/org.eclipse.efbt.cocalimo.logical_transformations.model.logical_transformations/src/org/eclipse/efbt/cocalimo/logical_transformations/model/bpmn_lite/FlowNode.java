/**
 */
package org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Flow Node</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.FlowNode#getOutgoing <em>Outgoing</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.FlowNode#getIncoming <em>Incoming</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.Bpmn_litePackage#getFlowNode()
 * @model abstract="true"
 * @generated
 */
public interface FlowNode extends FlowElement {
	/**
	 * Returns the value of the '<em><b>Outgoing</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.SequenceFlow}.
	 * It is bidirectional and its opposite is '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.SequenceFlow#getSourceRef <em>Source Ref</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Outgoing</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.Bpmn_litePackage#getFlowNode_Outgoing()
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.SequenceFlow#getSourceRef
	 * @model opposite="sourceRef"
	 * @generated
	 */
	EList<SequenceFlow> getOutgoing();

	/**
	 * Returns the value of the '<em><b>Incoming</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.SequenceFlow}.
	 * It is bidirectional and its opposite is '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.SequenceFlow#getTargetRef <em>Target Ref</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Incoming</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.Bpmn_litePackage#getFlowNode_Incoming()
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.SequenceFlow#getTargetRef
	 * @model opposite="targetRef" ordered="false"
	 * @generated
	 */
	EList<SequenceFlow> getIncoming();

} // FlowNode
