/**
 */
package org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Sequence Flow</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.SequenceFlow#getTargetRef <em>Target Ref</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.SequenceFlow#getSourceRef <em>Source Ref</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.Bpmn_litePackage#getSequenceFlow()
 * @model
 * @generated
 */
public interface SequenceFlow extends FlowElement {
	/**
	 * Returns the value of the '<em><b>Target Ref</b></em>' reference.
	 * It is bidirectional and its opposite is '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.FlowNode#getIncoming <em>Incoming</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Target Ref</em>' reference.
	 * @see #setTargetRef(FlowNode)
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.Bpmn_litePackage#getSequenceFlow_TargetRef()
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.FlowNode#getIncoming
	 * @model opposite="incoming" ordered="false"
	 * @generated
	 */
	FlowNode getTargetRef();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.SequenceFlow#getTargetRef <em>Target Ref</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Target Ref</em>' reference.
	 * @see #getTargetRef()
	 * @generated
	 */
	void setTargetRef(FlowNode value);

	/**
	 * Returns the value of the '<em><b>Source Ref</b></em>' reference.
	 * It is bidirectional and its opposite is '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.FlowNode#getOutgoing <em>Outgoing</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Source Ref</em>' reference.
	 * @see #setSourceRef(FlowNode)
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.Bpmn_litePackage#getSequenceFlow_SourceRef()
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.FlowNode#getOutgoing
	 * @model opposite="outgoing" ordered="false"
	 * @generated
	 */
	FlowNode getSourceRef();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.bpmn_lite.SequenceFlow#getSourceRef <em>Source Ref</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Source Ref</em>' reference.
	 * @see #getSourceRef()
	 * @generated
	 */
	void setSourceRef(FlowNode value);

} // SequenceFlow
