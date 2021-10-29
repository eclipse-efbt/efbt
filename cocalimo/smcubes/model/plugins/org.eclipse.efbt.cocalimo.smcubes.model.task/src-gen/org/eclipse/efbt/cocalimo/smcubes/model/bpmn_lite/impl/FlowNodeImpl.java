/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage;
import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.FlowNode;
import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.SequenceFlow;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectWithInverseResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Flow Node</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.impl.FlowNodeImpl#getOutgoing <em>Outgoing</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.impl.FlowNodeImpl#getIncoming <em>Incoming</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class FlowNodeImpl extends FlowElementImpl implements FlowNode {
	/**
	 * The cached value of the '{@link #getOutgoing() <em>Outgoing</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOutgoing()
	 * @generated
	 * @ordered
	 */
	protected EList<SequenceFlow> outgoing;

	/**
	 * The cached value of the '{@link #getIncoming() <em>Incoming</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIncoming()
	 * @generated
	 * @ordered
	 */
	protected EList<SequenceFlow> incoming;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FlowNodeImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Bpmn_litePackage.Literals.FLOW_NODE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<SequenceFlow> getOutgoing() {
		if (outgoing == null) {
			outgoing = new EObjectWithInverseResolvingEList<SequenceFlow>(SequenceFlow.class, this, Bpmn_litePackage.FLOW_NODE__OUTGOING, Bpmn_litePackage.SEQUENCE_FLOW__SOURCE_REF);
		}
		return outgoing;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<SequenceFlow> getIncoming() {
		if (incoming == null) {
			incoming = new EObjectWithInverseResolvingEList<SequenceFlow>(SequenceFlow.class, this, Bpmn_litePackage.FLOW_NODE__INCOMING, Bpmn_litePackage.SEQUENCE_FLOW__TARGET_REF);
		}
		return incoming;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public NotificationChain eInverseAdd(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Bpmn_litePackage.FLOW_NODE__OUTGOING:
				return ((InternalEList<InternalEObject>)(InternalEList<?>)getOutgoing()).basicAdd(otherEnd, msgs);
			case Bpmn_litePackage.FLOW_NODE__INCOMING:
				return ((InternalEList<InternalEObject>)(InternalEList<?>)getIncoming()).basicAdd(otherEnd, msgs);
		}
		return super.eInverseAdd(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Bpmn_litePackage.FLOW_NODE__OUTGOING:
				return ((InternalEList<?>)getOutgoing()).basicRemove(otherEnd, msgs);
			case Bpmn_litePackage.FLOW_NODE__INCOMING:
				return ((InternalEList<?>)getIncoming()).basicRemove(otherEnd, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Bpmn_litePackage.FLOW_NODE__OUTGOING:
				return getOutgoing();
			case Bpmn_litePackage.FLOW_NODE__INCOMING:
				return getIncoming();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Bpmn_litePackage.FLOW_NODE__OUTGOING:
				getOutgoing().clear();
				getOutgoing().addAll((Collection<? extends SequenceFlow>)newValue);
				return;
			case Bpmn_litePackage.FLOW_NODE__INCOMING:
				getIncoming().clear();
				getIncoming().addAll((Collection<? extends SequenceFlow>)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID) {
		switch (featureID) {
			case Bpmn_litePackage.FLOW_NODE__OUTGOING:
				getOutgoing().clear();
				return;
			case Bpmn_litePackage.FLOW_NODE__INCOMING:
				getIncoming().clear();
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case Bpmn_litePackage.FLOW_NODE__OUTGOING:
				return outgoing != null && !outgoing.isEmpty();
			case Bpmn_litePackage.FLOW_NODE__INCOMING:
				return incoming != null && !incoming.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //FlowNodeImpl
