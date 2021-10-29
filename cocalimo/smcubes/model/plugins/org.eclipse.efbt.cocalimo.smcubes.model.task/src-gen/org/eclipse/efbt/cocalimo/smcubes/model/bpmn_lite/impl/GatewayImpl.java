/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage;
import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Gateway;

import org.eclipse.emf.ecore.EClass;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Gateway</b></em>'.
 * <!-- end-user-doc -->
 *
 * @generated
 */
public abstract class GatewayImpl extends FlowNodeImpl implements Gateway {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected GatewayImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Bpmn_litePackage.Literals.GATEWAY;
	}

} //GatewayImpl
