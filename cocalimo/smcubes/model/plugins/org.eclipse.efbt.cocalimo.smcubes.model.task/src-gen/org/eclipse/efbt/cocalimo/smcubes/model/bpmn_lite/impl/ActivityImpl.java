/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Activity;
import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage;
import org.eclipse.emf.ecore.EClass;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Activity</b></em>'.
 * <!-- end-user-doc -->
 *
 * @generated
 */
public abstract class ActivityImpl extends FlowNodeImpl implements Activity {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ActivityImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Bpmn_litePackage.Literals.ACTIVITY;
	}

} //ActivityImpl
