/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunctionSpec;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateOperation;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Aggregate Operation</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateOperationImpl#getFunctionSpec <em>Function Spec</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AggregateOperationImpl extends OperationImpl implements AggregateOperation {
	/**
	 * The cached value of the '{@link #getFunctionSpec() <em>Function Spec</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFunctionSpec()
	 * @generated
	 * @ordered
	 */
	protected AggregateFunctionSpec functionSpec;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AggregateOperationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.AGGREGATE_OPERATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AggregateFunctionSpec getFunctionSpec() {
		if (functionSpec != null && functionSpec.eIsProxy()) {
			InternalEObject oldFunctionSpec = (InternalEObject)functionSpec;
			functionSpec = (AggregateFunctionSpec)eResolveProxy(oldFunctionSpec);
			if (functionSpec != oldFunctionSpec) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_logicPackage.AGGREGATE_OPERATION__FUNCTION_SPEC, oldFunctionSpec, functionSpec));
			}
		}
		return functionSpec;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AggregateFunctionSpec basicGetFunctionSpec() {
		return functionSpec;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setFunctionSpec(AggregateFunctionSpec newFunctionSpec) {
		AggregateFunctionSpec oldFunctionSpec = functionSpec;
		functionSpec = newFunctionSpec;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.AGGREGATE_OPERATION__FUNCTION_SPEC, oldFunctionSpec, functionSpec));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore_plus_logicPackage.AGGREGATE_OPERATION__FUNCTION_SPEC:
				if (resolve) return getFunctionSpec();
				return basicGetFunctionSpec();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Ecore_plus_logicPackage.AGGREGATE_OPERATION__FUNCTION_SPEC:
				setFunctionSpec((AggregateFunctionSpec)newValue);
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
			case Ecore_plus_logicPackage.AGGREGATE_OPERATION__FUNCTION_SPEC:
				setFunctionSpec((AggregateFunctionSpec)null);
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
			case Ecore_plus_logicPackage.AGGREGATE_OPERATION__FUNCTION_SPEC:
				return functionSpec != null;
		}
		return super.eIsSet(featureID);
	}

} //AggregateOperationImpl
