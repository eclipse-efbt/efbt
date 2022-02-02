/**
 */
package functions.impl;

import functions.AggregateFunction;
import functions.AggregateFunctionSpec;
import functions.FunctionsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Aggregate Function</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link functions.impl.AggregateFunctionImpl#getFunctionSpec <em>Function Spec</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AggregateFunctionImpl extends FunctionImpl implements AggregateFunction {
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
	protected AggregateFunctionImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return FunctionsPackage.Literals.AGGREGATE_FUNCTION;
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, FunctionsPackage.AGGREGATE_FUNCTION__FUNCTION_SPEC, oldFunctionSpec, functionSpec));
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
			eNotify(new ENotificationImpl(this, Notification.SET, FunctionsPackage.AGGREGATE_FUNCTION__FUNCTION_SPEC, oldFunctionSpec, functionSpec));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case FunctionsPackage.AGGREGATE_FUNCTION__FUNCTION_SPEC:
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
			case FunctionsPackage.AGGREGATE_FUNCTION__FUNCTION_SPEC:
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
			case FunctionsPackage.AGGREGATE_FUNCTION__FUNCTION_SPEC:
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
			case FunctionsPackage.AGGREGATE_FUNCTION__FUNCTION_SPEC:
				return functionSpec != null;
		}
		return super.eIsSet(featureID);
	}

} //AggregateFunctionImpl
