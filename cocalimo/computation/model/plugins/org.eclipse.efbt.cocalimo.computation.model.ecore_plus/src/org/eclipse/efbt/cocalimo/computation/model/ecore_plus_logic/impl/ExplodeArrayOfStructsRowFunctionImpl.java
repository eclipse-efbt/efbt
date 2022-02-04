/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ExplodeArrayOfStructsRowFunction;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.ETypedElement;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Explode Array Of Structs Row Function</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ExplodeArrayOfStructsRowFunctionImpl#getArraySourceVariable <em>Array Source Variable</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ExplodeArrayOfStructsRowFunctionImpl extends RowCreationApproachImpl implements ExplodeArrayOfStructsRowFunction {
	/**
	 * The cached value of the '{@link #getArraySourceVariable() <em>Array Source Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getArraySourceVariable()
	 * @generated
	 * @ordered
	 */
	protected ETypedElement arraySourceVariable;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ExplodeArrayOfStructsRowFunctionImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ETypedElement getArraySourceVariable() {
		if (arraySourceVariable != null && arraySourceVariable.eIsProxy()) {
			InternalEObject oldArraySourceVariable = (InternalEObject)arraySourceVariable;
			arraySourceVariable = (ETypedElement)eResolveProxy(oldArraySourceVariable);
			if (arraySourceVariable != oldArraySourceVariable) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_logicPackage.EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE, oldArraySourceVariable, arraySourceVariable));
			}
		}
		return arraySourceVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ETypedElement basicGetArraySourceVariable() {
		return arraySourceVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setArraySourceVariable(ETypedElement newArraySourceVariable) {
		ETypedElement oldArraySourceVariable = arraySourceVariable;
		arraySourceVariable = newArraySourceVariable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE, oldArraySourceVariable, arraySourceVariable));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore_plus_logicPackage.EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE:
				if (resolve) return getArraySourceVariable();
				return basicGetArraySourceVariable();
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
			case Ecore_plus_logicPackage.EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE:
				setArraySourceVariable((ETypedElement)newValue);
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
			case Ecore_plus_logicPackage.EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE:
				setArraySourceVariable((ETypedElement)null);
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
			case Ecore_plus_logicPackage.EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE:
				return arraySourceVariable != null;
		}
		return super.eIsSet(featureID);
	}

} //ExplodeArrayOfStructsRowFunctionImpl
