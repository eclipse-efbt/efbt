/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunctionSpec;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Basic Function Spec</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicFunctionSpecImpl#isIsInfix <em>Is Infix</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BasicFunctionSpecImpl extends FunctionSpecImpl implements BasicFunctionSpec {
	/**
	 * The default value of the '{@link #isIsInfix() <em>Is Infix</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIsInfix()
	 * @generated
	 * @ordered
	 */
	protected static final boolean IS_INFIX_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isIsInfix() <em>Is Infix</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIsInfix()
	 * @generated
	 * @ordered
	 */
	protected boolean isInfix = IS_INFIX_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BasicFunctionSpecImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.BASIC_FUNCTION_SPEC;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isIsInfix() {
		return isInfix;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setIsInfix(boolean newIsInfix) {
		boolean oldIsInfix = isInfix;
		isInfix = newIsInfix;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.BASIC_FUNCTION_SPEC__IS_INFIX, oldIsInfix, isInfix));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore_plus_logicPackage.BASIC_FUNCTION_SPEC__IS_INFIX:
				return isIsInfix();
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
			case Ecore_plus_logicPackage.BASIC_FUNCTION_SPEC__IS_INFIX:
				setIsInfix((Boolean)newValue);
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
			case Ecore_plus_logicPackage.BASIC_FUNCTION_SPEC__IS_INFIX:
				setIsInfix(IS_INFIX_EDEFAULT);
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
			case Ecore_plus_logicPackage.BASIC_FUNCTION_SPEC__IS_INFIX:
				return isInfix != IS_INFIX_EDEFAULT;
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (isInfix: ");
		result.append(isInfix);
		result.append(')');
		return result.toString();
	}

} //BasicFunctionSpecImpl
