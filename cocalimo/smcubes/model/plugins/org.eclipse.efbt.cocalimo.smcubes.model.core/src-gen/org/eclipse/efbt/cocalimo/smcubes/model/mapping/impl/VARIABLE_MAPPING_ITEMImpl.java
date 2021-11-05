/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VARIABLE MAPPING ITEM</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_MAPPING_ITEMImpl#getIsSource <em>Is Source</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.VARIABLE_MAPPING_ITEMImpl#getVariable <em>Variable</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VARIABLE_MAPPING_ITEMImpl extends MinimalEObjectImpl.Container implements VARIABLE_MAPPING_ITEM {
	/**
	 * The default value of the '{@link #getIsSource() <em>Is Source</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIsSource()
	 * @generated
	 * @ordered
	 */
	protected static final String IS_SOURCE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getIsSource() <em>Is Source</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIsSource()
	 * @generated
	 * @ordered
	 */
	protected String isSource = IS_SOURCE_EDEFAULT;

	/**
	 * The cached value of the '{@link #getVariable() <em>Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE variable;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VARIABLE_MAPPING_ITEMImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return MappingPackage.Literals.VARIABLE_MAPPING_ITEM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getIsSource() {
		return isSource;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setIsSource(String newIsSource) {
		String oldIsSource = isSource;
		isSource = newIsSource;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.VARIABLE_MAPPING_ITEM__IS_SOURCE, oldIsSource, isSource));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getVariable() {
		if (variable != null && variable.eIsProxy()) {
			InternalEObject oldVariable = (InternalEObject)variable;
			variable = (VARIABLE)eResolveProxy(oldVariable);
			if (variable != oldVariable) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, MappingPackage.VARIABLE_MAPPING_ITEM__VARIABLE, oldVariable, variable));
			}
		}
		return variable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetVariable() {
		return variable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setVariable(VARIABLE newVariable) {
		VARIABLE oldVariable = variable;
		variable = newVariable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.VARIABLE_MAPPING_ITEM__VARIABLE, oldVariable, variable));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case MappingPackage.VARIABLE_MAPPING_ITEM__IS_SOURCE:
				return getIsSource();
			case MappingPackage.VARIABLE_MAPPING_ITEM__VARIABLE:
				if (resolve) return getVariable();
				return basicGetVariable();
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
			case MappingPackage.VARIABLE_MAPPING_ITEM__IS_SOURCE:
				setIsSource((String)newValue);
				return;
			case MappingPackage.VARIABLE_MAPPING_ITEM__VARIABLE:
				setVariable((VARIABLE)newValue);
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
			case MappingPackage.VARIABLE_MAPPING_ITEM__IS_SOURCE:
				setIsSource(IS_SOURCE_EDEFAULT);
				return;
			case MappingPackage.VARIABLE_MAPPING_ITEM__VARIABLE:
				setVariable((VARIABLE)null);
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
			case MappingPackage.VARIABLE_MAPPING_ITEM__IS_SOURCE:
				return IS_SOURCE_EDEFAULT == null ? isSource != null : !IS_SOURCE_EDEFAULT.equals(isSource);
			case MappingPackage.VARIABLE_MAPPING_ITEM__VARIABLE:
				return variable != null;
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
		result.append(" (isSource: ");
		result.append(isSource);
		result.append(')');
		return result.toString();
	}

} //VARIABLE_MAPPING_ITEMImpl
