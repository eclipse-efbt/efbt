/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import java.util.Collection;

import org.eclipse.efbt.regdna.model.regdna.AllowedTypes;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;
import org.eclipse.efbt.regdna.model.regdna.RequirementType;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Allowed Types</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.AllowedTypesImpl#getAllowedTypes <em>Allowed Types</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AllowedTypesImpl extends MinimalEObjectImpl.Container implements AllowedTypes {
	/**
	 * The cached value of the '{@link #getAllowedTypes() <em>Allowed Types</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAllowedTypes()
	 * @generated
	 * @ordered
	 */
	protected EList<RequirementType> allowedTypes;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AllowedTypesImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getAllowedTypes();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<RequirementType> getAllowedTypes() {
		if (allowedTypes == null) {
			allowedTypes = new EObjectContainmentEList<RequirementType>(RequirementType.class, this, regdnaPackage.ALLOWED_TYPES__ALLOWED_TYPES);
		}
		return allowedTypes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case regdnaPackage.ALLOWED_TYPES__ALLOWED_TYPES:
				return ((InternalEList<?>)getAllowedTypes()).basicRemove(otherEnd, msgs);
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
			case regdnaPackage.ALLOWED_TYPES__ALLOWED_TYPES:
				return getAllowedTypes();
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
			case regdnaPackage.ALLOWED_TYPES__ALLOWED_TYPES:
				getAllowedTypes().clear();
				getAllowedTypes().addAll((Collection<? extends RequirementType>)newValue);
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
			case regdnaPackage.ALLOWED_TYPES__ALLOWED_TYPES:
				getAllowedTypes().clear();
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
			case regdnaPackage.ALLOWED_TYPES__ALLOWED_TYPES:
				return allowedTypes != null && !allowedTypes.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //AllowedTypesImpl
