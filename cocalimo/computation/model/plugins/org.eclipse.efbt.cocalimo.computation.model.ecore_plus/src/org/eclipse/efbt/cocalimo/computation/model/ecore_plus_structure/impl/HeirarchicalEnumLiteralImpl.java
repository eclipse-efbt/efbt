/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.HeirarchicalEnumLiteral;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.EEnumLiteralImpl;
import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Heirarchical Enum Literal</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.HeirarchicalEnumLiteralImpl#getParentLiteral <em>Parent Literal</em>}</li>
 * </ul>
 *
 * @generated
 */
public class HeirarchicalEnumLiteralImpl extends EEnumLiteralImpl implements HeirarchicalEnumLiteral {
	/**
	 * The cached value of the '{@link #getParentLiteral() <em>Parent Literal</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getParentLiteral()
	 * @generated
	 * @ordered
	 */
	protected EEnumLiteral parentLiteral;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected HeirarchicalEnumLiteralImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_structurePackage.Literals.HEIRARCHICAL_ENUM_LITERAL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EEnumLiteral getParentLiteral() {
		if (parentLiteral != null && parentLiteral.eIsProxy()) {
			InternalEObject oldParentLiteral = (InternalEObject)parentLiteral;
			parentLiteral = (EEnumLiteral)eResolveProxy(oldParentLiteral);
			if (parentLiteral != oldParentLiteral) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_structurePackage.HEIRARCHICAL_ENUM_LITERAL__PARENT_LITERAL, oldParentLiteral, parentLiteral));
			}
		}
		return parentLiteral;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EEnumLiteral basicGetParentLiteral() {
		return parentLiteral;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setParentLiteral(EEnumLiteral newParentLiteral) {
		EEnumLiteral oldParentLiteral = parentLiteral;
		parentLiteral = newParentLiteral;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_structurePackage.HEIRARCHICAL_ENUM_LITERAL__PARENT_LITERAL, oldParentLiteral, parentLiteral));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore_plus_structurePackage.HEIRARCHICAL_ENUM_LITERAL__PARENT_LITERAL:
				if (resolve) return getParentLiteral();
				return basicGetParentLiteral();
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
			case Ecore_plus_structurePackage.HEIRARCHICAL_ENUM_LITERAL__PARENT_LITERAL:
				setParentLiteral((EEnumLiteral)newValue);
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
			case Ecore_plus_structurePackage.HEIRARCHICAL_ENUM_LITERAL__PARENT_LITERAL:
				setParentLiteral((EEnumLiteral)null);
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
			case Ecore_plus_structurePackage.HEIRARCHICAL_ENUM_LITERAL__PARENT_LITERAL:
				return parentLiteral != null;
		}
		return super.eIsSet(featureID);
	}

} //HeirarchicalEnumLiteralImpl
