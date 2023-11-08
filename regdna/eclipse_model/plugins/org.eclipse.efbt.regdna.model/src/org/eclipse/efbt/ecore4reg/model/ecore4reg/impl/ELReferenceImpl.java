/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import org.eclipse.efbt.regdna.model.regdna.ELClassifier;
import org.eclipse.efbt.regdna.model.regdna.ELReference;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>EL Reference</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ELReferenceImpl#isContainment <em>Containment</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ELReferenceImpl#getEReferenceType <em>EReference Type</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ELReferenceImpl extends ELStructuralFeatureImpl implements ELReference {
	/**
	 * The default value of the '{@link #isContainment() <em>Containment</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isContainment()
	 * @generated
	 * @ordered
	 */
	protected static final boolean CONTAINMENT_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isContainment() <em>Containment</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isContainment()
	 * @generated
	 * @ordered
	 */
	protected boolean containment = CONTAINMENT_EDEFAULT;

	/**
	 * The cached value of the '{@link #getEReferenceType() <em>EReference Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEReferenceType()
	 * @generated
	 * @ordered
	 */
	protected ELClassifier eReferenceType;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ELReferenceImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getELReference();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isContainment() {
		return containment;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setContainment(boolean newContainment) {
		boolean oldContainment = containment;
		containment = newContainment;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.EL_REFERENCE__CONTAINMENT, oldContainment, containment));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELClassifier getEReferenceType() {
		if (eReferenceType != null && eReferenceType.eIsProxy()) {
			InternalEObject oldEReferenceType = (InternalEObject)eReferenceType;
			eReferenceType = (ELClassifier)eResolveProxy(oldEReferenceType);
			if (eReferenceType != oldEReferenceType) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.EL_REFERENCE__EREFERENCE_TYPE, oldEReferenceType, eReferenceType));
			}
		}
		return eReferenceType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELClassifier basicGetEReferenceType() {
		return eReferenceType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setEReferenceType(ELClassifier newEReferenceType) {
		ELClassifier oldEReferenceType = eReferenceType;
		eReferenceType = newEReferenceType;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.EL_REFERENCE__EREFERENCE_TYPE, oldEReferenceType, eReferenceType));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case regdnaPackage.EL_REFERENCE__CONTAINMENT:
				return isContainment();
			case regdnaPackage.EL_REFERENCE__EREFERENCE_TYPE:
				if (resolve) return getEReferenceType();
				return basicGetEReferenceType();
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
			case regdnaPackage.EL_REFERENCE__CONTAINMENT:
				setContainment((Boolean)newValue);
				return;
			case regdnaPackage.EL_REFERENCE__EREFERENCE_TYPE:
				setEReferenceType((ELClassifier)newValue);
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
			case regdnaPackage.EL_REFERENCE__CONTAINMENT:
				setContainment(CONTAINMENT_EDEFAULT);
				return;
			case regdnaPackage.EL_REFERENCE__EREFERENCE_TYPE:
				setEReferenceType((ELClassifier)null);
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
			case regdnaPackage.EL_REFERENCE__CONTAINMENT:
				return containment != CONTAINMENT_EDEFAULT;
			case regdnaPackage.EL_REFERENCE__EREFERENCE_TYPE:
				return eReferenceType != null;
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
		result.append(" (containment: ");
		result.append(containment);
		result.append(')');
		return result.toString();
	}

} //ELReferenceImpl
