/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import org.eclipse.efbt.regdna.model.regdna.ELAnnotation;
import org.eclipse.efbt.regdna.model.regdna.ELModelElement;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>EL Model Element</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ELModelElementImpl#getEAnnotations <em>EAnnotations</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class ELModelElementImpl extends MinimalEObjectImpl.Container implements ELModelElement {
	/**
	 * The cached value of the '{@link #getEAnnotations() <em>EAnnotations</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEAnnotations()
	 * @generated
	 * @ordered
	 */
	protected ELAnnotation eAnnotations;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ELModelElementImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getELModelElement();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELAnnotation getEAnnotations() {
		return eAnnotations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetEAnnotations(ELAnnotation newEAnnotations, NotificationChain msgs) {
		ELAnnotation oldEAnnotations = eAnnotations;
		eAnnotations = newEAnnotations;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, regdnaPackage.EL_MODEL_ELEMENT__EANNOTATIONS, oldEAnnotations, newEAnnotations);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setEAnnotations(ELAnnotation newEAnnotations) {
		if (newEAnnotations != eAnnotations) {
			NotificationChain msgs = null;
			if (eAnnotations != null)
				msgs = ((InternalEObject)eAnnotations).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - regdnaPackage.EL_MODEL_ELEMENT__EANNOTATIONS, null, msgs);
			if (newEAnnotations != null)
				msgs = ((InternalEObject)newEAnnotations).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - regdnaPackage.EL_MODEL_ELEMENT__EANNOTATIONS, null, msgs);
			msgs = basicSetEAnnotations(newEAnnotations, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.EL_MODEL_ELEMENT__EANNOTATIONS, newEAnnotations, newEAnnotations));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case regdnaPackage.EL_MODEL_ELEMENT__EANNOTATIONS:
				return basicSetEAnnotations(null, msgs);
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
			case regdnaPackage.EL_MODEL_ELEMENT__EANNOTATIONS:
				return getEAnnotations();
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
			case regdnaPackage.EL_MODEL_ELEMENT__EANNOTATIONS:
				setEAnnotations((ELAnnotation)newValue);
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
			case regdnaPackage.EL_MODEL_ELEMENT__EANNOTATIONS:
				setEAnnotations((ELAnnotation)null);
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
			case regdnaPackage.EL_MODEL_ELEMENT__EANNOTATIONS:
				return eAnnotations != null;
		}
		return super.eIsSet(featureID);
	}

} //ELModelElementImpl
