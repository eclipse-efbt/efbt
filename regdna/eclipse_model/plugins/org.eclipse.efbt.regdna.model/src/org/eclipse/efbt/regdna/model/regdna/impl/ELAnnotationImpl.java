/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import java.util.Collection;

import org.eclipse.efbt.regdna.model.regdna.ELAnnotation;
import org.eclipse.efbt.regdna.model.regdna.ELAnnotationDirective;
import org.eclipse.efbt.regdna.model.regdna.ELStringToStringMapEntry;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>EL Annotation</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ELAnnotationImpl#getDetails <em>Details</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ELAnnotationImpl#getSource <em>Source</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ELAnnotationImpl extends ELModelElementImpl implements ELAnnotation {
	/**
	 * The cached value of the '{@link #getDetails() <em>Details</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDetails()
	 * @generated
	 * @ordered
	 */
	protected EList<ELStringToStringMapEntry> details;

	/**
	 * The cached value of the '{@link #getSource() <em>Source</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSource()
	 * @generated
	 * @ordered
	 */
	protected ELAnnotationDirective source;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ELAnnotationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getELAnnotation();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ELStringToStringMapEntry> getDetails() {
		if (details == null) {
			details = new EObjectContainmentEList<ELStringToStringMapEntry>(ELStringToStringMapEntry.class, this, regdnaPackage.EL_ANNOTATION__DETAILS);
		}
		return details;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELAnnotationDirective getSource() {
		if (source != null && source.eIsProxy()) {
			InternalEObject oldSource = (InternalEObject)source;
			source = (ELAnnotationDirective)eResolveProxy(oldSource);
			if (source != oldSource) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.EL_ANNOTATION__SOURCE, oldSource, source));
			}
		}
		return source;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELAnnotationDirective basicGetSource() {
		return source;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSource(ELAnnotationDirective newSource) {
		ELAnnotationDirective oldSource = source;
		source = newSource;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.EL_ANNOTATION__SOURCE, oldSource, source));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case regdnaPackage.EL_ANNOTATION__DETAILS:
				return ((InternalEList<?>)getDetails()).basicRemove(otherEnd, msgs);
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
			case regdnaPackage.EL_ANNOTATION__DETAILS:
				return getDetails();
			case regdnaPackage.EL_ANNOTATION__SOURCE:
				if (resolve) return getSource();
				return basicGetSource();
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
			case regdnaPackage.EL_ANNOTATION__DETAILS:
				getDetails().clear();
				getDetails().addAll((Collection<? extends ELStringToStringMapEntry>)newValue);
				return;
			case regdnaPackage.EL_ANNOTATION__SOURCE:
				setSource((ELAnnotationDirective)newValue);
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
			case regdnaPackage.EL_ANNOTATION__DETAILS:
				getDetails().clear();
				return;
			case regdnaPackage.EL_ANNOTATION__SOURCE:
				setSource((ELAnnotationDirective)null);
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
			case regdnaPackage.EL_ANNOTATION__DETAILS:
				return details != null && !details.isEmpty();
			case regdnaPackage.EL_ANNOTATION__SOURCE:
				return source != null;
		}
		return super.eIsSet(featureID);
	}

} //ELAnnotationImpl
