/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELClassifier;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.ELPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>EL Classifier</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.ELClassifierImpl#getPackage <em>Package</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class ELClassifierImpl extends ELNamedElementImpl implements ELClassifier {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ELClassifierImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getELClassifier();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELPackage getPackage() {
		if (eContainerFeatureID() != Ecore4regPackage.EL_CLASSIFIER__PACKAGE) return null;
		return (ELPackage)eInternalContainer();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseAdd(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.EL_CLASSIFIER__PACKAGE:
				if (eInternalContainer() != null)
					msgs = eBasicRemoveFromContainer(msgs);
				return eBasicSetContainer(otherEnd, Ecore4regPackage.EL_CLASSIFIER__PACKAGE, msgs);
		}
		return super.eInverseAdd(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.EL_CLASSIFIER__PACKAGE:
				return eBasicSetContainer(null, Ecore4regPackage.EL_CLASSIFIER__PACKAGE, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eBasicRemoveFromContainerFeature(NotificationChain msgs) {
		switch (eContainerFeatureID()) {
			case Ecore4regPackage.EL_CLASSIFIER__PACKAGE:
				return eInternalContainer().eInverseRemove(this, Ecore4regPackage.EL_PACKAGE__ECLASSIFIERS, ELPackage.class, msgs);
		}
		return super.eBasicRemoveFromContainerFeature(msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore4regPackage.EL_CLASSIFIER__PACKAGE:
				return getPackage();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case Ecore4regPackage.EL_CLASSIFIER__PACKAGE:
				return getPackage() != null;
		}
		return super.eIsSet(featureID);
	}

} //ELClassifierImpl
