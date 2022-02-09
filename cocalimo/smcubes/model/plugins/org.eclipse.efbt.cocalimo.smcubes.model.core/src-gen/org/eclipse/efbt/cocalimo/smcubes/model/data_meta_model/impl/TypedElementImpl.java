/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Classifier;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.TypedElement;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Typed Element</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.TypedElementImpl#getClassifier <em>Classifier</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class TypedElementImpl extends NamedElementImpl implements TypedElement {
	/**
	 * The cached value of the '{@link #getClassifier() <em>Classifier</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getClassifier()
	 * @generated
	 * @ordered
	 */
	protected Classifier classifier;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TypedElementImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_meta_modelPackage.Literals.TYPED_ELEMENT;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Classifier getClassifier() {
		if (classifier != null && classifier.eIsProxy()) {
			InternalEObject oldClassifier = (InternalEObject)classifier;
			classifier = (Classifier)eResolveProxy(oldClassifier);
			if (classifier != oldClassifier) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_meta_modelPackage.TYPED_ELEMENT__CLASSIFIER, oldClassifier, classifier));
			}
		}
		return classifier;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Classifier basicGetClassifier() {
		return classifier;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setClassifier(Classifier newClassifier) {
		Classifier oldClassifier = classifier;
		classifier = newClassifier;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.TYPED_ELEMENT__CLASSIFIER, oldClassifier, classifier));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_meta_modelPackage.TYPED_ELEMENT__CLASSIFIER:
				if (resolve) return getClassifier();
				return basicGetClassifier();
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
			case Data_meta_modelPackage.TYPED_ELEMENT__CLASSIFIER:
				setClassifier((Classifier)newValue);
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
			case Data_meta_modelPackage.TYPED_ELEMENT__CLASSIFIER:
				setClassifier((Classifier)null);
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
			case Data_meta_modelPackage.TYPED_ELEMENT__CLASSIFIER:
				return classifier != null;
		}
		return super.eIsSet(featureID);
	}

} //TypedElementImpl
