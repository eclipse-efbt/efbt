/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedAttribute;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedFeatureGroup;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Derived Feature Group</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedFeatureGroupImpl#getDerivedFeatures <em>Derived Features</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DerivedFeatureGroupImpl extends MinimalEObjectImpl.Container implements DerivedFeatureGroup {
	/**
	 * The cached value of the '{@link #getDerivedFeatures() <em>Derived Features</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDerivedFeatures()
	 * @generated
	 * @ordered
	 */
	protected EList<DerivedAttribute> derivedFeatures;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DerivedFeatureGroupImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.DERIVED_FEATURE_GROUP;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<DerivedAttribute> getDerivedFeatures() {
		if (derivedFeatures == null) {
			derivedFeatures = new EObjectContainmentEList<DerivedAttribute>(DerivedAttribute.class, this, Ecore_plus_logicPackage.DERIVED_FEATURE_GROUP__DERIVED_FEATURES);
		}
		return derivedFeatures;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.DERIVED_FEATURE_GROUP__DERIVED_FEATURES:
				return ((InternalEList<?>)getDerivedFeatures()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_logicPackage.DERIVED_FEATURE_GROUP__DERIVED_FEATURES:
				return getDerivedFeatures();
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
			case Ecore_plus_logicPackage.DERIVED_FEATURE_GROUP__DERIVED_FEATURES:
				getDerivedFeatures().clear();
				getDerivedFeatures().addAll((Collection<? extends DerivedAttribute>)newValue);
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
			case Ecore_plus_logicPackage.DERIVED_FEATURE_GROUP__DERIVED_FEATURES:
				getDerivedFeatures().clear();
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
			case Ecore_plus_logicPackage.DERIVED_FEATURE_GROUP__DERIVED_FEATURES:
				return derivedFeatures != null && !derivedFeatures.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //DerivedFeatureGroupImpl
