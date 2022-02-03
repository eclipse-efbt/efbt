/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityFeature;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;
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
 * An implementation of the model object '<em><b>Base Entity</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.BaseEntityImpl#getBaseFeatures <em>Base Features</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.BaseEntityImpl#getCube <em>Cube</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BaseEntityImpl extends EntityImpl implements BaseEntity {
	/**
	 * The cached value of the '{@link #getBaseFeatures() <em>Base Features</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBaseFeatures()
	 * @generated
	 * @ordered
	 */
	protected EList<BaseEntityFeature> baseFeatures;

	/**
	 * The cached value of the '{@link #getCube() <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube()
	 * @generated
	 * @ordered
	 */
	protected CUBE cube;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BaseEntityImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_structurePackage.Literals.BASE_ENTITY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<BaseEntityFeature> getBaseFeatures() {
		if (baseFeatures == null) {
			baseFeatures = new EObjectContainmentEList<BaseEntityFeature>(BaseEntityFeature.class, this, Ecore_plus_structurePackage.BASE_ENTITY__BASE_FEATURES);
		}
		return baseFeatures;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE getCube() {
		if (cube != null && cube.eIsProxy()) {
			InternalEObject oldCube = (InternalEObject)cube;
			cube = (CUBE)eResolveProxy(oldCube);
			if (cube != oldCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_structurePackage.BASE_ENTITY__CUBE, oldCube, cube));
			}
		}
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE basicGetCube() {
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCube(CUBE newCube) {
		CUBE oldCube = cube;
		cube = newCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_structurePackage.BASE_ENTITY__CUBE, oldCube, cube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_structurePackage.BASE_ENTITY__BASE_FEATURES:
				return ((InternalEList<?>)getBaseFeatures()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_structurePackage.BASE_ENTITY__BASE_FEATURES:
				return getBaseFeatures();
			case Ecore_plus_structurePackage.BASE_ENTITY__CUBE:
				if (resolve) return getCube();
				return basicGetCube();
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
			case Ecore_plus_structurePackage.BASE_ENTITY__BASE_FEATURES:
				getBaseFeatures().clear();
				getBaseFeatures().addAll((Collection<? extends BaseEntityFeature>)newValue);
				return;
			case Ecore_plus_structurePackage.BASE_ENTITY__CUBE:
				setCube((CUBE)newValue);
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
			case Ecore_plus_structurePackage.BASE_ENTITY__BASE_FEATURES:
				getBaseFeatures().clear();
				return;
			case Ecore_plus_structurePackage.BASE_ENTITY__CUBE:
				setCube((CUBE)null);
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
			case Ecore_plus_structurePackage.BASE_ENTITY__BASE_FEATURES:
				return baseFeatures != null && !baseFeatures.isEmpty();
			case Ecore_plus_structurePackage.BASE_ENTITY__CUBE:
				return cube != null;
		}
		return super.eIsSet(featureID);
	}

} //BaseEntityImpl
