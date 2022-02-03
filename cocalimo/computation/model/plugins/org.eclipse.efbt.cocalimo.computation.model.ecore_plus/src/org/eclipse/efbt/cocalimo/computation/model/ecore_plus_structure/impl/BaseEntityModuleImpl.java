/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityModule;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Base Entity Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.BaseEntityModuleImpl#getBaseEntities <em>Base Entities</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BaseEntityModuleImpl extends ModuleImpl implements BaseEntityModule {
	/**
	 * The cached value of the '{@link #getBaseEntities() <em>Base Entities</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBaseEntities()
	 * @generated
	 * @ordered
	 */
	protected BaseEntity baseEntities;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BaseEntityModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_structurePackage.Literals.BASE_ENTITY_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseEntity getBaseEntities() {
		return baseEntities;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetBaseEntities(BaseEntity newBaseEntities, NotificationChain msgs) {
		BaseEntity oldBaseEntities = baseEntities;
		baseEntities = newBaseEntities;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES, oldBaseEntities, newBaseEntities);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setBaseEntities(BaseEntity newBaseEntities) {
		if (newBaseEntities != baseEntities) {
			NotificationChain msgs = null;
			if (baseEntities != null)
				msgs = ((InternalEObject)baseEntities).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES, null, msgs);
			if (newBaseEntities != null)
				msgs = ((InternalEObject)newBaseEntities).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES, null, msgs);
			msgs = basicSetBaseEntities(newBaseEntities, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES, newBaseEntities, newBaseEntities));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES:
				return basicSetBaseEntities(null, msgs);
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
			case Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES:
				return getBaseEntities();
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
			case Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES:
				setBaseEntities((BaseEntity)newValue);
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
			case Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES:
				setBaseEntities((BaseEntity)null);
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
			case Ecore_plus_structurePackage.BASE_ENTITY_MODULE__BASE_ENTITIES:
				return baseEntities != null;
		}
		return super.eIsSet(featureID);
	}

} //BaseEntityModuleImpl
