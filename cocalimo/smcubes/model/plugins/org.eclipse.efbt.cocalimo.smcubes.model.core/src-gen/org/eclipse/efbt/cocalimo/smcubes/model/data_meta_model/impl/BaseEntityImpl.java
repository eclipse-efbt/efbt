/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BaseEntity;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage;

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
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.BaseEntityImpl#getSuperType <em>Super Type</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.BaseEntityImpl#getAttributes <em>Attributes</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BaseEntityImpl extends EntityImpl implements BaseEntity {
	/**
	 * The cached value of the '{@link #getSuperType() <em>Super Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSuperType()
	 * @generated
	 * @ordered
	 */
	protected BaseEntity superType;

	/**
	 * This is true if the Super Type reference has been set.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	protected boolean superTypeESet;

	/**
	 * The cached value of the '{@link #getAttributes() <em>Attributes</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAttributes()
	 * @generated
	 * @ordered
	 */
	protected EList<Attribute> attributes;

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
		return Data_meta_modelPackage.Literals.BASE_ENTITY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BaseEntity getSuperType() {
		if (superType != null && superType.eIsProxy()) {
			InternalEObject oldSuperType = (InternalEObject)superType;
			superType = (BaseEntity)eResolveProxy(oldSuperType);
			if (superType != oldSuperType) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_meta_modelPackage.BASE_ENTITY__SUPER_TYPE, oldSuperType, superType));
			}
		}
		return superType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseEntity basicGetSuperType() {
		return superType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSuperType(BaseEntity newSuperType) {
		BaseEntity oldSuperType = superType;
		superType = newSuperType;
		boolean oldSuperTypeESet = superTypeESet;
		superTypeESet = true;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.BASE_ENTITY__SUPER_TYPE, oldSuperType, superType, !oldSuperTypeESet));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void unsetSuperType() {
		BaseEntity oldSuperType = superType;
		boolean oldSuperTypeESet = superTypeESet;
		superType = null;
		superTypeESet = false;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.UNSET, Data_meta_modelPackage.BASE_ENTITY__SUPER_TYPE, oldSuperType, null, oldSuperTypeESet));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isSetSuperType() {
		return superTypeESet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Attribute> getAttributes() {
		if (attributes == null) {
			attributes = new EObjectContainmentEList<Attribute>(Attribute.class, this, Data_meta_modelPackage.BASE_ENTITY__ATTRIBUTES);
		}
		return attributes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Data_meta_modelPackage.BASE_ENTITY__ATTRIBUTES:
				return ((InternalEList<?>)getAttributes()).basicRemove(otherEnd, msgs);
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
			case Data_meta_modelPackage.BASE_ENTITY__SUPER_TYPE:
				if (resolve) return getSuperType();
				return basicGetSuperType();
			case Data_meta_modelPackage.BASE_ENTITY__ATTRIBUTES:
				return getAttributes();
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
			case Data_meta_modelPackage.BASE_ENTITY__SUPER_TYPE:
				setSuperType((BaseEntity)newValue);
				return;
			case Data_meta_modelPackage.BASE_ENTITY__ATTRIBUTES:
				getAttributes().clear();
				getAttributes().addAll((Collection<? extends Attribute>)newValue);
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
			case Data_meta_modelPackage.BASE_ENTITY__SUPER_TYPE:
				unsetSuperType();
				return;
			case Data_meta_modelPackage.BASE_ENTITY__ATTRIBUTES:
				getAttributes().clear();
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
			case Data_meta_modelPackage.BASE_ENTITY__SUPER_TYPE:
				return isSetSuperType();
			case Data_meta_modelPackage.BASE_ENTITY__ATTRIBUTES:
				return attributes != null && !attributes.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //BaseEntityImpl
