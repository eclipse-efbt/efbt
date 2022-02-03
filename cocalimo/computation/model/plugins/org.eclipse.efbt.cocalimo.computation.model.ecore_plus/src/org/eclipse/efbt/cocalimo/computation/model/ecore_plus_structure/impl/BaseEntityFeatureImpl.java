/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.BaseEntityFeature;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.EAttributeImpl;
import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Base Entity Feature</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.BaseEntityFeatureImpl#isIsFK <em>Is FK</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.BaseEntityFeatureImpl#getFkEntity <em>Fk Entity</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.BaseEntityFeatureImpl#getVaraible <em>Varaible</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.BaseEntityFeatureImpl#getCube <em>Cube</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BaseEntityFeatureImpl extends EAttributeImpl implements BaseEntityFeature {
	/**
	 * The default value of the '{@link #isIsFK() <em>Is FK</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIsFK()
	 * @generated
	 * @ordered
	 */
	protected static final boolean IS_FK_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isIsFK() <em>Is FK</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIsFK()
	 * @generated
	 * @ordered
	 */
	protected boolean isFK = IS_FK_EDEFAULT;

	/**
	 * The cached value of the '{@link #getFkEntity() <em>Fk Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFkEntity()
	 * @generated
	 * @ordered
	 */
	protected BaseEntity fkEntity;

	/**
	 * The cached value of the '{@link #getVaraible() <em>Varaible</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVaraible()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE varaible;

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
	protected BaseEntityFeatureImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_structurePackage.Literals.BASE_ENTITY_FEATURE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isIsFK() {
		return isFK;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setIsFK(boolean newIsFK) {
		boolean oldIsFK = isFK;
		isFK = newIsFK;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__IS_FK, oldIsFK, isFK));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseEntity getFkEntity() {
		if (fkEntity != null && fkEntity.eIsProxy()) {
			InternalEObject oldFkEntity = (InternalEObject)fkEntity;
			fkEntity = (BaseEntity)eResolveProxy(oldFkEntity);
			if (fkEntity != oldFkEntity) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__FK_ENTITY, oldFkEntity, fkEntity));
			}
		}
		return fkEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseEntity basicGetFkEntity() {
		return fkEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setFkEntity(BaseEntity newFkEntity) {
		BaseEntity oldFkEntity = fkEntity;
		fkEntity = newFkEntity;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__FK_ENTITY, oldFkEntity, fkEntity));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE getVaraible() {
		if (varaible != null && varaible.eIsProxy()) {
			InternalEObject oldVaraible = (InternalEObject)varaible;
			varaible = (VARIABLE)eResolveProxy(oldVaraible);
			if (varaible != oldVaraible) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__VARAIBLE, oldVaraible, varaible));
			}
		}
		return varaible;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetVaraible() {
		return varaible;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVaraible(VARIABLE newVaraible) {
		VARIABLE oldVaraible = varaible;
		varaible = newVaraible;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__VARAIBLE, oldVaraible, varaible));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__CUBE, oldCube, cube));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__CUBE, oldCube, cube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__IS_FK:
				return isIsFK();
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__FK_ENTITY:
				if (resolve) return getFkEntity();
				return basicGetFkEntity();
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__VARAIBLE:
				if (resolve) return getVaraible();
				return basicGetVaraible();
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__CUBE:
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
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__IS_FK:
				setIsFK((Boolean)newValue);
				return;
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__FK_ENTITY:
				setFkEntity((BaseEntity)newValue);
				return;
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__VARAIBLE:
				setVaraible((VARIABLE)newValue);
				return;
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__CUBE:
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
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__IS_FK:
				setIsFK(IS_FK_EDEFAULT);
				return;
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__FK_ENTITY:
				setFkEntity((BaseEntity)null);
				return;
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__VARAIBLE:
				setVaraible((VARIABLE)null);
				return;
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__CUBE:
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
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__IS_FK:
				return isFK != IS_FK_EDEFAULT;
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__FK_ENTITY:
				return fkEntity != null;
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__VARAIBLE:
				return varaible != null;
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE__CUBE:
				return cube != null;
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
		result.append(" (isFK: ");
		result.append(isFK);
		result.append(')');
		return result.toString();
	}

} //BaseEntityFeatureImpl
