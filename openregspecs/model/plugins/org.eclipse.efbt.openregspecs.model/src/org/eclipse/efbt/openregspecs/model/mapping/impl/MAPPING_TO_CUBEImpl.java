/**
 */
package org.eclipse.efbt.openregspecs.model.mapping.impl;

import org.eclipse.efbt.openregspecs.model.mapping.MAPPING_DEFINITION;
import org.eclipse.efbt.openregspecs.model.mapping.MAPPING_TO_CUBE;
import org.eclipse.efbt.openregspecs.model.mapping.MappingPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>MAPPING TO CUBE</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.MAPPING_TO_CUBEImpl#getMapping <em>Mapping</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MAPPING_TO_CUBEImpl extends MinimalEObjectImpl.Container implements MAPPING_TO_CUBE {
	/**
	 * The cached value of the '{@link #getMapping() <em>Mapping</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMapping()
	 * @generated
	 * @ordered
	 */
	protected MAPPING_DEFINITION mapping;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected MAPPING_TO_CUBEImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return MappingPackage.Literals.MAPPING_TO_CUBE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MAPPING_DEFINITION getMapping() {
		if (mapping != null && mapping.eIsProxy()) {
			InternalEObject oldMapping = (InternalEObject)mapping;
			mapping = (MAPPING_DEFINITION)eResolveProxy(oldMapping);
			if (mapping != oldMapping) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, MappingPackage.MAPPING_TO_CUBE__MAPPING, oldMapping, mapping));
			}
		}
		return mapping;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MAPPING_DEFINITION basicGetMapping() {
		return mapping;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setMapping(MAPPING_DEFINITION newMapping) {
		MAPPING_DEFINITION oldMapping = mapping;
		mapping = newMapping;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MAPPING_TO_CUBE__MAPPING, oldMapping, mapping));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case MappingPackage.MAPPING_TO_CUBE__MAPPING:
				if (resolve) return getMapping();
				return basicGetMapping();
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
			case MappingPackage.MAPPING_TO_CUBE__MAPPING:
				setMapping((MAPPING_DEFINITION)newValue);
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
			case MappingPackage.MAPPING_TO_CUBE__MAPPING:
				setMapping((MAPPING_DEFINITION)null);
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
			case MappingPackage.MAPPING_TO_CUBE__MAPPING:
				return mapping != null;
		}
		return super.eIsSet(featureID);
	}

} //MAPPING_TO_CUBEImpl
