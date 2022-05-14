/**
 */
package org.eclipse.efbt.openregspecs.model.mapping.impl;

import org.eclipse.efbt.openregspecs.model.mapping.MAPPING_DEFINITION;
import org.eclipse.efbt.openregspecs.model.mapping.MappingPackage;
import org.eclipse.efbt.openregspecs.model.mapping.VARIABLE_SET_MAPPING;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VARIABLE SET MAPPING</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.VARIABLE_SET_MAPPINGImpl#getSourceMapping <em>Source Mapping</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.VARIABLE_SET_MAPPINGImpl#getTargetMapping <em>Target Mapping</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VARIABLE_SET_MAPPINGImpl extends MinimalEObjectImpl.Container implements VARIABLE_SET_MAPPING {
	/**
	 * The cached value of the '{@link #getSourceMapping() <em>Source Mapping</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSourceMapping()
	 * @generated
	 * @ordered
	 */
	protected MAPPING_DEFINITION sourceMapping;

	/**
	 * The cached value of the '{@link #getTargetMapping() <em>Target Mapping</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTargetMapping()
	 * @generated
	 * @ordered
	 */
	protected MAPPING_DEFINITION targetMapping;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VARIABLE_SET_MAPPINGImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return MappingPackage.Literals.VARIABLE_SET_MAPPING;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MAPPING_DEFINITION getSourceMapping() {
		if (sourceMapping != null && sourceMapping.eIsProxy()) {
			InternalEObject oldSourceMapping = (InternalEObject)sourceMapping;
			sourceMapping = (MAPPING_DEFINITION)eResolveProxy(oldSourceMapping);
			if (sourceMapping != oldSourceMapping) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, MappingPackage.VARIABLE_SET_MAPPING__SOURCE_MAPPING, oldSourceMapping, sourceMapping));
			}
		}
		return sourceMapping;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MAPPING_DEFINITION basicGetSourceMapping() {
		return sourceMapping;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSourceMapping(MAPPING_DEFINITION newSourceMapping) {
		MAPPING_DEFINITION oldSourceMapping = sourceMapping;
		sourceMapping = newSourceMapping;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.VARIABLE_SET_MAPPING__SOURCE_MAPPING, oldSourceMapping, sourceMapping));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MAPPING_DEFINITION getTargetMapping() {
		if (targetMapping != null && targetMapping.eIsProxy()) {
			InternalEObject oldTargetMapping = (InternalEObject)targetMapping;
			targetMapping = (MAPPING_DEFINITION)eResolveProxy(oldTargetMapping);
			if (targetMapping != oldTargetMapping) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, MappingPackage.VARIABLE_SET_MAPPING__TARGET_MAPPING, oldTargetMapping, targetMapping));
			}
		}
		return targetMapping;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MAPPING_DEFINITION basicGetTargetMapping() {
		return targetMapping;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setTargetMapping(MAPPING_DEFINITION newTargetMapping) {
		MAPPING_DEFINITION oldTargetMapping = targetMapping;
		targetMapping = newTargetMapping;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.VARIABLE_SET_MAPPING__TARGET_MAPPING, oldTargetMapping, targetMapping));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case MappingPackage.VARIABLE_SET_MAPPING__SOURCE_MAPPING:
				if (resolve) return getSourceMapping();
				return basicGetSourceMapping();
			case MappingPackage.VARIABLE_SET_MAPPING__TARGET_MAPPING:
				if (resolve) return getTargetMapping();
				return basicGetTargetMapping();
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
			case MappingPackage.VARIABLE_SET_MAPPING__SOURCE_MAPPING:
				setSourceMapping((MAPPING_DEFINITION)newValue);
				return;
			case MappingPackage.VARIABLE_SET_MAPPING__TARGET_MAPPING:
				setTargetMapping((MAPPING_DEFINITION)newValue);
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
			case MappingPackage.VARIABLE_SET_MAPPING__SOURCE_MAPPING:
				setSourceMapping((MAPPING_DEFINITION)null);
				return;
			case MappingPackage.VARIABLE_SET_MAPPING__TARGET_MAPPING:
				setTargetMapping((MAPPING_DEFINITION)null);
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
			case MappingPackage.VARIABLE_SET_MAPPING__SOURCE_MAPPING:
				return sourceMapping != null;
			case MappingPackage.VARIABLE_SET_MAPPING__TARGET_MAPPING:
				return targetMapping != null;
		}
		return super.eIsSet(featureID);
	}

} //VARIABLE_SET_MAPPINGImpl
