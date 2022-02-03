/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.EParameterImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Attribute From Entity Parameter</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AttributeFromEntityParameterImpl#getAttributeInEntity <em>Attribute In Entity</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AttributeFromEntityParameterImpl#getEntityFeature <em>Entity Feature</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AttributeFromEntityParameterImpl extends EParameterImpl implements AttributeFromEntityParameter {
	/**
	 * The cached value of the '{@link #getAttributeInEntity() <em>Attribute In Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAttributeInEntity()
	 * @generated
	 * @ordered
	 */
	protected EAttribute attributeInEntity;

	/**
	 * The cached value of the '{@link #getEntityFeature() <em>Entity Feature</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEntityFeature()
	 * @generated
	 * @ordered
	 */
	protected EStructuralFeature entityFeature;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AttributeFromEntityParameterImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.ATTRIBUTE_FROM_ENTITY_PARAMETER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributeInEntity() {
		if (attributeInEntity != null && attributeInEntity.eIsProxy()) {
			InternalEObject oldAttributeInEntity = (InternalEObject)attributeInEntity;
			attributeInEntity = (EAttribute)eResolveProxy(oldAttributeInEntity);
			if (attributeInEntity != oldAttributeInEntity) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER__ATTRIBUTE_IN_ENTITY, oldAttributeInEntity, attributeInEntity));
			}
		}
		return attributeInEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute basicGetAttributeInEntity() {
		return attributeInEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setAttributeInEntity(EAttribute newAttributeInEntity) {
		EAttribute oldAttributeInEntity = attributeInEntity;
		attributeInEntity = newAttributeInEntity;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER__ATTRIBUTE_IN_ENTITY, oldAttributeInEntity, attributeInEntity));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EStructuralFeature getEntityFeature() {
		if (entityFeature != null && entityFeature.eIsProxy()) {
			InternalEObject oldEntityFeature = (InternalEObject)entityFeature;
			entityFeature = (EStructuralFeature)eResolveProxy(oldEntityFeature);
			if (entityFeature != oldEntityFeature) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER__ENTITY_FEATURE, oldEntityFeature, entityFeature));
			}
		}
		return entityFeature;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EStructuralFeature basicGetEntityFeature() {
		return entityFeature;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setEntityFeature(EStructuralFeature newEntityFeature) {
		EStructuralFeature oldEntityFeature = entityFeature;
		entityFeature = newEntityFeature;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER__ENTITY_FEATURE, oldEntityFeature, entityFeature));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER__ATTRIBUTE_IN_ENTITY:
				if (resolve) return getAttributeInEntity();
				return basicGetAttributeInEntity();
			case Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER__ENTITY_FEATURE:
				if (resolve) return getEntityFeature();
				return basicGetEntityFeature();
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
			case Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER__ATTRIBUTE_IN_ENTITY:
				setAttributeInEntity((EAttribute)newValue);
				return;
			case Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER__ENTITY_FEATURE:
				setEntityFeature((EStructuralFeature)newValue);
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
			case Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER__ATTRIBUTE_IN_ENTITY:
				setAttributeInEntity((EAttribute)null);
				return;
			case Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER__ENTITY_FEATURE:
				setEntityFeature((EStructuralFeature)null);
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
			case Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER__ATTRIBUTE_IN_ENTITY:
				return attributeInEntity != null;
			case Ecore_plus_logicPackage.ATTRIBUTE_FROM_ENTITY_PARAMETER__ENTITY_FEATURE:
				return entityFeature != null;
		}
		return super.eIsSet(featureID);
	}

} //AttributeFromEntityParameterImpl
