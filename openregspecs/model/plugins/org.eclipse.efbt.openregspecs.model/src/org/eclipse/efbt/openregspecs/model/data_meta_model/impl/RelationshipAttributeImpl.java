/**
 */
package org.eclipse.efbt.openregspecs.model.data_meta_model.impl;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelPackage;
import org.eclipse.efbt.openregspecs.model.data_meta_model.Entity;
import org.eclipse.efbt.openregspecs.model.data_meta_model.RelationshipAttribute;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Relationship Attribute</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.RelationshipAttributeImpl#getEntity <em>Entity</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.RelationshipAttributeImpl#isContainment <em>Containment</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.RelationshipAttributeImpl#isMandatory <em>Mandatory</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.RelationshipAttributeImpl#isDominant <em>Dominant</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RelationshipAttributeImpl extends AttributeImpl implements RelationshipAttribute {
	/**
	 * The cached value of the '{@link #getEntity() <em>Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEntity()
	 * @generated
	 * @ordered
	 */
	protected Entity entity;

	/**
	 * The default value of the '{@link #isContainment() <em>Containment</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isContainment()
	 * @generated
	 * @ordered
	 */
	protected static final boolean CONTAINMENT_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isContainment() <em>Containment</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isContainment()
	 * @generated
	 * @ordered
	 */
	protected boolean containment = CONTAINMENT_EDEFAULT;

	/**
	 * The default value of the '{@link #isMandatory() <em>Mandatory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isMandatory()
	 * @generated
	 * @ordered
	 */
	protected static final boolean MANDATORY_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isMandatory() <em>Mandatory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isMandatory()
	 * @generated
	 * @ordered
	 */
	protected boolean mandatory = MANDATORY_EDEFAULT;

	/**
	 * The default value of the '{@link #isDominant() <em>Dominant</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isDominant()
	 * @generated
	 * @ordered
	 */
	protected static final boolean DOMINANT_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isDominant() <em>Dominant</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isDominant()
	 * @generated
	 * @ordered
	 */
	protected boolean dominant = DOMINANT_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RelationshipAttributeImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_meta_modelPackage.Literals.RELATIONSHIP_ATTRIBUTE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Entity getEntity() {
		if (entity != null && entity.eIsProxy()) {
			InternalEObject oldEntity = (InternalEObject)entity;
			entity = (Entity)eResolveProxy(oldEntity);
			if (entity != oldEntity) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__ENTITY, oldEntity, entity));
			}
		}
		return entity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Entity basicGetEntity() {
		return entity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setEntity(Entity newEntity) {
		Entity oldEntity = entity;
		entity = newEntity;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__ENTITY, oldEntity, entity));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isContainment() {
		return containment;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setContainment(boolean newContainment) {
		boolean oldContainment = containment;
		containment = newContainment;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__CONTAINMENT, oldContainment, containment));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isMandatory() {
		return mandatory;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setMandatory(boolean newMandatory) {
		boolean oldMandatory = mandatory;
		mandatory = newMandatory;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__MANDATORY, oldMandatory, mandatory));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isDominant() {
		return dominant;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDominant(boolean newDominant) {
		boolean oldDominant = dominant;
		dominant = newDominant;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__DOMINANT, oldDominant, dominant));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__ENTITY:
				if (resolve) return getEntity();
				return basicGetEntity();
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__CONTAINMENT:
				return isContainment();
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__MANDATORY:
				return isMandatory();
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__DOMINANT:
				return isDominant();
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
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__ENTITY:
				setEntity((Entity)newValue);
				return;
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__CONTAINMENT:
				setContainment((Boolean)newValue);
				return;
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__MANDATORY:
				setMandatory((Boolean)newValue);
				return;
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__DOMINANT:
				setDominant((Boolean)newValue);
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
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__ENTITY:
				setEntity((Entity)null);
				return;
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__CONTAINMENT:
				setContainment(CONTAINMENT_EDEFAULT);
				return;
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__MANDATORY:
				setMandatory(MANDATORY_EDEFAULT);
				return;
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__DOMINANT:
				setDominant(DOMINANT_EDEFAULT);
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
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__ENTITY:
				return entity != null;
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__CONTAINMENT:
				return containment != CONTAINMENT_EDEFAULT;
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__MANDATORY:
				return mandatory != MANDATORY_EDEFAULT;
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE__DOMINANT:
				return dominant != DOMINANT_EDEFAULT;
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
		result.append(" (containment: ");
		result.append(containment);
		result.append(", mandatory: ");
		result.append(mandatory);
		result.append(", dominant: ");
		result.append(dominant);
		result.append(')');
		return result.toString();
	}

} //RelationshipAttributeImpl
