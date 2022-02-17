/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BasicEntity;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Foreign Key Attribute</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ForeignKeyAttributeImpl#getEntity <em>Entity</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ForeignKeyAttributeImpl#isContainment <em>Containment</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ForeignKeyAttributeImpl#getCardinality <em>Cardinality</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ForeignKeyAttributeImpl#isMandatory <em>Mandatory</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ForeignKeyAttributeImpl extends AttributeImpl implements ForeignKeyAttribute {
	/**
	 * The cached value of the '{@link #getEntity() <em>Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEntity()
	 * @generated
	 * @ordered
	 */
	protected BasicEntity entity;

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
	 * The default value of the '{@link #getCardinality() <em>Cardinality</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCardinality()
	 * @generated
	 * @ordered
	 */
	protected static final int CARDINALITY_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getCardinality() <em>Cardinality</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCardinality()
	 * @generated
	 * @ordered
	 */
	protected int cardinality = CARDINALITY_EDEFAULT;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ForeignKeyAttributeImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_meta_modelPackage.Literals.FOREIGN_KEY_ATTRIBUTE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BasicEntity getEntity() {
		if (entity != null && entity.eIsProxy()) {
			InternalEObject oldEntity = (InternalEObject)entity;
			entity = (BasicEntity)eResolveProxy(oldEntity);
			if (entity != oldEntity) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__ENTITY, oldEntity, entity));
			}
		}
		return entity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicEntity basicGetEntity() {
		return entity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setEntity(BasicEntity newEntity) {
		BasicEntity oldEntity = entity;
		entity = newEntity;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__ENTITY, oldEntity, entity));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isContainment() {
		return containment;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setContainment(boolean newContainment) {
		boolean oldContainment = containment;
		containment = newContainment;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__CONTAINMENT, oldContainment, containment));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public int getCardinality() {
		return cardinality;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCardinality(int newCardinality) {
		int oldCardinality = cardinality;
		cardinality = newCardinality;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__CARDINALITY, oldCardinality, cardinality));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isMandatory() {
		return mandatory;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMandatory(boolean newMandatory) {
		boolean oldMandatory = mandatory;
		mandatory = newMandatory;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__MANDATORY, oldMandatory, mandatory));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__ENTITY:
				if (resolve) return getEntity();
				return basicGetEntity();
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__CONTAINMENT:
				return isContainment();
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__CARDINALITY:
				return getCardinality();
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__MANDATORY:
				return isMandatory();
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
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__ENTITY:
				setEntity((BasicEntity)newValue);
				return;
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__CONTAINMENT:
				setContainment((Boolean)newValue);
				return;
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__CARDINALITY:
				setCardinality((Integer)newValue);
				return;
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__MANDATORY:
				setMandatory((Boolean)newValue);
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
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__ENTITY:
				setEntity((BasicEntity)null);
				return;
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__CONTAINMENT:
				setContainment(CONTAINMENT_EDEFAULT);
				return;
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__CARDINALITY:
				setCardinality(CARDINALITY_EDEFAULT);
				return;
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__MANDATORY:
				setMandatory(MANDATORY_EDEFAULT);
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
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__ENTITY:
				return entity != null;
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__CONTAINMENT:
				return containment != CONTAINMENT_EDEFAULT;
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__CARDINALITY:
				return cardinality != CARDINALITY_EDEFAULT;
			case Data_meta_modelPackage.FOREIGN_KEY_ATTRIBUTE__MANDATORY:
				return mandatory != MANDATORY_EDEFAULT;
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
		result.append(", cardinality: ");
		result.append(cardinality);
		result.append(", mandatory: ");
		result.append(mandatory);
		result.append(')');
		return result.toString();
	}

} //ForeignKeyAttributeImpl
