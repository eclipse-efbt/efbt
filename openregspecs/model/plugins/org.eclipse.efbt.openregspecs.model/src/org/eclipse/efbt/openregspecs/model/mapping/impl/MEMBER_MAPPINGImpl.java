/**
 */
package org.eclipse.efbt.openregspecs.model.mapping.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.mapping.MEMBER_MAPPING;
import org.eclipse.efbt.openregspecs.model.mapping.MEMBER_MAPPING_ITEM;
import org.eclipse.efbt.openregspecs.model.mapping.MappingPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>MEMBER MAPPING</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.MEMBER_MAPPINGImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.MEMBER_MAPPINGImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.MEMBER_MAPPINGImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.MEMBER_MAPPINGImpl#getMember_mapping_id <em>Member mapping id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.MEMBER_MAPPINGImpl#getMemberMappingItems <em>Member Mapping Items</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MEMBER_MAPPINGImpl extends MinimalEObjectImpl.Container implements MEMBER_MAPPING {
	/**
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * The default value of the '{@link #getCode() <em>Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCode()
	 * @generated
	 * @ordered
	 */
	protected static final String CODE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getCode() <em>Code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCode()
	 * @generated
	 * @ordered
	 */
	protected String code = CODE_EDEFAULT;

	/**
	 * The default value of the '{@link #getMaintenance_agency_id() <em>Maintenance agency id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMaintenance_agency_id()
	 * @generated
	 * @ordered
	 */
	protected static final String MAINTENANCE_AGENCY_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getMaintenance_agency_id() <em>Maintenance agency id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMaintenance_agency_id()
	 * @generated
	 * @ordered
	 */
	protected String maintenance_agency_id = MAINTENANCE_AGENCY_ID_EDEFAULT;

	/**
	 * The default value of the '{@link #getMember_mapping_id() <em>Member mapping id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMember_mapping_id()
	 * @generated
	 * @ordered
	 */
	protected static final String MEMBER_MAPPING_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getMember_mapping_id() <em>Member mapping id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMember_mapping_id()
	 * @generated
	 * @ordered
	 */
	protected String member_mapping_id = MEMBER_MAPPING_ID_EDEFAULT;

	/**
	 * The cached value of the '{@link #getMemberMappingItems() <em>Member Mapping Items</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMemberMappingItems()
	 * @generated
	 * @ordered
	 */
	protected EList<MEMBER_MAPPING_ITEM> memberMappingItems;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected MEMBER_MAPPINGImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return MappingPackage.Literals.MEMBER_MAPPING;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MEMBER_MAPPING__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getCode() {
		return code;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCode(String newCode) {
		String oldCode = code;
		code = newCode;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MEMBER_MAPPING__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getMaintenance_agency_id() {
		return maintenance_agency_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setMaintenance_agency_id(String newMaintenance_agency_id) {
		String oldMaintenance_agency_id = maintenance_agency_id;
		maintenance_agency_id = newMaintenance_agency_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MEMBER_MAPPING__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getMember_mapping_id() {
		return member_mapping_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setMember_mapping_id(String newMember_mapping_id) {
		String oldMember_mapping_id = member_mapping_id;
		member_mapping_id = newMember_mapping_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MEMBER_MAPPING__MEMBER_MAPPING_ID, oldMember_mapping_id, member_mapping_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<MEMBER_MAPPING_ITEM> getMemberMappingItems() {
		if (memberMappingItems == null) {
			memberMappingItems = new EObjectContainmentEList<MEMBER_MAPPING_ITEM>(MEMBER_MAPPING_ITEM.class, this, MappingPackage.MEMBER_MAPPING__MEMBER_MAPPING_ITEMS);
		}
		return memberMappingItems;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case MappingPackage.MEMBER_MAPPING__MEMBER_MAPPING_ITEMS:
				return ((InternalEList<?>)getMemberMappingItems()).basicRemove(otherEnd, msgs);
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
			case MappingPackage.MEMBER_MAPPING__NAME:
				return getName();
			case MappingPackage.MEMBER_MAPPING__CODE:
				return getCode();
			case MappingPackage.MEMBER_MAPPING__MAINTENANCE_AGENCY_ID:
				return getMaintenance_agency_id();
			case MappingPackage.MEMBER_MAPPING__MEMBER_MAPPING_ID:
				return getMember_mapping_id();
			case MappingPackage.MEMBER_MAPPING__MEMBER_MAPPING_ITEMS:
				return getMemberMappingItems();
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
			case MappingPackage.MEMBER_MAPPING__NAME:
				setName((String)newValue);
				return;
			case MappingPackage.MEMBER_MAPPING__CODE:
				setCode((String)newValue);
				return;
			case MappingPackage.MEMBER_MAPPING__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((String)newValue);
				return;
			case MappingPackage.MEMBER_MAPPING__MEMBER_MAPPING_ID:
				setMember_mapping_id((String)newValue);
				return;
			case MappingPackage.MEMBER_MAPPING__MEMBER_MAPPING_ITEMS:
				getMemberMappingItems().clear();
				getMemberMappingItems().addAll((Collection<? extends MEMBER_MAPPING_ITEM>)newValue);
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
			case MappingPackage.MEMBER_MAPPING__NAME:
				setName(NAME_EDEFAULT);
				return;
			case MappingPackage.MEMBER_MAPPING__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case MappingPackage.MEMBER_MAPPING__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id(MAINTENANCE_AGENCY_ID_EDEFAULT);
				return;
			case MappingPackage.MEMBER_MAPPING__MEMBER_MAPPING_ID:
				setMember_mapping_id(MEMBER_MAPPING_ID_EDEFAULT);
				return;
			case MappingPackage.MEMBER_MAPPING__MEMBER_MAPPING_ITEMS:
				getMemberMappingItems().clear();
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
			case MappingPackage.MEMBER_MAPPING__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case MappingPackage.MEMBER_MAPPING__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case MappingPackage.MEMBER_MAPPING__MAINTENANCE_AGENCY_ID:
				return MAINTENANCE_AGENCY_ID_EDEFAULT == null ? maintenance_agency_id != null : !MAINTENANCE_AGENCY_ID_EDEFAULT.equals(maintenance_agency_id);
			case MappingPackage.MEMBER_MAPPING__MEMBER_MAPPING_ID:
				return MEMBER_MAPPING_ID_EDEFAULT == null ? member_mapping_id != null : !MEMBER_MAPPING_ID_EDEFAULT.equals(member_mapping_id);
			case MappingPackage.MEMBER_MAPPING__MEMBER_MAPPING_ITEMS:
				return memberMappingItems != null && !memberMappingItems.isEmpty();
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
		result.append(" (name: ");
		result.append(name);
		result.append(", code: ");
		result.append(code);
		result.append(", maintenance_agency_id: ");
		result.append(maintenance_agency_id);
		result.append(", member_mapping_id: ");
		result.append(member_mapping_id);
		result.append(')');
		return result.toString();
	}

} //MEMBER_MAPPINGImpl
