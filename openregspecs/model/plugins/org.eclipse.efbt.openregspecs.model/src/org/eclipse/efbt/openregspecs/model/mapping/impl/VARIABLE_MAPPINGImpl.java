/**
 */
package org.eclipse.efbt.openregspecs.model.mapping.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.mapping.MappingPackage;
import org.eclipse.efbt.openregspecs.model.mapping.VARIABLE_MAPPING;
import org.eclipse.efbt.openregspecs.model.mapping.VARIABLE_MAPPING_ITEM;

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
 * An implementation of the model object '<em><b>VARIABLE MAPPING</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.VARIABLE_MAPPINGImpl#getVaraibleMappingItems <em>Varaible Mapping Items</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.VARIABLE_MAPPINGImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.VARIABLE_MAPPINGImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.VARIABLE_MAPPINGImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.mapping.impl.VARIABLE_MAPPINGImpl#getVariable_mapping_id <em>Variable mapping id</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VARIABLE_MAPPINGImpl extends MinimalEObjectImpl.Container implements VARIABLE_MAPPING {
	/**
	 * The cached value of the '{@link #getVaraibleMappingItems() <em>Varaible Mapping Items</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVaraibleMappingItems()
	 * @generated
	 * @ordered
	 */
	protected EList<VARIABLE_MAPPING_ITEM> varaibleMappingItems;

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
	 * The default value of the '{@link #getVariable_mapping_id() <em>Variable mapping id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable_mapping_id()
	 * @generated
	 * @ordered
	 */
	protected static final String VARIABLE_MAPPING_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getVariable_mapping_id() <em>Variable mapping id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable_mapping_id()
	 * @generated
	 * @ordered
	 */
	protected String variable_mapping_id = VARIABLE_MAPPING_ID_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VARIABLE_MAPPINGImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return MappingPackage.Literals.VARIABLE_MAPPING;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<VARIABLE_MAPPING_ITEM> getVaraibleMappingItems() {
		if (varaibleMappingItems == null) {
			varaibleMappingItems = new EObjectContainmentEList<VARIABLE_MAPPING_ITEM>(VARIABLE_MAPPING_ITEM.class, this, MappingPackage.VARIABLE_MAPPING__VARAIBLE_MAPPING_ITEMS);
		}
		return varaibleMappingItems;
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
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.VARIABLE_MAPPING__NAME, oldName, name));
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
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.VARIABLE_MAPPING__CODE, oldCode, code));
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
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.VARIABLE_MAPPING__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getVariable_mapping_id() {
		return variable_mapping_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVariable_mapping_id(String newVariable_mapping_id) {
		String oldVariable_mapping_id = variable_mapping_id;
		variable_mapping_id = newVariable_mapping_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.VARIABLE_MAPPING__VARIABLE_MAPPING_ID, oldVariable_mapping_id, variable_mapping_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case MappingPackage.VARIABLE_MAPPING__VARAIBLE_MAPPING_ITEMS:
				return ((InternalEList<?>)getVaraibleMappingItems()).basicRemove(otherEnd, msgs);
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
			case MappingPackage.VARIABLE_MAPPING__VARAIBLE_MAPPING_ITEMS:
				return getVaraibleMappingItems();
			case MappingPackage.VARIABLE_MAPPING__NAME:
				return getName();
			case MappingPackage.VARIABLE_MAPPING__CODE:
				return getCode();
			case MappingPackage.VARIABLE_MAPPING__MAINTENANCE_AGENCY_ID:
				return getMaintenance_agency_id();
			case MappingPackage.VARIABLE_MAPPING__VARIABLE_MAPPING_ID:
				return getVariable_mapping_id();
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
			case MappingPackage.VARIABLE_MAPPING__VARAIBLE_MAPPING_ITEMS:
				getVaraibleMappingItems().clear();
				getVaraibleMappingItems().addAll((Collection<? extends VARIABLE_MAPPING_ITEM>)newValue);
				return;
			case MappingPackage.VARIABLE_MAPPING__NAME:
				setName((String)newValue);
				return;
			case MappingPackage.VARIABLE_MAPPING__CODE:
				setCode((String)newValue);
				return;
			case MappingPackage.VARIABLE_MAPPING__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((String)newValue);
				return;
			case MappingPackage.VARIABLE_MAPPING__VARIABLE_MAPPING_ID:
				setVariable_mapping_id((String)newValue);
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
			case MappingPackage.VARIABLE_MAPPING__VARAIBLE_MAPPING_ITEMS:
				getVaraibleMappingItems().clear();
				return;
			case MappingPackage.VARIABLE_MAPPING__NAME:
				setName(NAME_EDEFAULT);
				return;
			case MappingPackage.VARIABLE_MAPPING__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case MappingPackage.VARIABLE_MAPPING__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id(MAINTENANCE_AGENCY_ID_EDEFAULT);
				return;
			case MappingPackage.VARIABLE_MAPPING__VARIABLE_MAPPING_ID:
				setVariable_mapping_id(VARIABLE_MAPPING_ID_EDEFAULT);
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
			case MappingPackage.VARIABLE_MAPPING__VARAIBLE_MAPPING_ITEMS:
				return varaibleMappingItems != null && !varaibleMappingItems.isEmpty();
			case MappingPackage.VARIABLE_MAPPING__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case MappingPackage.VARIABLE_MAPPING__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case MappingPackage.VARIABLE_MAPPING__MAINTENANCE_AGENCY_ID:
				return MAINTENANCE_AGENCY_ID_EDEFAULT == null ? maintenance_agency_id != null : !MAINTENANCE_AGENCY_ID_EDEFAULT.equals(maintenance_agency_id);
			case MappingPackage.VARIABLE_MAPPING__VARIABLE_MAPPING_ID:
				return VARIABLE_MAPPING_ID_EDEFAULT == null ? variable_mapping_id != null : !VARIABLE_MAPPING_ID_EDEFAULT.equals(variable_mapping_id);
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
		result.append(", variable_mapping_id: ");
		result.append(variable_mapping_id);
		result.append(')');
		return result.toString();
	}

} //VARIABLE_MAPPINGImpl
