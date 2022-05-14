/**
 */
package org.eclipse.efbt.openregspecs.model.data_definition.impl;

import org.eclipse.efbt.openregspecs.model.core.MAINTENANCE_AGENCY;

import org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY;
import org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage;
import org.eclipse.efbt.openregspecs.model.data_definition.FRAMEWORK;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>CUBE HIERARCHY</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHYImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHYImpl#getCube_hierarchy_id <em>Cube hierarchy id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHYImpl#getCube_hierarchy_type <em>Cube hierarchy type</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHYImpl#getFramework_id <em>Framework id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHYImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHYImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CUBE_HIERARCHYImpl extends MinimalEObjectImpl.Container implements CUBE_HIERARCHY {
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
	 * The default value of the '{@link #getCube_hierarchy_id() <em>Cube hierarchy id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_hierarchy_id()
	 * @generated
	 * @ordered
	 */
	protected static final String CUBE_HIERARCHY_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getCube_hierarchy_id() <em>Cube hierarchy id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_hierarchy_id()
	 * @generated
	 * @ordered
	 */
	protected String cube_hierarchy_id = CUBE_HIERARCHY_ID_EDEFAULT;

	/**
	 * The default value of the '{@link #getCube_hierarchy_type() <em>Cube hierarchy type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_hierarchy_type()
	 * @generated
	 * @ordered
	 */
	protected static final String CUBE_HIERARCHY_TYPE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getCube_hierarchy_type() <em>Cube hierarchy type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_hierarchy_type()
	 * @generated
	 * @ordered
	 */
	protected String cube_hierarchy_type = CUBE_HIERARCHY_TYPE_EDEFAULT;

	/**
	 * The cached value of the '{@link #getFramework_id() <em>Framework id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFramework_id()
	 * @generated
	 * @ordered
	 */
	protected FRAMEWORK framework_id;

	/**
	 * The cached value of the '{@link #getMaintenance_agency_id() <em>Maintenance agency id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMaintenance_agency_id()
	 * @generated
	 * @ordered
	 */
	protected MAINTENANCE_AGENCY maintenance_agency_id;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CUBE_HIERARCHYImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_definitionPackage.Literals.CUBE_HIERARCHY;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getCube_hierarchy_id() {
		return cube_hierarchy_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCube_hierarchy_id(String newCube_hierarchy_id) {
		String oldCube_hierarchy_id = cube_hierarchy_id;
		cube_hierarchy_id = newCube_hierarchy_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY__CUBE_HIERARCHY_ID, oldCube_hierarchy_id, cube_hierarchy_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getCube_hierarchy_type() {
		return cube_hierarchy_type;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCube_hierarchy_type(String newCube_hierarchy_type) {
		String oldCube_hierarchy_type = cube_hierarchy_type;
		cube_hierarchy_type = newCube_hierarchy_type;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY__CUBE_HIERARCHY_TYPE, oldCube_hierarchy_type, cube_hierarchy_type));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FRAMEWORK getFramework_id() {
		if (framework_id != null && framework_id.eIsProxy()) {
			InternalEObject oldFramework_id = (InternalEObject)framework_id;
			framework_id = (FRAMEWORK)eResolveProxy(oldFramework_id);
			if (framework_id != oldFramework_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_HIERARCHY__FRAMEWORK_ID, oldFramework_id, framework_id));
			}
		}
		return framework_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FRAMEWORK basicGetFramework_id() {
		return framework_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setFramework_id(FRAMEWORK newFramework_id) {
		FRAMEWORK oldFramework_id = framework_id;
		framework_id = newFramework_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY__FRAMEWORK_ID, oldFramework_id, framework_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MAINTENANCE_AGENCY getMaintenance_agency_id() {
		if (maintenance_agency_id != null && maintenance_agency_id.eIsProxy()) {
			InternalEObject oldMaintenance_agency_id = (InternalEObject)maintenance_agency_id;
			maintenance_agency_id = (MAINTENANCE_AGENCY)eResolveProxy(oldMaintenance_agency_id);
			if (maintenance_agency_id != oldMaintenance_agency_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_HIERARCHY__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
			}
		}
		return maintenance_agency_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MAINTENANCE_AGENCY basicGetMaintenance_agency_id() {
		return maintenance_agency_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setMaintenance_agency_id(MAINTENANCE_AGENCY newMaintenance_agency_id) {
		MAINTENANCE_AGENCY oldMaintenance_agency_id = maintenance_agency_id;
		maintenance_agency_id = newMaintenance_agency_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_definitionPackage.CUBE_HIERARCHY__CODE:
				return getCode();
			case Data_definitionPackage.CUBE_HIERARCHY__CUBE_HIERARCHY_ID:
				return getCube_hierarchy_id();
			case Data_definitionPackage.CUBE_HIERARCHY__CUBE_HIERARCHY_TYPE:
				return getCube_hierarchy_type();
			case Data_definitionPackage.CUBE_HIERARCHY__FRAMEWORK_ID:
				if (resolve) return getFramework_id();
				return basicGetFramework_id();
			case Data_definitionPackage.CUBE_HIERARCHY__MAINTENANCE_AGENCY_ID:
				if (resolve) return getMaintenance_agency_id();
				return basicGetMaintenance_agency_id();
			case Data_definitionPackage.CUBE_HIERARCHY__NAME:
				return getName();
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
			case Data_definitionPackage.CUBE_HIERARCHY__CODE:
				setCode((String)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY__CUBE_HIERARCHY_ID:
				setCube_hierarchy_id((String)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY__CUBE_HIERARCHY_TYPE:
				setCube_hierarchy_type((String)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY__FRAMEWORK_ID:
				setFramework_id((FRAMEWORK)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY__NAME:
				setName((String)newValue);
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
			case Data_definitionPackage.CUBE_HIERARCHY__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY__CUBE_HIERARCHY_ID:
				setCube_hierarchy_id(CUBE_HIERARCHY_ID_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY__CUBE_HIERARCHY_TYPE:
				setCube_hierarchy_type(CUBE_HIERARCHY_TYPE_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY__FRAMEWORK_ID:
				setFramework_id((FRAMEWORK)null);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)null);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY__NAME:
				setName(NAME_EDEFAULT);
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
			case Data_definitionPackage.CUBE_HIERARCHY__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case Data_definitionPackage.CUBE_HIERARCHY__CUBE_HIERARCHY_ID:
				return CUBE_HIERARCHY_ID_EDEFAULT == null ? cube_hierarchy_id != null : !CUBE_HIERARCHY_ID_EDEFAULT.equals(cube_hierarchy_id);
			case Data_definitionPackage.CUBE_HIERARCHY__CUBE_HIERARCHY_TYPE:
				return CUBE_HIERARCHY_TYPE_EDEFAULT == null ? cube_hierarchy_type != null : !CUBE_HIERARCHY_TYPE_EDEFAULT.equals(cube_hierarchy_type);
			case Data_definitionPackage.CUBE_HIERARCHY__FRAMEWORK_ID:
				return framework_id != null;
			case Data_definitionPackage.CUBE_HIERARCHY__MAINTENANCE_AGENCY_ID:
				return maintenance_agency_id != null;
			case Data_definitionPackage.CUBE_HIERARCHY__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
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
		result.append(" (code: ");
		result.append(code);
		result.append(", cube_hierarchy_id: ");
		result.append(cube_hierarchy_id);
		result.append(", cube_hierarchy_type: ");
		result.append(cube_hierarchy_type);
		result.append(", name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //CUBE_HIERARCHYImpl
