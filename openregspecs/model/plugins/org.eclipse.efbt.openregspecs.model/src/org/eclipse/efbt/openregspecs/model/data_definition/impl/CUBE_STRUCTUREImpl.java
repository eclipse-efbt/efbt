/**
 */
package org.eclipse.efbt.openregspecs.model.data_definition.impl;

import java.util.Date;

import org.eclipse.efbt.openregspecs.model.core.MAINTENANCE_AGENCY;

import org.eclipse.efbt.openregspecs.model.data_definition.CUBE_STRUCTURE;
import org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>CUBE STRUCTURE</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_STRUCTUREImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_STRUCTUREImpl#getCube_structure_id <em>Cube structure id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_STRUCTUREImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_STRUCTUREImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_STRUCTUREImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_STRUCTUREImpl#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_STRUCTUREImpl#getValid_to <em>Valid to</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_STRUCTUREImpl#getVersion <em>Version</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_STRUCTUREImpl#getDisplayName <em>Display Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CUBE_STRUCTUREImpl extends MinimalEObjectImpl.Container implements CUBE_STRUCTURE {
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
	 * The default value of the '{@link #getCube_structure_id() <em>Cube structure id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_structure_id()
	 * @generated
	 * @ordered
	 */
	protected static final String CUBE_STRUCTURE_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getCube_structure_id() <em>Cube structure id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_structure_id()
	 * @generated
	 * @ordered
	 */
	protected String cube_structure_id = CUBE_STRUCTURE_ID_EDEFAULT;

	/**
	 * The default value of the '{@link #getDescription() <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDescription()
	 * @generated
	 * @ordered
	 */
	protected static final String DESCRIPTION_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getDescription() <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDescription()
	 * @generated
	 * @ordered
	 */
	protected String description = DESCRIPTION_EDEFAULT;

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
	 * The default value of the '{@link #getValid_from() <em>Valid from</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValid_from()
	 * @generated
	 * @ordered
	 */
	protected static final Date VALID_FROM_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getValid_from() <em>Valid from</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValid_from()
	 * @generated
	 * @ordered
	 */
	protected Date valid_from = VALID_FROM_EDEFAULT;

	/**
	 * The default value of the '{@link #getValid_to() <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValid_to()
	 * @generated
	 * @ordered
	 */
	protected static final Date VALID_TO_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getValid_to() <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValid_to()
	 * @generated
	 * @ordered
	 */
	protected Date valid_to = VALID_TO_EDEFAULT;

	/**
	 * The default value of the '{@link #getVersion() <em>Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVersion()
	 * @generated
	 * @ordered
	 */
	protected static final String VERSION_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getVersion() <em>Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVersion()
	 * @generated
	 * @ordered
	 */
	protected String version = VERSION_EDEFAULT;

	/**
	 * The default value of the '{@link #getDisplayName() <em>Display Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDisplayName()
	 * @generated
	 * @ordered
	 */
	protected static final String DISPLAY_NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getDisplayName() <em>Display Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDisplayName()
	 * @generated
	 * @ordered
	 */
	protected String displayName = DISPLAY_NAME_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CUBE_STRUCTUREImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_definitionPackage.Literals.CUBE_STRUCTURE;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getCube_structure_id() {
		return cube_structure_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCube_structure_id(String newCube_structure_id) {
		String oldCube_structure_id = cube_structure_id;
		cube_structure_id = newCube_structure_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE__CUBE_STRUCTURE_ID, oldCube_structure_id, cube_structure_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDescription(String newDescription) {
		String oldDescription = description;
		description = newDescription;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE__DESCRIPTION, oldDescription, description));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_STRUCTURE__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Date getValid_from() {
		return valid_from;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setValid_from(Date newValid_from) {
		Date oldValid_from = valid_from;
		valid_from = newValid_from;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE__VALID_FROM, oldValid_from, valid_from));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Date getValid_to() {
		return valid_to;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setValid_to(Date newValid_to) {
		Date oldValid_to = valid_to;
		valid_to = newValid_to;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE__VALID_TO, oldValid_to, valid_to));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getVersion() {
		return version;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVersion(String newVersion) {
		String oldVersion = version;
		version = newVersion;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE__VERSION, oldVersion, version));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getDisplayName() {
		return displayName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDisplayName(String newDisplayName) {
		String oldDisplayName = displayName;
		displayName = newDisplayName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE__DISPLAY_NAME, oldDisplayName, displayName));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_definitionPackage.CUBE_STRUCTURE__CODE:
				return getCode();
			case Data_definitionPackage.CUBE_STRUCTURE__CUBE_STRUCTURE_ID:
				return getCube_structure_id();
			case Data_definitionPackage.CUBE_STRUCTURE__DESCRIPTION:
				return getDescription();
			case Data_definitionPackage.CUBE_STRUCTURE__MAINTENANCE_AGENCY_ID:
				if (resolve) return getMaintenance_agency_id();
				return basicGetMaintenance_agency_id();
			case Data_definitionPackage.CUBE_STRUCTURE__NAME:
				return getName();
			case Data_definitionPackage.CUBE_STRUCTURE__VALID_FROM:
				return getValid_from();
			case Data_definitionPackage.CUBE_STRUCTURE__VALID_TO:
				return getValid_to();
			case Data_definitionPackage.CUBE_STRUCTURE__VERSION:
				return getVersion();
			case Data_definitionPackage.CUBE_STRUCTURE__DISPLAY_NAME:
				return getDisplayName();
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
			case Data_definitionPackage.CUBE_STRUCTURE__CODE:
				setCode((String)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__CUBE_STRUCTURE_ID:
				setCube_structure_id((String)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__NAME:
				setName((String)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__VALID_FROM:
				setValid_from((Date)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__VALID_TO:
				setValid_to((Date)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__VERSION:
				setVersion((String)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__DISPLAY_NAME:
				setDisplayName((String)newValue);
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
			case Data_definitionPackage.CUBE_STRUCTURE__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__CUBE_STRUCTURE_ID:
				setCube_structure_id(CUBE_STRUCTURE_ID_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)null);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__VALID_FROM:
				setValid_from(VALID_FROM_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__VALID_TO:
				setValid_to(VALID_TO_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__VERSION:
				setVersion(VERSION_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE__DISPLAY_NAME:
				setDisplayName(DISPLAY_NAME_EDEFAULT);
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
			case Data_definitionPackage.CUBE_STRUCTURE__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case Data_definitionPackage.CUBE_STRUCTURE__CUBE_STRUCTURE_ID:
				return CUBE_STRUCTURE_ID_EDEFAULT == null ? cube_structure_id != null : !CUBE_STRUCTURE_ID_EDEFAULT.equals(cube_structure_id);
			case Data_definitionPackage.CUBE_STRUCTURE__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case Data_definitionPackage.CUBE_STRUCTURE__MAINTENANCE_AGENCY_ID:
				return maintenance_agency_id != null;
			case Data_definitionPackage.CUBE_STRUCTURE__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Data_definitionPackage.CUBE_STRUCTURE__VALID_FROM:
				return VALID_FROM_EDEFAULT == null ? valid_from != null : !VALID_FROM_EDEFAULT.equals(valid_from);
			case Data_definitionPackage.CUBE_STRUCTURE__VALID_TO:
				return VALID_TO_EDEFAULT == null ? valid_to != null : !VALID_TO_EDEFAULT.equals(valid_to);
			case Data_definitionPackage.CUBE_STRUCTURE__VERSION:
				return VERSION_EDEFAULT == null ? version != null : !VERSION_EDEFAULT.equals(version);
			case Data_definitionPackage.CUBE_STRUCTURE__DISPLAY_NAME:
				return DISPLAY_NAME_EDEFAULT == null ? displayName != null : !DISPLAY_NAME_EDEFAULT.equals(displayName);
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
		result.append(", cube_structure_id: ");
		result.append(cube_structure_id);
		result.append(", description: ");
		result.append(description);
		result.append(", name: ");
		result.append(name);
		result.append(", valid_from: ");
		result.append(valid_from);
		result.append(", valid_to: ");
		result.append(valid_to);
		result.append(", version: ");
		result.append(version);
		result.append(", displayName: ");
		result.append(displayName);
		result.append(')');
		return result.toString();
	}

} //CUBE_STRUCTUREImpl
