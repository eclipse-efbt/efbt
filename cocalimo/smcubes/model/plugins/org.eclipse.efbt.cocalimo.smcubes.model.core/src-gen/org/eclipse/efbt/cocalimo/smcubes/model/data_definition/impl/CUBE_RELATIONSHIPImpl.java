/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl;

import java.util.Date;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY;
import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_RELATIONSHIP;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>CUBE RELATIONSHIP</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#getCube_relationship_id <em>Cube relationship id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#isEstablishes_integrity <em>Establishes integrity</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#getForeign_cube_id <em>Foreign cube id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#getForeign_cube_variable_code <em>Foreign cube variable code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#getPrimary_cube_id <em>Primary cube id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#getPrimary_cube_variable_id <em>Primary cube variable id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#getValid_to <em>Valid to</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_RELATIONSHIPImpl#getVersion <em>Version</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CUBE_RELATIONSHIPImpl extends MinimalEObjectImpl.Container implements CUBE_RELATIONSHIP {
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
	 * The default value of the '{@link #getCube_relationship_id() <em>Cube relationship id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_relationship_id()
	 * @generated
	 * @ordered
	 */
	protected static final String CUBE_RELATIONSHIP_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getCube_relationship_id() <em>Cube relationship id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_relationship_id()
	 * @generated
	 * @ordered
	 */
	protected String cube_relationship_id = CUBE_RELATIONSHIP_ID_EDEFAULT;

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
	 * The default value of the '{@link #isEstablishes_integrity() <em>Establishes integrity</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isEstablishes_integrity()
	 * @generated
	 * @ordered
	 */
	protected static final boolean ESTABLISHES_INTEGRITY_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isEstablishes_integrity() <em>Establishes integrity</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isEstablishes_integrity()
	 * @generated
	 * @ordered
	 */
	protected boolean establishes_integrity = ESTABLISHES_INTEGRITY_EDEFAULT;

	/**
	 * The cached value of the '{@link #getForeign_cube_id() <em>Foreign cube id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getForeign_cube_id()
	 * @generated
	 * @ordered
	 */
	protected CUBE foreign_cube_id;

	/**
	 * The cached value of the '{@link #getForeign_cube_variable_code() <em>Foreign cube variable code</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getForeign_cube_variable_code()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE foreign_cube_variable_code;

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
	 * The cached value of the '{@link #getPrimary_cube_id() <em>Primary cube id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getPrimary_cube_id()
	 * @generated
	 * @ordered
	 */
	protected CUBE primary_cube_id;

	/**
	 * The cached value of the '{@link #getPrimary_cube_variable_id() <em>Primary cube variable id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getPrimary_cube_variable_id()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE primary_cube_variable_id;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CUBE_RELATIONSHIPImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_definitionPackage.Literals.CUBE_RELATIONSHIP;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getCode() {
		return code;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCode(String newCode) {
		String oldCode = code;
		code = newCode;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getCube_relationship_id() {
		return cube_relationship_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube_relationship_id(String newCube_relationship_id) {
		String oldCube_relationship_id = cube_relationship_id;
		cube_relationship_id = newCube_relationship_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__CUBE_RELATIONSHIP_ID, oldCube_relationship_id, cube_relationship_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getDescription() {
		return description;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setDescription(String newDescription) {
		String oldDescription = description;
		description = newDescription;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isEstablishes_integrity() {
		return establishes_integrity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setEstablishes_integrity(boolean newEstablishes_integrity) {
		boolean oldEstablishes_integrity = establishes_integrity;
		establishes_integrity = newEstablishes_integrity;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__ESTABLISHES_INTEGRITY, oldEstablishes_integrity, establishes_integrity));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE getForeign_cube_id() {
		if (foreign_cube_id != null && foreign_cube_id.eIsProxy()) {
			InternalEObject oldForeign_cube_id = (InternalEObject)foreign_cube_id;
			foreign_cube_id = (CUBE)eResolveProxy(oldForeign_cube_id);
			if (foreign_cube_id != oldForeign_cube_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_RELATIONSHIP__FOREIGN_CUBE_ID, oldForeign_cube_id, foreign_cube_id));
			}
		}
		return foreign_cube_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE basicGetForeign_cube_id() {
		return foreign_cube_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setForeign_cube_id(CUBE newForeign_cube_id) {
		CUBE oldForeign_cube_id = foreign_cube_id;
		foreign_cube_id = newForeign_cube_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__FOREIGN_CUBE_ID, oldForeign_cube_id, foreign_cube_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getForeign_cube_variable_code() {
		if (foreign_cube_variable_code != null && foreign_cube_variable_code.eIsProxy()) {
			InternalEObject oldForeign_cube_variable_code = (InternalEObject)foreign_cube_variable_code;
			foreign_cube_variable_code = (VARIABLE)eResolveProxy(oldForeign_cube_variable_code);
			if (foreign_cube_variable_code != oldForeign_cube_variable_code) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_RELATIONSHIP__FOREIGN_CUBE_VARIABLE_CODE, oldForeign_cube_variable_code, foreign_cube_variable_code));
			}
		}
		return foreign_cube_variable_code;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetForeign_cube_variable_code() {
		return foreign_cube_variable_code;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setForeign_cube_variable_code(VARIABLE newForeign_cube_variable_code) {
		VARIABLE oldForeign_cube_variable_code = foreign_cube_variable_code;
		foreign_cube_variable_code = newForeign_cube_variable_code;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__FOREIGN_CUBE_VARIABLE_CODE, oldForeign_cube_variable_code, foreign_cube_variable_code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MAINTENANCE_AGENCY getMaintenance_agency_id() {
		if (maintenance_agency_id != null && maintenance_agency_id.eIsProxy()) {
			InternalEObject oldMaintenance_agency_id = (InternalEObject)maintenance_agency_id;
			maintenance_agency_id = (MAINTENANCE_AGENCY)eResolveProxy(oldMaintenance_agency_id);
			if (maintenance_agency_id != oldMaintenance_agency_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_RELATIONSHIP__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
	@Override
	public void setMaintenance_agency_id(MAINTENANCE_AGENCY newMaintenance_agency_id) {
		MAINTENANCE_AGENCY oldMaintenance_agency_id = maintenance_agency_id;
		maintenance_agency_id = newMaintenance_agency_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE getPrimary_cube_id() {
		if (primary_cube_id != null && primary_cube_id.eIsProxy()) {
			InternalEObject oldPrimary_cube_id = (InternalEObject)primary_cube_id;
			primary_cube_id = (CUBE)eResolveProxy(oldPrimary_cube_id);
			if (primary_cube_id != oldPrimary_cube_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_RELATIONSHIP__PRIMARY_CUBE_ID, oldPrimary_cube_id, primary_cube_id));
			}
		}
		return primary_cube_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE basicGetPrimary_cube_id() {
		return primary_cube_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setPrimary_cube_id(CUBE newPrimary_cube_id) {
		CUBE oldPrimary_cube_id = primary_cube_id;
		primary_cube_id = newPrimary_cube_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__PRIMARY_CUBE_ID, oldPrimary_cube_id, primary_cube_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getPrimary_cube_variable_id() {
		if (primary_cube_variable_id != null && primary_cube_variable_id.eIsProxy()) {
			InternalEObject oldPrimary_cube_variable_id = (InternalEObject)primary_cube_variable_id;
			primary_cube_variable_id = (VARIABLE)eResolveProxy(oldPrimary_cube_variable_id);
			if (primary_cube_variable_id != oldPrimary_cube_variable_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_RELATIONSHIP__PRIMARY_CUBE_VARIABLE_ID, oldPrimary_cube_variable_id, primary_cube_variable_id));
			}
		}
		return primary_cube_variable_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetPrimary_cube_variable_id() {
		return primary_cube_variable_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setPrimary_cube_variable_id(VARIABLE newPrimary_cube_variable_id) {
		VARIABLE oldPrimary_cube_variable_id = primary_cube_variable_id;
		primary_cube_variable_id = newPrimary_cube_variable_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__PRIMARY_CUBE_VARIABLE_ID, oldPrimary_cube_variable_id, primary_cube_variable_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Date getValid_from() {
		return valid_from;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setValid_from(Date newValid_from) {
		Date oldValid_from = valid_from;
		valid_from = newValid_from;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__VALID_FROM, oldValid_from, valid_from));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Date getValid_to() {
		return valid_to;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setValid_to(Date newValid_to) {
		Date oldValid_to = valid_to;
		valid_to = newValid_to;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__VALID_TO, oldValid_to, valid_to));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getVersion() {
		return version;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setVersion(String newVersion) {
		String oldVersion = version;
		version = newVersion;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_RELATIONSHIP__VERSION, oldVersion, version));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_definitionPackage.CUBE_RELATIONSHIP__CODE:
				return getCode();
			case Data_definitionPackage.CUBE_RELATIONSHIP__CUBE_RELATIONSHIP_ID:
				return getCube_relationship_id();
			case Data_definitionPackage.CUBE_RELATIONSHIP__DESCRIPTION:
				return getDescription();
			case Data_definitionPackage.CUBE_RELATIONSHIP__ESTABLISHES_INTEGRITY:
				return isEstablishes_integrity();
			case Data_definitionPackage.CUBE_RELATIONSHIP__FOREIGN_CUBE_ID:
				if (resolve) return getForeign_cube_id();
				return basicGetForeign_cube_id();
			case Data_definitionPackage.CUBE_RELATIONSHIP__FOREIGN_CUBE_VARIABLE_CODE:
				if (resolve) return getForeign_cube_variable_code();
				return basicGetForeign_cube_variable_code();
			case Data_definitionPackage.CUBE_RELATIONSHIP__MAINTENANCE_AGENCY_ID:
				if (resolve) return getMaintenance_agency_id();
				return basicGetMaintenance_agency_id();
			case Data_definitionPackage.CUBE_RELATIONSHIP__NAME:
				return getName();
			case Data_definitionPackage.CUBE_RELATIONSHIP__PRIMARY_CUBE_ID:
				if (resolve) return getPrimary_cube_id();
				return basicGetPrimary_cube_id();
			case Data_definitionPackage.CUBE_RELATIONSHIP__PRIMARY_CUBE_VARIABLE_ID:
				if (resolve) return getPrimary_cube_variable_id();
				return basicGetPrimary_cube_variable_id();
			case Data_definitionPackage.CUBE_RELATIONSHIP__VALID_FROM:
				return getValid_from();
			case Data_definitionPackage.CUBE_RELATIONSHIP__VALID_TO:
				return getValid_to();
			case Data_definitionPackage.CUBE_RELATIONSHIP__VERSION:
				return getVersion();
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
			case Data_definitionPackage.CUBE_RELATIONSHIP__CODE:
				setCode((String)newValue);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__CUBE_RELATIONSHIP_ID:
				setCube_relationship_id((String)newValue);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__ESTABLISHES_INTEGRITY:
				setEstablishes_integrity((Boolean)newValue);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__FOREIGN_CUBE_ID:
				setForeign_cube_id((CUBE)newValue);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__FOREIGN_CUBE_VARIABLE_CODE:
				setForeign_cube_variable_code((VARIABLE)newValue);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)newValue);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__NAME:
				setName((String)newValue);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__PRIMARY_CUBE_ID:
				setPrimary_cube_id((CUBE)newValue);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__PRIMARY_CUBE_VARIABLE_ID:
				setPrimary_cube_variable_id((VARIABLE)newValue);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__VALID_FROM:
				setValid_from((Date)newValue);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__VALID_TO:
				setValid_to((Date)newValue);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__VERSION:
				setVersion((String)newValue);
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
			case Data_definitionPackage.CUBE_RELATIONSHIP__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__CUBE_RELATIONSHIP_ID:
				setCube_relationship_id(CUBE_RELATIONSHIP_ID_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__ESTABLISHES_INTEGRITY:
				setEstablishes_integrity(ESTABLISHES_INTEGRITY_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__FOREIGN_CUBE_ID:
				setForeign_cube_id((CUBE)null);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__FOREIGN_CUBE_VARIABLE_CODE:
				setForeign_cube_variable_code((VARIABLE)null);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)null);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__PRIMARY_CUBE_ID:
				setPrimary_cube_id((CUBE)null);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__PRIMARY_CUBE_VARIABLE_ID:
				setPrimary_cube_variable_id((VARIABLE)null);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__VALID_FROM:
				setValid_from(VALID_FROM_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__VALID_TO:
				setValid_to(VALID_TO_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_RELATIONSHIP__VERSION:
				setVersion(VERSION_EDEFAULT);
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
			case Data_definitionPackage.CUBE_RELATIONSHIP__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case Data_definitionPackage.CUBE_RELATIONSHIP__CUBE_RELATIONSHIP_ID:
				return CUBE_RELATIONSHIP_ID_EDEFAULT == null ? cube_relationship_id != null : !CUBE_RELATIONSHIP_ID_EDEFAULT.equals(cube_relationship_id);
			case Data_definitionPackage.CUBE_RELATIONSHIP__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case Data_definitionPackage.CUBE_RELATIONSHIP__ESTABLISHES_INTEGRITY:
				return establishes_integrity != ESTABLISHES_INTEGRITY_EDEFAULT;
			case Data_definitionPackage.CUBE_RELATIONSHIP__FOREIGN_CUBE_ID:
				return foreign_cube_id != null;
			case Data_definitionPackage.CUBE_RELATIONSHIP__FOREIGN_CUBE_VARIABLE_CODE:
				return foreign_cube_variable_code != null;
			case Data_definitionPackage.CUBE_RELATIONSHIP__MAINTENANCE_AGENCY_ID:
				return maintenance_agency_id != null;
			case Data_definitionPackage.CUBE_RELATIONSHIP__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Data_definitionPackage.CUBE_RELATIONSHIP__PRIMARY_CUBE_ID:
				return primary_cube_id != null;
			case Data_definitionPackage.CUBE_RELATIONSHIP__PRIMARY_CUBE_VARIABLE_ID:
				return primary_cube_variable_id != null;
			case Data_definitionPackage.CUBE_RELATIONSHIP__VALID_FROM:
				return VALID_FROM_EDEFAULT == null ? valid_from != null : !VALID_FROM_EDEFAULT.equals(valid_from);
			case Data_definitionPackage.CUBE_RELATIONSHIP__VALID_TO:
				return VALID_TO_EDEFAULT == null ? valid_to != null : !VALID_TO_EDEFAULT.equals(valid_to);
			case Data_definitionPackage.CUBE_RELATIONSHIP__VERSION:
				return VERSION_EDEFAULT == null ? version != null : !VERSION_EDEFAULT.equals(version);
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
		result.append(", cube_relationship_id: ");
		result.append(cube_relationship_id);
		result.append(", description: ");
		result.append(description);
		result.append(", establishes_integrity: ");
		result.append(establishes_integrity);
		result.append(", name: ");
		result.append(name);
		result.append(", valid_from: ");
		result.append(valid_from);
		result.append(", valid_to: ");
		result.append(valid_to);
		result.append(", version: ");
		result.append(version);
		result.append(')');
		return result.toString();
	}

} //CUBE_RELATIONSHIPImpl
