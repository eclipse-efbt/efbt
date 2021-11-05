/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl;

import java.util.Date;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.FRAMEWORK;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>CUBE</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#getCube_id <em>Cube id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#getCube_structure_id <em>Cube structure id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#getCube_type <em>Cube type</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#getFramework_id <em>Framework id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#isIs_allowed <em>Is allowed</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#isPublished <em>Published</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#getValid_to <em>Valid to</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#getVersion <em>Version</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBEImpl#getDisplayName <em>Display Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CUBEImpl extends MinimalEObjectImpl.Container implements CUBE {
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
	 * The default value of the '{@link #getCube_id() <em>Cube id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_id()
	 * @generated
	 * @ordered
	 */
	protected static final String CUBE_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getCube_id() <em>Cube id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_id()
	 * @generated
	 * @ordered
	 */
	protected String cube_id = CUBE_ID_EDEFAULT;

	/**
	 * The cached value of the '{@link #getCube_structure_id() <em>Cube structure id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_structure_id()
	 * @generated
	 * @ordered
	 */
	protected CUBE_STRUCTURE cube_structure_id;

	/**
	 * The default value of the '{@link #getCube_type() <em>Cube type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_type()
	 * @generated
	 * @ordered
	 */
	protected static final String CUBE_TYPE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getCube_type() <em>Cube type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_type()
	 * @generated
	 * @ordered
	 */
	protected String cube_type = CUBE_TYPE_EDEFAULT;

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
	 * The cached value of the '{@link #getFramework_id() <em>Framework id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFramework_id()
	 * @generated
	 * @ordered
	 */
	protected FRAMEWORK framework_id;

	/**
	 * The default value of the '{@link #isIs_allowed() <em>Is allowed</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_allowed()
	 * @generated
	 * @ordered
	 */
	protected static final boolean IS_ALLOWED_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isIs_allowed() <em>Is allowed</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_allowed()
	 * @generated
	 * @ordered
	 */
	protected boolean is_allowed = IS_ALLOWED_EDEFAULT;

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
	 * The default value of the '{@link #isPublished() <em>Published</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isPublished()
	 * @generated
	 * @ordered
	 */
	protected static final boolean PUBLISHED_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isPublished() <em>Published</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isPublished()
	 * @generated
	 * @ordered
	 */
	protected boolean published = PUBLISHED_EDEFAULT;

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
	protected CUBEImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_definitionPackage.Literals.CUBE;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getCube_id() {
		return cube_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube_id(String newCube_id) {
		String oldCube_id = cube_id;
		cube_id = newCube_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__CUBE_ID, oldCube_id, cube_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE_STRUCTURE getCube_structure_id() {
		if (cube_structure_id != null && cube_structure_id.eIsProxy()) {
			InternalEObject oldCube_structure_id = (InternalEObject)cube_structure_id;
			cube_structure_id = (CUBE_STRUCTURE)eResolveProxy(oldCube_structure_id);
			if (cube_structure_id != oldCube_structure_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE__CUBE_STRUCTURE_ID, oldCube_structure_id, cube_structure_id));
			}
		}
		return cube_structure_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE_STRUCTURE basicGetCube_structure_id() {
		return cube_structure_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube_structure_id(CUBE_STRUCTURE newCube_structure_id) {
		CUBE_STRUCTURE oldCube_structure_id = cube_structure_id;
		cube_structure_id = newCube_structure_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__CUBE_STRUCTURE_ID, oldCube_structure_id, cube_structure_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getCube_type() {
		return cube_type;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube_type(String newCube_type) {
		String oldCube_type = cube_type;
		cube_type = newCube_type;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__CUBE_TYPE, oldCube_type, cube_type));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FRAMEWORK getFramework_id() {
		if (framework_id != null && framework_id.eIsProxy()) {
			InternalEObject oldFramework_id = (InternalEObject)framework_id;
			framework_id = (FRAMEWORK)eResolveProxy(oldFramework_id);
			if (framework_id != oldFramework_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE__FRAMEWORK_ID, oldFramework_id, framework_id));
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
	@Override
	public void setFramework_id(FRAMEWORK newFramework_id) {
		FRAMEWORK oldFramework_id = framework_id;
		framework_id = newFramework_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__FRAMEWORK_ID, oldFramework_id, framework_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isIs_allowed() {
		return is_allowed;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setIs_allowed(boolean newIs_allowed) {
		boolean oldIs_allowed = is_allowed;
		is_allowed = newIs_allowed;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__IS_ALLOWED, oldIs_allowed, is_allowed));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isPublished() {
		return published;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setPublished(boolean newPublished) {
		boolean oldPublished = published;
		published = newPublished;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__PUBLISHED, oldPublished, published));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__VALID_FROM, oldValid_from, valid_from));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__VALID_TO, oldValid_to, valid_to));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__VERSION, oldVersion, version));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getDisplayName() {
		return displayName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setDisplayName(String newDisplayName) {
		String oldDisplayName = displayName;
		displayName = newDisplayName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE__DISPLAY_NAME, oldDisplayName, displayName));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_definitionPackage.CUBE__CODE:
				return getCode();
			case Data_definitionPackage.CUBE__CUBE_ID:
				return getCube_id();
			case Data_definitionPackage.CUBE__CUBE_STRUCTURE_ID:
				if (resolve) return getCube_structure_id();
				return basicGetCube_structure_id();
			case Data_definitionPackage.CUBE__CUBE_TYPE:
				return getCube_type();
			case Data_definitionPackage.CUBE__DESCRIPTION:
				return getDescription();
			case Data_definitionPackage.CUBE__FRAMEWORK_ID:
				if (resolve) return getFramework_id();
				return basicGetFramework_id();
			case Data_definitionPackage.CUBE__IS_ALLOWED:
				return isIs_allowed();
			case Data_definitionPackage.CUBE__MAINTENANCE_AGENCY_ID:
				if (resolve) return getMaintenance_agency_id();
				return basicGetMaintenance_agency_id();
			case Data_definitionPackage.CUBE__NAME:
				return getName();
			case Data_definitionPackage.CUBE__PUBLISHED:
				return isPublished();
			case Data_definitionPackage.CUBE__VALID_FROM:
				return getValid_from();
			case Data_definitionPackage.CUBE__VALID_TO:
				return getValid_to();
			case Data_definitionPackage.CUBE__VERSION:
				return getVersion();
			case Data_definitionPackage.CUBE__DISPLAY_NAME:
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
			case Data_definitionPackage.CUBE__CODE:
				setCode((String)newValue);
				return;
			case Data_definitionPackage.CUBE__CUBE_ID:
				setCube_id((String)newValue);
				return;
			case Data_definitionPackage.CUBE__CUBE_STRUCTURE_ID:
				setCube_structure_id((CUBE_STRUCTURE)newValue);
				return;
			case Data_definitionPackage.CUBE__CUBE_TYPE:
				setCube_type((String)newValue);
				return;
			case Data_definitionPackage.CUBE__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case Data_definitionPackage.CUBE__FRAMEWORK_ID:
				setFramework_id((FRAMEWORK)newValue);
				return;
			case Data_definitionPackage.CUBE__IS_ALLOWED:
				setIs_allowed((Boolean)newValue);
				return;
			case Data_definitionPackage.CUBE__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)newValue);
				return;
			case Data_definitionPackage.CUBE__NAME:
				setName((String)newValue);
				return;
			case Data_definitionPackage.CUBE__PUBLISHED:
				setPublished((Boolean)newValue);
				return;
			case Data_definitionPackage.CUBE__VALID_FROM:
				setValid_from((Date)newValue);
				return;
			case Data_definitionPackage.CUBE__VALID_TO:
				setValid_to((Date)newValue);
				return;
			case Data_definitionPackage.CUBE__VERSION:
				setVersion((String)newValue);
				return;
			case Data_definitionPackage.CUBE__DISPLAY_NAME:
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
			case Data_definitionPackage.CUBE__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE__CUBE_ID:
				setCube_id(CUBE_ID_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE__CUBE_STRUCTURE_ID:
				setCube_structure_id((CUBE_STRUCTURE)null);
				return;
			case Data_definitionPackage.CUBE__CUBE_TYPE:
				setCube_type(CUBE_TYPE_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE__FRAMEWORK_ID:
				setFramework_id((FRAMEWORK)null);
				return;
			case Data_definitionPackage.CUBE__IS_ALLOWED:
				setIs_allowed(IS_ALLOWED_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)null);
				return;
			case Data_definitionPackage.CUBE__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE__PUBLISHED:
				setPublished(PUBLISHED_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE__VALID_FROM:
				setValid_from(VALID_FROM_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE__VALID_TO:
				setValid_to(VALID_TO_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE__VERSION:
				setVersion(VERSION_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE__DISPLAY_NAME:
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
			case Data_definitionPackage.CUBE__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case Data_definitionPackage.CUBE__CUBE_ID:
				return CUBE_ID_EDEFAULT == null ? cube_id != null : !CUBE_ID_EDEFAULT.equals(cube_id);
			case Data_definitionPackage.CUBE__CUBE_STRUCTURE_ID:
				return cube_structure_id != null;
			case Data_definitionPackage.CUBE__CUBE_TYPE:
				return CUBE_TYPE_EDEFAULT == null ? cube_type != null : !CUBE_TYPE_EDEFAULT.equals(cube_type);
			case Data_definitionPackage.CUBE__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case Data_definitionPackage.CUBE__FRAMEWORK_ID:
				return framework_id != null;
			case Data_definitionPackage.CUBE__IS_ALLOWED:
				return is_allowed != IS_ALLOWED_EDEFAULT;
			case Data_definitionPackage.CUBE__MAINTENANCE_AGENCY_ID:
				return maintenance_agency_id != null;
			case Data_definitionPackage.CUBE__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Data_definitionPackage.CUBE__PUBLISHED:
				return published != PUBLISHED_EDEFAULT;
			case Data_definitionPackage.CUBE__VALID_FROM:
				return VALID_FROM_EDEFAULT == null ? valid_from != null : !VALID_FROM_EDEFAULT.equals(valid_from);
			case Data_definitionPackage.CUBE__VALID_TO:
				return VALID_TO_EDEFAULT == null ? valid_to != null : !VALID_TO_EDEFAULT.equals(valid_to);
			case Data_definitionPackage.CUBE__VERSION:
				return VERSION_EDEFAULT == null ? version != null : !VERSION_EDEFAULT.equals(version);
			case Data_definitionPackage.CUBE__DISPLAY_NAME:
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
		result.append(", cube_id: ");
		result.append(cube_id);
		result.append(", cube_type: ");
		result.append(cube_type);
		result.append(", description: ");
		result.append(description);
		result.append(", is_allowed: ");
		result.append(is_allowed);
		result.append(", name: ");
		result.append(name);
		result.append(", published: ");
		result.append(published);
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

} //CUBEImpl
