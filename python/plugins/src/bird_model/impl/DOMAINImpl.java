/**
 */
package bird_model.impl;

import bird_model.Bird_modelPackage;
import bird_model.DOMAIN;
import bird_model.FACET_COLLECTION;
import bird_model.FACET_VALUE_TYPE;
import bird_model.MAINTENANCE_AGENCY;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>DOMAIN</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link bird_model.impl.DOMAINImpl#getCode <em>Code</em>}</li>
 *   <li>{@link bird_model.impl.DOMAINImpl#getData_type <em>Data type</em>}</li>
 *   <li>{@link bird_model.impl.DOMAINImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link bird_model.impl.DOMAINImpl#getDomain_id <em>Domain id</em>}</li>
 *   <li>{@link bird_model.impl.DOMAINImpl#getFacet_id <em>Facet id</em>}</li>
 *   <li>{@link bird_model.impl.DOMAINImpl#isIs_enumerated <em>Is enumerated</em>}</li>
 *   <li>{@link bird_model.impl.DOMAINImpl#isIs_reference <em>Is reference</em>}</li>
 *   <li>{@link bird_model.impl.DOMAINImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link bird_model.impl.DOMAINImpl#getName <em>Name</em>}</li>
 *   <li>{@link bird_model.impl.DOMAINImpl#getDisplayName <em>Display Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DOMAINImpl extends MinimalEObjectImpl.Container implements DOMAIN {
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
	 * The default value of the '{@link #getData_type() <em>Data type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getData_type()
	 * @generated
	 * @ordered
	 */
	protected static final FACET_VALUE_TYPE DATA_TYPE_EDEFAULT = FACET_VALUE_TYPE.BIG_INTEGER;

	/**
	 * The cached value of the '{@link #getData_type() <em>Data type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getData_type()
	 * @generated
	 * @ordered
	 */
	protected FACET_VALUE_TYPE data_type = DATA_TYPE_EDEFAULT;

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
	 * The default value of the '{@link #getDomain_id() <em>Domain id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDomain_id()
	 * @generated
	 * @ordered
	 */
	protected static final String DOMAIN_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getDomain_id() <em>Domain id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDomain_id()
	 * @generated
	 * @ordered
	 */
	protected String domain_id = DOMAIN_ID_EDEFAULT;

	/**
	 * The cached value of the '{@link #getFacet_id() <em>Facet id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFacet_id()
	 * @generated
	 * @ordered
	 */
	protected FACET_COLLECTION facet_id;

	/**
	 * The default value of the '{@link #isIs_enumerated() <em>Is enumerated</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_enumerated()
	 * @generated
	 * @ordered
	 */
	protected static final boolean IS_ENUMERATED_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isIs_enumerated() <em>Is enumerated</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_enumerated()
	 * @generated
	 * @ordered
	 */
	protected boolean is_enumerated = IS_ENUMERATED_EDEFAULT;

	/**
	 * The default value of the '{@link #isIs_reference() <em>Is reference</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_reference()
	 * @generated
	 * @ordered
	 */
	protected static final boolean IS_REFERENCE_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isIs_reference() <em>Is reference</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_reference()
	 * @generated
	 * @ordered
	 */
	protected boolean is_reference = IS_REFERENCE_EDEFAULT;

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
	protected DOMAINImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Bird_modelPackage.Literals.DOMAIN;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.DOMAIN__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FACET_VALUE_TYPE getData_type() {
		return data_type;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setData_type(FACET_VALUE_TYPE newData_type) {
		FACET_VALUE_TYPE oldData_type = data_type;
		data_type = newData_type == null ? DATA_TYPE_EDEFAULT : newData_type;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.DOMAIN__DATA_TYPE, oldData_type, data_type));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.DOMAIN__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getDomain_id() {
		return domain_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDomain_id(String newDomain_id) {
		String oldDomain_id = domain_id;
		domain_id = newDomain_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.DOMAIN__DOMAIN_ID, oldDomain_id, domain_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FACET_COLLECTION getFacet_id() {
		if (facet_id != null && facet_id.eIsProxy()) {
			InternalEObject oldFacet_id = (InternalEObject)facet_id;
			facet_id = (FACET_COLLECTION)eResolveProxy(oldFacet_id);
			if (facet_id != oldFacet_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Bird_modelPackage.DOMAIN__FACET_ID, oldFacet_id, facet_id));
			}
		}
		return facet_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FACET_COLLECTION basicGetFacet_id() {
		return facet_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setFacet_id(FACET_COLLECTION newFacet_id) {
		FACET_COLLECTION oldFacet_id = facet_id;
		facet_id = newFacet_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.DOMAIN__FACET_ID, oldFacet_id, facet_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isIs_enumerated() {
		return is_enumerated;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setIs_enumerated(boolean newIs_enumerated) {
		boolean oldIs_enumerated = is_enumerated;
		is_enumerated = newIs_enumerated;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.DOMAIN__IS_ENUMERATED, oldIs_enumerated, is_enumerated));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isIs_reference() {
		return is_reference;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setIs_reference(boolean newIs_reference) {
		boolean oldIs_reference = is_reference;
		is_reference = newIs_reference;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.DOMAIN__IS_REFERENCE, oldIs_reference, is_reference));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Bird_modelPackage.DOMAIN__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.DOMAIN__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.DOMAIN__NAME, oldName, name));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.DOMAIN__DISPLAY_NAME, oldDisplayName, displayName));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Bird_modelPackage.DOMAIN__CODE:
				return getCode();
			case Bird_modelPackage.DOMAIN__DATA_TYPE:
				return getData_type();
			case Bird_modelPackage.DOMAIN__DESCRIPTION:
				return getDescription();
			case Bird_modelPackage.DOMAIN__DOMAIN_ID:
				return getDomain_id();
			case Bird_modelPackage.DOMAIN__FACET_ID:
				if (resolve) return getFacet_id();
				return basicGetFacet_id();
			case Bird_modelPackage.DOMAIN__IS_ENUMERATED:
				return isIs_enumerated();
			case Bird_modelPackage.DOMAIN__IS_REFERENCE:
				return isIs_reference();
			case Bird_modelPackage.DOMAIN__MAINTENANCE_AGENCY_ID:
				if (resolve) return getMaintenance_agency_id();
				return basicGetMaintenance_agency_id();
			case Bird_modelPackage.DOMAIN__NAME:
				return getName();
			case Bird_modelPackage.DOMAIN__DISPLAY_NAME:
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
			case Bird_modelPackage.DOMAIN__CODE:
				setCode((String)newValue);
				return;
			case Bird_modelPackage.DOMAIN__DATA_TYPE:
				setData_type((FACET_VALUE_TYPE)newValue);
				return;
			case Bird_modelPackage.DOMAIN__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case Bird_modelPackage.DOMAIN__DOMAIN_ID:
				setDomain_id((String)newValue);
				return;
			case Bird_modelPackage.DOMAIN__FACET_ID:
				setFacet_id((FACET_COLLECTION)newValue);
				return;
			case Bird_modelPackage.DOMAIN__IS_ENUMERATED:
				setIs_enumerated((Boolean)newValue);
				return;
			case Bird_modelPackage.DOMAIN__IS_REFERENCE:
				setIs_reference((Boolean)newValue);
				return;
			case Bird_modelPackage.DOMAIN__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)newValue);
				return;
			case Bird_modelPackage.DOMAIN__NAME:
				setName((String)newValue);
				return;
			case Bird_modelPackage.DOMAIN__DISPLAY_NAME:
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
			case Bird_modelPackage.DOMAIN__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case Bird_modelPackage.DOMAIN__DATA_TYPE:
				setData_type(DATA_TYPE_EDEFAULT);
				return;
			case Bird_modelPackage.DOMAIN__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case Bird_modelPackage.DOMAIN__DOMAIN_ID:
				setDomain_id(DOMAIN_ID_EDEFAULT);
				return;
			case Bird_modelPackage.DOMAIN__FACET_ID:
				setFacet_id((FACET_COLLECTION)null);
				return;
			case Bird_modelPackage.DOMAIN__IS_ENUMERATED:
				setIs_enumerated(IS_ENUMERATED_EDEFAULT);
				return;
			case Bird_modelPackage.DOMAIN__IS_REFERENCE:
				setIs_reference(IS_REFERENCE_EDEFAULT);
				return;
			case Bird_modelPackage.DOMAIN__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)null);
				return;
			case Bird_modelPackage.DOMAIN__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Bird_modelPackage.DOMAIN__DISPLAY_NAME:
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
			case Bird_modelPackage.DOMAIN__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case Bird_modelPackage.DOMAIN__DATA_TYPE:
				return data_type != DATA_TYPE_EDEFAULT;
			case Bird_modelPackage.DOMAIN__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case Bird_modelPackage.DOMAIN__DOMAIN_ID:
				return DOMAIN_ID_EDEFAULT == null ? domain_id != null : !DOMAIN_ID_EDEFAULT.equals(domain_id);
			case Bird_modelPackage.DOMAIN__FACET_ID:
				return facet_id != null;
			case Bird_modelPackage.DOMAIN__IS_ENUMERATED:
				return is_enumerated != IS_ENUMERATED_EDEFAULT;
			case Bird_modelPackage.DOMAIN__IS_REFERENCE:
				return is_reference != IS_REFERENCE_EDEFAULT;
			case Bird_modelPackage.DOMAIN__MAINTENANCE_AGENCY_ID:
				return maintenance_agency_id != null;
			case Bird_modelPackage.DOMAIN__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Bird_modelPackage.DOMAIN__DISPLAY_NAME:
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
		result.append(", data_type: ");
		result.append(data_type);
		result.append(", description: ");
		result.append(description);
		result.append(", domain_id: ");
		result.append(domain_id);
		result.append(", is_enumerated: ");
		result.append(is_enumerated);
		result.append(", is_reference: ");
		result.append(is_reference);
		result.append(", name: ");
		result.append(name);
		result.append(", displayName: ");
		result.append(displayName);
		result.append(')');
		return result.toString();
	}

} //DOMAINImpl
