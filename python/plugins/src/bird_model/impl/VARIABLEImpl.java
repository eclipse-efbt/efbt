/**
 */
package bird_model.impl;

import bird_model.Bird_modelPackage;
import bird_model.DOMAIN;
import bird_model.MAINTENANCE_AGENCY;
import bird_model.VARIABLE;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VARIABLE</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link bird_model.impl.VARIABLEImpl#getCode <em>Code</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLEImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLEImpl#getDomain_id <em>Domain id</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLEImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLEImpl#getPrimary_concept <em>Primary concept</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLEImpl#getVariable_id <em>Variable id</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLEImpl#getName <em>Name</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLEImpl#getDisplayName <em>Display Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VARIABLEImpl extends MinimalEObjectImpl.Container implements VARIABLE {
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
	 * The cached value of the '{@link #getDomain_id() <em>Domain id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDomain_id()
	 * @generated
	 * @ordered
	 */
	protected DOMAIN domain_id;

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
	 * The default value of the '{@link #getPrimary_concept() <em>Primary concept</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getPrimary_concept()
	 * @generated
	 * @ordered
	 */
	protected static final String PRIMARY_CONCEPT_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getPrimary_concept() <em>Primary concept</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getPrimary_concept()
	 * @generated
	 * @ordered
	 */
	protected String primary_concept = PRIMARY_CONCEPT_EDEFAULT;

	/**
	 * The default value of the '{@link #getVariable_id() <em>Variable id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable_id()
	 * @generated
	 * @ordered
	 */
	protected static final String VARIABLE_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getVariable_id() <em>Variable id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable_id()
	 * @generated
	 * @ordered
	 */
	protected String variable_id = VARIABLE_ID_EDEFAULT;

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
	protected VARIABLEImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Bird_modelPackage.Literals.VARIABLE;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE__CODE, oldCode, code));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DOMAIN getDomain_id() {
		if (domain_id != null && domain_id.eIsProxy()) {
			InternalEObject oldDomain_id = (InternalEObject)domain_id;
			domain_id = (DOMAIN)eResolveProxy(oldDomain_id);
			if (domain_id != oldDomain_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Bird_modelPackage.VARIABLE__DOMAIN_ID, oldDomain_id, domain_id));
			}
		}
		return domain_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DOMAIN basicGetDomain_id() {
		return domain_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDomain_id(DOMAIN newDomain_id) {
		DOMAIN oldDomain_id = domain_id;
		domain_id = newDomain_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE__DOMAIN_ID, oldDomain_id, domain_id));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Bird_modelPackage.VARIABLE__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getPrimary_concept() {
		return primary_concept;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setPrimary_concept(String newPrimary_concept) {
		String oldPrimary_concept = primary_concept;
		primary_concept = newPrimary_concept;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE__PRIMARY_CONCEPT, oldPrimary_concept, primary_concept));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getVariable_id() {
		return variable_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVariable_id(String newVariable_id) {
		String oldVariable_id = variable_id;
		variable_id = newVariable_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE__VARIABLE_ID, oldVariable_id, variable_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE__NAME, oldName, name));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE__DISPLAY_NAME, oldDisplayName, displayName));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Bird_modelPackage.VARIABLE__CODE:
				return getCode();
			case Bird_modelPackage.VARIABLE__DESCRIPTION:
				return getDescription();
			case Bird_modelPackage.VARIABLE__DOMAIN_ID:
				if (resolve) return getDomain_id();
				return basicGetDomain_id();
			case Bird_modelPackage.VARIABLE__MAINTENANCE_AGENCY_ID:
				if (resolve) return getMaintenance_agency_id();
				return basicGetMaintenance_agency_id();
			case Bird_modelPackage.VARIABLE__PRIMARY_CONCEPT:
				return getPrimary_concept();
			case Bird_modelPackage.VARIABLE__VARIABLE_ID:
				return getVariable_id();
			case Bird_modelPackage.VARIABLE__NAME:
				return getName();
			case Bird_modelPackage.VARIABLE__DISPLAY_NAME:
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
			case Bird_modelPackage.VARIABLE__CODE:
				setCode((String)newValue);
				return;
			case Bird_modelPackage.VARIABLE__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case Bird_modelPackage.VARIABLE__DOMAIN_ID:
				setDomain_id((DOMAIN)newValue);
				return;
			case Bird_modelPackage.VARIABLE__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)newValue);
				return;
			case Bird_modelPackage.VARIABLE__PRIMARY_CONCEPT:
				setPrimary_concept((String)newValue);
				return;
			case Bird_modelPackage.VARIABLE__VARIABLE_ID:
				setVariable_id((String)newValue);
				return;
			case Bird_modelPackage.VARIABLE__NAME:
				setName((String)newValue);
				return;
			case Bird_modelPackage.VARIABLE__DISPLAY_NAME:
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
			case Bird_modelPackage.VARIABLE__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case Bird_modelPackage.VARIABLE__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case Bird_modelPackage.VARIABLE__DOMAIN_ID:
				setDomain_id((DOMAIN)null);
				return;
			case Bird_modelPackage.VARIABLE__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)null);
				return;
			case Bird_modelPackage.VARIABLE__PRIMARY_CONCEPT:
				setPrimary_concept(PRIMARY_CONCEPT_EDEFAULT);
				return;
			case Bird_modelPackage.VARIABLE__VARIABLE_ID:
				setVariable_id(VARIABLE_ID_EDEFAULT);
				return;
			case Bird_modelPackage.VARIABLE__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Bird_modelPackage.VARIABLE__DISPLAY_NAME:
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
			case Bird_modelPackage.VARIABLE__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case Bird_modelPackage.VARIABLE__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case Bird_modelPackage.VARIABLE__DOMAIN_ID:
				return domain_id != null;
			case Bird_modelPackage.VARIABLE__MAINTENANCE_AGENCY_ID:
				return maintenance_agency_id != null;
			case Bird_modelPackage.VARIABLE__PRIMARY_CONCEPT:
				return PRIMARY_CONCEPT_EDEFAULT == null ? primary_concept != null : !PRIMARY_CONCEPT_EDEFAULT.equals(primary_concept);
			case Bird_modelPackage.VARIABLE__VARIABLE_ID:
				return VARIABLE_ID_EDEFAULT == null ? variable_id != null : !VARIABLE_ID_EDEFAULT.equals(variable_id);
			case Bird_modelPackage.VARIABLE__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Bird_modelPackage.VARIABLE__DISPLAY_NAME:
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
		result.append(", description: ");
		result.append(description);
		result.append(", primary_concept: ");
		result.append(primary_concept);
		result.append(", variable_id: ");
		result.append(variable_id);
		result.append(", name: ");
		result.append(name);
		result.append(", displayName: ");
		result.append(displayName);
		result.append(')');
		return result.toString();
	}

} //VARIABLEImpl
