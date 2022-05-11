/**
 */
package bird_model.impl;

import bird_model.Bird_modelPackage;
import bird_model.MAINTENANCE_AGENCY;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>MAINTENANCE AGENCY</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link bird_model.impl.MAINTENANCE_AGENCYImpl#getCode <em>Code</em>}</li>
 *   <li>{@link bird_model.impl.MAINTENANCE_AGENCYImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link bird_model.impl.MAINTENANCE_AGENCYImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MAINTENANCE_AGENCYImpl extends MinimalEObjectImpl.Container implements MAINTENANCE_AGENCY {
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
	protected MAINTENANCE_AGENCYImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Bird_modelPackage.Literals.MAINTENANCE_AGENCY;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.MAINTENANCE_AGENCY__CODE, oldCode, code));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.MAINTENANCE_AGENCY__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.MAINTENANCE_AGENCY__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Bird_modelPackage.MAINTENANCE_AGENCY__CODE:
				return getCode();
			case Bird_modelPackage.MAINTENANCE_AGENCY__MAINTENANCE_AGENCY_ID:
				return getMaintenance_agency_id();
			case Bird_modelPackage.MAINTENANCE_AGENCY__NAME:
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
			case Bird_modelPackage.MAINTENANCE_AGENCY__CODE:
				setCode((String)newValue);
				return;
			case Bird_modelPackage.MAINTENANCE_AGENCY__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((String)newValue);
				return;
			case Bird_modelPackage.MAINTENANCE_AGENCY__NAME:
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
			case Bird_modelPackage.MAINTENANCE_AGENCY__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case Bird_modelPackage.MAINTENANCE_AGENCY__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id(MAINTENANCE_AGENCY_ID_EDEFAULT);
				return;
			case Bird_modelPackage.MAINTENANCE_AGENCY__NAME:
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
			case Bird_modelPackage.MAINTENANCE_AGENCY__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case Bird_modelPackage.MAINTENANCE_AGENCY__MAINTENANCE_AGENCY_ID:
				return MAINTENANCE_AGENCY_ID_EDEFAULT == null ? maintenance_agency_id != null : !MAINTENANCE_AGENCY_ID_EDEFAULT.equals(maintenance_agency_id);
			case Bird_modelPackage.MAINTENANCE_AGENCY__NAME:
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
		result.append(", maintenance_agency_id: ");
		result.append(maintenance_agency_id);
		result.append(", name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //MAINTENANCE_AGENCYImpl
