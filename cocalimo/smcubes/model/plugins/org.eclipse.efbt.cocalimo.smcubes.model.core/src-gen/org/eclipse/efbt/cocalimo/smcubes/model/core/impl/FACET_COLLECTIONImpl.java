/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;
import org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION;
import org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE;
import org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>FACET COLLECTION</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_COLLECTIONImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_COLLECTIONImpl#getFacet_id <em>Facet id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_COLLECTIONImpl#getFacet_value_type <em>Facet value type</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_COLLECTIONImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.FACET_COLLECTIONImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FACET_COLLECTIONImpl extends MinimalEObjectImpl.Container implements FACET_COLLECTION {
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
	 * The default value of the '{@link #getFacet_id() <em>Facet id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFacet_id()
	 * @generated
	 * @ordered
	 */
	protected static final String FACET_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getFacet_id() <em>Facet id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFacet_id()
	 * @generated
	 * @ordered
	 */
	protected String facet_id = FACET_ID_EDEFAULT;

	/**
	 * The default value of the '{@link #getFacet_value_type() <em>Facet value type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFacet_value_type()
	 * @generated
	 * @ordered
	 */
	protected static final FACET_VALUE_TYPE FACET_VALUE_TYPE_EDEFAULT = FACET_VALUE_TYPE.BIG_INTEGER;

	/**
	 * The cached value of the '{@link #getFacet_value_type() <em>Facet value type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFacet_value_type()
	 * @generated
	 * @ordered
	 */
	protected FACET_VALUE_TYPE facet_value_type = FACET_VALUE_TYPE_EDEFAULT;

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
	protected FACET_COLLECTIONImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return CorePackage.Literals.FACET_COLLECTION;
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
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_COLLECTION__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getFacet_id() {
		return facet_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setFacet_id(String newFacet_id) {
		String oldFacet_id = facet_id;
		facet_id = newFacet_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_COLLECTION__FACET_ID, oldFacet_id, facet_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FACET_VALUE_TYPE getFacet_value_type() {
		return facet_value_type;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setFacet_value_type(FACET_VALUE_TYPE newFacet_value_type) {
		FACET_VALUE_TYPE oldFacet_value_type = facet_value_type;
		facet_value_type = newFacet_value_type == null ? FACET_VALUE_TYPE_EDEFAULT : newFacet_value_type;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_COLLECTION__FACET_VALUE_TYPE, oldFacet_value_type, facet_value_type));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CorePackage.FACET_COLLECTION__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_COLLECTION__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_COLLECTION__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case CorePackage.FACET_COLLECTION__CODE:
				return getCode();
			case CorePackage.FACET_COLLECTION__FACET_ID:
				return getFacet_id();
			case CorePackage.FACET_COLLECTION__FACET_VALUE_TYPE:
				return getFacet_value_type();
			case CorePackage.FACET_COLLECTION__MAINTENANCE_AGENCY_ID:
				if (resolve) return getMaintenance_agency_id();
				return basicGetMaintenance_agency_id();
			case CorePackage.FACET_COLLECTION__NAME:
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
			case CorePackage.FACET_COLLECTION__CODE:
				setCode((String)newValue);
				return;
			case CorePackage.FACET_COLLECTION__FACET_ID:
				setFacet_id((String)newValue);
				return;
			case CorePackage.FACET_COLLECTION__FACET_VALUE_TYPE:
				setFacet_value_type((FACET_VALUE_TYPE)newValue);
				return;
			case CorePackage.FACET_COLLECTION__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)newValue);
				return;
			case CorePackage.FACET_COLLECTION__NAME:
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
			case CorePackage.FACET_COLLECTION__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case CorePackage.FACET_COLLECTION__FACET_ID:
				setFacet_id(FACET_ID_EDEFAULT);
				return;
			case CorePackage.FACET_COLLECTION__FACET_VALUE_TYPE:
				setFacet_value_type(FACET_VALUE_TYPE_EDEFAULT);
				return;
			case CorePackage.FACET_COLLECTION__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)null);
				return;
			case CorePackage.FACET_COLLECTION__NAME:
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
			case CorePackage.FACET_COLLECTION__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case CorePackage.FACET_COLLECTION__FACET_ID:
				return FACET_ID_EDEFAULT == null ? facet_id != null : !FACET_ID_EDEFAULT.equals(facet_id);
			case CorePackage.FACET_COLLECTION__FACET_VALUE_TYPE:
				return facet_value_type != FACET_VALUE_TYPE_EDEFAULT;
			case CorePackage.FACET_COLLECTION__MAINTENANCE_AGENCY_ID:
				return maintenance_agency_id != null;
			case CorePackage.FACET_COLLECTION__NAME:
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
		result.append(", facet_id: ");
		result.append(facet_id);
		result.append(", facet_value_type: ");
		result.append(facet_value_type);
		result.append(", name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //FACET_COLLECTIONImpl
