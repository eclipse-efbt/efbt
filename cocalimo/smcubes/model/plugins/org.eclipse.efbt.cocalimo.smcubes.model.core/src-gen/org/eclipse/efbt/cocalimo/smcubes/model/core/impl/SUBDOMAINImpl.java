/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;
import org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN;
import org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_COLLECTION;
import org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY;
import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ClassifierImpl;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>SUBDOMAIN</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAINImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAINImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAINImpl#getDomain_id <em>Domain id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAINImpl#getFacet_id <em>Facet id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAINImpl#isIs_listed <em>Is listed</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAINImpl#isIs_natural <em>Is natural</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAINImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAINImpl#getSubdomain_id <em>Subdomain id</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SUBDOMAINImpl extends ClassifierImpl implements SUBDOMAIN {
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
	 * The cached value of the '{@link #getFacet_id() <em>Facet id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFacet_id()
	 * @generated
	 * @ordered
	 */
	protected FACET_COLLECTION facet_id;

	/**
	 * The default value of the '{@link #isIs_listed() <em>Is listed</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_listed()
	 * @generated
	 * @ordered
	 */
	protected static final boolean IS_LISTED_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isIs_listed() <em>Is listed</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_listed()
	 * @generated
	 * @ordered
	 */
	protected boolean is_listed = IS_LISTED_EDEFAULT;

	/**
	 * The default value of the '{@link #isIs_natural() <em>Is natural</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_natural()
	 * @generated
	 * @ordered
	 */
	protected static final boolean IS_NATURAL_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isIs_natural() <em>Is natural</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_natural()
	 * @generated
	 * @ordered
	 */
	protected boolean is_natural = IS_NATURAL_EDEFAULT;

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
	 * The default value of the '{@link #getSubdomain_id() <em>Subdomain id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubdomain_id()
	 * @generated
	 * @ordered
	 */
	protected static final String SUBDOMAIN_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getSubdomain_id() <em>Subdomain id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubdomain_id()
	 * @generated
	 * @ordered
	 */
	protected String subdomain_id = SUBDOMAIN_ID_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SUBDOMAINImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return CorePackage.Literals.SUBDOMAIN;
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
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.SUBDOMAIN__CODE, oldCode, code));
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
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.SUBDOMAIN__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public DOMAIN getDomain_id() {
		if (domain_id != null && domain_id.eIsProxy()) {
			InternalEObject oldDomain_id = (InternalEObject)domain_id;
			domain_id = (DOMAIN)eResolveProxy(oldDomain_id);
			if (domain_id != oldDomain_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CorePackage.SUBDOMAIN__DOMAIN_ID, oldDomain_id, domain_id));
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
	@Override
	public void setDomain_id(DOMAIN newDomain_id) {
		DOMAIN oldDomain_id = domain_id;
		domain_id = newDomain_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.SUBDOMAIN__DOMAIN_ID, oldDomain_id, domain_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FACET_COLLECTION getFacet_id() {
		if (facet_id != null && facet_id.eIsProxy()) {
			InternalEObject oldFacet_id = (InternalEObject)facet_id;
			facet_id = (FACET_COLLECTION)eResolveProxy(oldFacet_id);
			if (facet_id != oldFacet_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CorePackage.SUBDOMAIN__FACET_ID, oldFacet_id, facet_id));
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
	@Override
	public void setFacet_id(FACET_COLLECTION newFacet_id) {
		FACET_COLLECTION oldFacet_id = facet_id;
		facet_id = newFacet_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.SUBDOMAIN__FACET_ID, oldFacet_id, facet_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isIs_listed() {
		return is_listed;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setIs_listed(boolean newIs_listed) {
		boolean oldIs_listed = is_listed;
		is_listed = newIs_listed;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.SUBDOMAIN__IS_LISTED, oldIs_listed, is_listed));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isIs_natural() {
		return is_natural;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setIs_natural(boolean newIs_natural) {
		boolean oldIs_natural = is_natural;
		is_natural = newIs_natural;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.SUBDOMAIN__IS_NATURAL, oldIs_natural, is_natural));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CorePackage.SUBDOMAIN__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.SUBDOMAIN__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getSubdomain_id() {
		return subdomain_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSubdomain_id(String newSubdomain_id) {
		String oldSubdomain_id = subdomain_id;
		subdomain_id = newSubdomain_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.SUBDOMAIN__SUBDOMAIN_ID, oldSubdomain_id, subdomain_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case CorePackage.SUBDOMAIN__CODE:
				return getCode();
			case CorePackage.SUBDOMAIN__DESCRIPTION:
				return getDescription();
			case CorePackage.SUBDOMAIN__DOMAIN_ID:
				if (resolve) return getDomain_id();
				return basicGetDomain_id();
			case CorePackage.SUBDOMAIN__FACET_ID:
				if (resolve) return getFacet_id();
				return basicGetFacet_id();
			case CorePackage.SUBDOMAIN__IS_LISTED:
				return isIs_listed();
			case CorePackage.SUBDOMAIN__IS_NATURAL:
				return isIs_natural();
			case CorePackage.SUBDOMAIN__MAINTENANCE_AGENCY_ID:
				if (resolve) return getMaintenance_agency_id();
				return basicGetMaintenance_agency_id();
			case CorePackage.SUBDOMAIN__SUBDOMAIN_ID:
				return getSubdomain_id();
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
			case CorePackage.SUBDOMAIN__CODE:
				setCode((String)newValue);
				return;
			case CorePackage.SUBDOMAIN__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case CorePackage.SUBDOMAIN__DOMAIN_ID:
				setDomain_id((DOMAIN)newValue);
				return;
			case CorePackage.SUBDOMAIN__FACET_ID:
				setFacet_id((FACET_COLLECTION)newValue);
				return;
			case CorePackage.SUBDOMAIN__IS_LISTED:
				setIs_listed((Boolean)newValue);
				return;
			case CorePackage.SUBDOMAIN__IS_NATURAL:
				setIs_natural((Boolean)newValue);
				return;
			case CorePackage.SUBDOMAIN__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)newValue);
				return;
			case CorePackage.SUBDOMAIN__SUBDOMAIN_ID:
				setSubdomain_id((String)newValue);
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
			case CorePackage.SUBDOMAIN__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case CorePackage.SUBDOMAIN__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case CorePackage.SUBDOMAIN__DOMAIN_ID:
				setDomain_id((DOMAIN)null);
				return;
			case CorePackage.SUBDOMAIN__FACET_ID:
				setFacet_id((FACET_COLLECTION)null);
				return;
			case CorePackage.SUBDOMAIN__IS_LISTED:
				setIs_listed(IS_LISTED_EDEFAULT);
				return;
			case CorePackage.SUBDOMAIN__IS_NATURAL:
				setIs_natural(IS_NATURAL_EDEFAULT);
				return;
			case CorePackage.SUBDOMAIN__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)null);
				return;
			case CorePackage.SUBDOMAIN__SUBDOMAIN_ID:
				setSubdomain_id(SUBDOMAIN_ID_EDEFAULT);
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
			case CorePackage.SUBDOMAIN__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case CorePackage.SUBDOMAIN__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case CorePackage.SUBDOMAIN__DOMAIN_ID:
				return domain_id != null;
			case CorePackage.SUBDOMAIN__FACET_ID:
				return facet_id != null;
			case CorePackage.SUBDOMAIN__IS_LISTED:
				return is_listed != IS_LISTED_EDEFAULT;
			case CorePackage.SUBDOMAIN__IS_NATURAL:
				return is_natural != IS_NATURAL_EDEFAULT;
			case CorePackage.SUBDOMAIN__MAINTENANCE_AGENCY_ID:
				return maintenance_agency_id != null;
			case CorePackage.SUBDOMAIN__SUBDOMAIN_ID:
				return SUBDOMAIN_ID_EDEFAULT == null ? subdomain_id != null : !SUBDOMAIN_ID_EDEFAULT.equals(subdomain_id);
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
		result.append(", is_listed: ");
		result.append(is_listed);
		result.append(", is_natural: ");
		result.append(is_natural);
		result.append(", subdomain_id: ");
		result.append(subdomain_id);
		result.append(')');
		return result.toString();
	}

} //SUBDOMAINImpl
