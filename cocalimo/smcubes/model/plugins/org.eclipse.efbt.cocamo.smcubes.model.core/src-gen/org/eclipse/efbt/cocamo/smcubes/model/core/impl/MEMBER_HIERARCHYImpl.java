/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.core.impl;

import org.eclipse.efbt.cocamo.smcubes.model.core.CorePackage;
import org.eclipse.efbt.cocamo.smcubes.model.core.DOMAIN;
import org.eclipse.efbt.cocamo.smcubes.model.core.MAINTENANCE_AGENCY;
import org.eclipse.efbt.cocamo.smcubes.model.core.MEMBER_HIERARCHY;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>MEMBER HIERARCHY</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.core.impl.MEMBER_HIERARCHYImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.core.impl.MEMBER_HIERARCHYImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.core.impl.MEMBER_HIERARCHYImpl#getDomain_id <em>Domain id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.core.impl.MEMBER_HIERARCHYImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.core.impl.MEMBER_HIERARCHYImpl#getMember_hierarchy_id <em>Member hierarchy id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.core.impl.MEMBER_HIERARCHYImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MEMBER_HIERARCHYImpl extends MinimalEObjectImpl.Container implements MEMBER_HIERARCHY
{
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
	 * The default value of the '{@link #getMember_hierarchy_id() <em>Member hierarchy id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMember_hierarchy_id()
	 * @generated
	 * @ordered
	 */
	protected static final String MEMBER_HIERARCHY_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getMember_hierarchy_id() <em>Member hierarchy id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMember_hierarchy_id()
	 * @generated
	 * @ordered
	 */
	protected String member_hierarchy_id = MEMBER_HIERARCHY_ID_EDEFAULT;

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
	protected MEMBER_HIERARCHYImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass()
	{
		return CorePackage.Literals.MEMBER_HIERARCHY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getCode()
	{
		return code;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCode(String newCode)
	{
		String oldCode = code;
		code = newCode;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getDescription()
	{
		return description;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setDescription(String newDescription)
	{
		String oldDescription = description;
		description = newDescription;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public DOMAIN getDomain_id()
	{
		if (domain_id != null && domain_id.eIsProxy())
		{
			InternalEObject oldDomain_id = (InternalEObject)domain_id;
			domain_id = (DOMAIN)eResolveProxy(oldDomain_id);
			if (domain_id != oldDomain_id)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CorePackage.MEMBER_HIERARCHY__DOMAIN_ID, oldDomain_id, domain_id));
			}
		}
		return domain_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DOMAIN basicGetDomain_id()
	{
		return domain_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setDomain_id(DOMAIN newDomain_id)
	{
		DOMAIN oldDomain_id = domain_id;
		domain_id = newDomain_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY__DOMAIN_ID, oldDomain_id, domain_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MAINTENANCE_AGENCY getMaintenance_agency_id()
	{
		if (maintenance_agency_id != null && maintenance_agency_id.eIsProxy())
		{
			InternalEObject oldMaintenance_agency_id = (InternalEObject)maintenance_agency_id;
			maintenance_agency_id = (MAINTENANCE_AGENCY)eResolveProxy(oldMaintenance_agency_id);
			if (maintenance_agency_id != oldMaintenance_agency_id)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CorePackage.MEMBER_HIERARCHY__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
			}
		}
		return maintenance_agency_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MAINTENANCE_AGENCY basicGetMaintenance_agency_id()
	{
		return maintenance_agency_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMaintenance_agency_id(MAINTENANCE_AGENCY newMaintenance_agency_id)
	{
		MAINTENANCE_AGENCY oldMaintenance_agency_id = maintenance_agency_id;
		maintenance_agency_id = newMaintenance_agency_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getMember_hierarchy_id()
	{
		return member_hierarchy_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMember_hierarchy_id(String newMember_hierarchy_id)
	{
		String oldMember_hierarchy_id = member_hierarchy_id;
		member_hierarchy_id = newMember_hierarchy_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY__MEMBER_HIERARCHY_ID, oldMember_hierarchy_id, member_hierarchy_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName()
	{
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName)
	{
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID)
		{
			case CorePackage.MEMBER_HIERARCHY__CODE:
				return getCode();
			case CorePackage.MEMBER_HIERARCHY__DESCRIPTION:
				return getDescription();
			case CorePackage.MEMBER_HIERARCHY__DOMAIN_ID:
				if (resolve) return getDomain_id();
				return basicGetDomain_id();
			case CorePackage.MEMBER_HIERARCHY__MAINTENANCE_AGENCY_ID:
				if (resolve) return getMaintenance_agency_id();
				return basicGetMaintenance_agency_id();
			case CorePackage.MEMBER_HIERARCHY__MEMBER_HIERARCHY_ID:
				return getMember_hierarchy_id();
			case CorePackage.MEMBER_HIERARCHY__NAME:
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
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID)
		{
			case CorePackage.MEMBER_HIERARCHY__CODE:
				setCode((String)newValue);
				return;
			case CorePackage.MEMBER_HIERARCHY__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case CorePackage.MEMBER_HIERARCHY__DOMAIN_ID:
				setDomain_id((DOMAIN)newValue);
				return;
			case CorePackage.MEMBER_HIERARCHY__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)newValue);
				return;
			case CorePackage.MEMBER_HIERARCHY__MEMBER_HIERARCHY_ID:
				setMember_hierarchy_id((String)newValue);
				return;
			case CorePackage.MEMBER_HIERARCHY__NAME:
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
	public void eUnset(int featureID)
	{
		switch (featureID)
		{
			case CorePackage.MEMBER_HIERARCHY__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case CorePackage.MEMBER_HIERARCHY__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case CorePackage.MEMBER_HIERARCHY__DOMAIN_ID:
				setDomain_id((DOMAIN)null);
				return;
			case CorePackage.MEMBER_HIERARCHY__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)null);
				return;
			case CorePackage.MEMBER_HIERARCHY__MEMBER_HIERARCHY_ID:
				setMember_hierarchy_id(MEMBER_HIERARCHY_ID_EDEFAULT);
				return;
			case CorePackage.MEMBER_HIERARCHY__NAME:
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
	public boolean eIsSet(int featureID)
	{
		switch (featureID)
		{
			case CorePackage.MEMBER_HIERARCHY__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case CorePackage.MEMBER_HIERARCHY__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case CorePackage.MEMBER_HIERARCHY__DOMAIN_ID:
				return domain_id != null;
			case CorePackage.MEMBER_HIERARCHY__MAINTENANCE_AGENCY_ID:
				return maintenance_agency_id != null;
			case CorePackage.MEMBER_HIERARCHY__MEMBER_HIERARCHY_ID:
				return MEMBER_HIERARCHY_ID_EDEFAULT == null ? member_hierarchy_id != null : !MEMBER_HIERARCHY_ID_EDEFAULT.equals(member_hierarchy_id);
			case CorePackage.MEMBER_HIERARCHY__NAME:
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
	public String toString()
	{
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (code: ");
		result.append(code);
		result.append(", description: ");
		result.append(description);
		result.append(", member_hierarchy_id: ");
		result.append(member_hierarchy_id);
		result.append(", name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //MEMBER_HIERARCHYImpl
