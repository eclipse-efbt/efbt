/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MAINTENANCE_AGENCY;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_GROUP;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>CUBE GROUP</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUPImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUPImpl#getCube_group_id <em>Cube group id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUPImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUPImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_GROUPImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CUBE_GROUPImpl extends MinimalEObjectImpl.Container implements CUBE_GROUP
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
	 * The default value of the '{@link #getCube_group_id() <em>Cube group id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_group_id()
	 * @generated
	 * @ordered
	 */
	protected static final String CUBE_GROUP_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getCube_group_id() <em>Cube group id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_group_id()
	 * @generated
	 * @ordered
	 */
	protected String cube_group_id = CUBE_GROUP_ID_EDEFAULT;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CUBE_GROUPImpl()
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
		return Data_definitionPackage.Literals.CUBE_GROUP;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_GROUP__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getCube_group_id()
	{
		return cube_group_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube_group_id(String newCube_group_id)
	{
		String oldCube_group_id = cube_group_id;
		cube_group_id = newCube_group_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_GROUP__CUBE_GROUP_ID, oldCube_group_id, cube_group_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_GROUP__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MAINTENANCE_AGENCY getMaintenance_agency_id()
	{
		if (maintenance_agency_id != null && maintenance_agency_id.eIsProxy()) {
			InternalEObject oldMaintenance_agency_id = (InternalEObject)maintenance_agency_id;
			maintenance_agency_id = (MAINTENANCE_AGENCY)eResolveProxy(oldMaintenance_agency_id);
			if (maintenance_agency_id != oldMaintenance_agency_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_GROUP__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_GROUP__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_GROUP__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID) {
			case Data_definitionPackage.CUBE_GROUP__CODE:
				return getCode();
			case Data_definitionPackage.CUBE_GROUP__CUBE_GROUP_ID:
				return getCube_group_id();
			case Data_definitionPackage.CUBE_GROUP__DESCRIPTION:
				return getDescription();
			case Data_definitionPackage.CUBE_GROUP__MAINTENANCE_AGENCY_ID:
				if (resolve) return getMaintenance_agency_id();
				return basicGetMaintenance_agency_id();
			case Data_definitionPackage.CUBE_GROUP__NAME:
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
		switch (featureID) {
			case Data_definitionPackage.CUBE_GROUP__CODE:
				setCode((String)newValue);
				return;
			case Data_definitionPackage.CUBE_GROUP__CUBE_GROUP_ID:
				setCube_group_id((String)newValue);
				return;
			case Data_definitionPackage.CUBE_GROUP__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case Data_definitionPackage.CUBE_GROUP__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)newValue);
				return;
			case Data_definitionPackage.CUBE_GROUP__NAME:
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
		switch (featureID) {
			case Data_definitionPackage.CUBE_GROUP__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_GROUP__CUBE_GROUP_ID:
				setCube_group_id(CUBE_GROUP_ID_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_GROUP__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_GROUP__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((MAINTENANCE_AGENCY)null);
				return;
			case Data_definitionPackage.CUBE_GROUP__NAME:
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
		switch (featureID) {
			case Data_definitionPackage.CUBE_GROUP__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case Data_definitionPackage.CUBE_GROUP__CUBE_GROUP_ID:
				return CUBE_GROUP_ID_EDEFAULT == null ? cube_group_id != null : !CUBE_GROUP_ID_EDEFAULT.equals(cube_group_id);
			case Data_definitionPackage.CUBE_GROUP__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case Data_definitionPackage.CUBE_GROUP__MAINTENANCE_AGENCY_ID:
				return maintenance_agency_id != null;
			case Data_definitionPackage.CUBE_GROUP__NAME:
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
		result.append(", cube_group_id: ");
		result.append(cube_group_id);
		result.append(", description: ");
		result.append(description);
		result.append(", name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //CUBE_GROUPImpl
