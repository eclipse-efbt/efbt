/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION;
import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING;
import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>MAPPING DEFINITION</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_DEFINITIONImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_DEFINITIONImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_DEFINITIONImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_DEFINITIONImpl#getMapping_id <em>Mapping id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_DEFINITIONImpl#getMapping_type <em>Mapping type</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_DEFINITIONImpl#getMemberMapping <em>Member Mapping</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MAPPING_DEFINITIONImpl#getVariableMapping <em>Variable Mapping</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MAPPING_DEFINITIONImpl extends MinimalEObjectImpl.Container implements MAPPING_DEFINITION
{
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
	 * The default value of the '{@link #getMapping_id() <em>Mapping id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMapping_id()
	 * @generated
	 * @ordered
	 */
	protected static final String MAPPING_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getMapping_id() <em>Mapping id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMapping_id()
	 * @generated
	 * @ordered
	 */
	protected String mapping_id = MAPPING_ID_EDEFAULT;

	/**
	 * The default value of the '{@link #getMapping_type() <em>Mapping type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMapping_type()
	 * @generated
	 * @ordered
	 */
	protected static final String MAPPING_TYPE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getMapping_type() <em>Mapping type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMapping_type()
	 * @generated
	 * @ordered
	 */
	protected String mapping_type = MAPPING_TYPE_EDEFAULT;

	/**
	 * The cached value of the '{@link #getMemberMapping() <em>Member Mapping</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMemberMapping()
	 * @generated
	 * @ordered
	 */
	protected MEMBER_MAPPING memberMapping;

	/**
	 * The cached value of the '{@link #getVariableMapping() <em>Variable Mapping</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariableMapping()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE_MAPPING variableMapping;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected MAPPING_DEFINITIONImpl()
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
		return MappingPackage.Literals.MAPPING_DEFINITION;
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
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MAPPING_DEFINITION__NAME, oldName, name));
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
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MAPPING_DEFINITION__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getMaintenance_agency_id()
	{
		return maintenance_agency_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMaintenance_agency_id(String newMaintenance_agency_id)
	{
		String oldMaintenance_agency_id = maintenance_agency_id;
		maintenance_agency_id = newMaintenance_agency_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MAPPING_DEFINITION__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getMapping_id()
	{
		return mapping_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMapping_id(String newMapping_id)
	{
		String oldMapping_id = mapping_id;
		mapping_id = newMapping_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MAPPING_DEFINITION__MAPPING_ID, oldMapping_id, mapping_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getMapping_type()
	{
		return mapping_type;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMapping_type(String newMapping_type)
	{
		String oldMapping_type = mapping_type;
		mapping_type = newMapping_type;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MAPPING_DEFINITION__MAPPING_TYPE, oldMapping_type, mapping_type));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MEMBER_MAPPING getMemberMapping()
	{
		if (memberMapping != null && memberMapping.eIsProxy()) {
			InternalEObject oldMemberMapping = (InternalEObject)memberMapping;
			memberMapping = (MEMBER_MAPPING)eResolveProxy(oldMemberMapping);
			if (memberMapping != oldMemberMapping) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, MappingPackage.MAPPING_DEFINITION__MEMBER_MAPPING, oldMemberMapping, memberMapping));
			}
		}
		return memberMapping;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER_MAPPING basicGetMemberMapping()
	{
		return memberMapping;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMemberMapping(MEMBER_MAPPING newMemberMapping)
	{
		MEMBER_MAPPING oldMemberMapping = memberMapping;
		memberMapping = newMemberMapping;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MAPPING_DEFINITION__MEMBER_MAPPING, oldMemberMapping, memberMapping));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE_MAPPING getVariableMapping()
	{
		if (variableMapping != null && variableMapping.eIsProxy()) {
			InternalEObject oldVariableMapping = (InternalEObject)variableMapping;
			variableMapping = (VARIABLE_MAPPING)eResolveProxy(oldVariableMapping);
			if (variableMapping != oldVariableMapping) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, MappingPackage.MAPPING_DEFINITION__VARIABLE_MAPPING, oldVariableMapping, variableMapping));
			}
		}
		return variableMapping;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE_MAPPING basicGetVariableMapping()
	{
		return variableMapping;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setVariableMapping(VARIABLE_MAPPING newVariableMapping)
	{
		VARIABLE_MAPPING oldVariableMapping = variableMapping;
		variableMapping = newVariableMapping;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MAPPING_DEFINITION__VARIABLE_MAPPING, oldVariableMapping, variableMapping));
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
			case MappingPackage.MAPPING_DEFINITION__NAME:
				return getName();
			case MappingPackage.MAPPING_DEFINITION__CODE:
				return getCode();
			case MappingPackage.MAPPING_DEFINITION__MAINTENANCE_AGENCY_ID:
				return getMaintenance_agency_id();
			case MappingPackage.MAPPING_DEFINITION__MAPPING_ID:
				return getMapping_id();
			case MappingPackage.MAPPING_DEFINITION__MAPPING_TYPE:
				return getMapping_type();
			case MappingPackage.MAPPING_DEFINITION__MEMBER_MAPPING:
				if (resolve) return getMemberMapping();
				return basicGetMemberMapping();
			case MappingPackage.MAPPING_DEFINITION__VARIABLE_MAPPING:
				if (resolve) return getVariableMapping();
				return basicGetVariableMapping();
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
			case MappingPackage.MAPPING_DEFINITION__NAME:
				setName((String)newValue);
				return;
			case MappingPackage.MAPPING_DEFINITION__CODE:
				setCode((String)newValue);
				return;
			case MappingPackage.MAPPING_DEFINITION__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((String)newValue);
				return;
			case MappingPackage.MAPPING_DEFINITION__MAPPING_ID:
				setMapping_id((String)newValue);
				return;
			case MappingPackage.MAPPING_DEFINITION__MAPPING_TYPE:
				setMapping_type((String)newValue);
				return;
			case MappingPackage.MAPPING_DEFINITION__MEMBER_MAPPING:
				setMemberMapping((MEMBER_MAPPING)newValue);
				return;
			case MappingPackage.MAPPING_DEFINITION__VARIABLE_MAPPING:
				setVariableMapping((VARIABLE_MAPPING)newValue);
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
			case MappingPackage.MAPPING_DEFINITION__NAME:
				setName(NAME_EDEFAULT);
				return;
			case MappingPackage.MAPPING_DEFINITION__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case MappingPackage.MAPPING_DEFINITION__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id(MAINTENANCE_AGENCY_ID_EDEFAULT);
				return;
			case MappingPackage.MAPPING_DEFINITION__MAPPING_ID:
				setMapping_id(MAPPING_ID_EDEFAULT);
				return;
			case MappingPackage.MAPPING_DEFINITION__MAPPING_TYPE:
				setMapping_type(MAPPING_TYPE_EDEFAULT);
				return;
			case MappingPackage.MAPPING_DEFINITION__MEMBER_MAPPING:
				setMemberMapping((MEMBER_MAPPING)null);
				return;
			case MappingPackage.MAPPING_DEFINITION__VARIABLE_MAPPING:
				setVariableMapping((VARIABLE_MAPPING)null);
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
			case MappingPackage.MAPPING_DEFINITION__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case MappingPackage.MAPPING_DEFINITION__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case MappingPackage.MAPPING_DEFINITION__MAINTENANCE_AGENCY_ID:
				return MAINTENANCE_AGENCY_ID_EDEFAULT == null ? maintenance_agency_id != null : !MAINTENANCE_AGENCY_ID_EDEFAULT.equals(maintenance_agency_id);
			case MappingPackage.MAPPING_DEFINITION__MAPPING_ID:
				return MAPPING_ID_EDEFAULT == null ? mapping_id != null : !MAPPING_ID_EDEFAULT.equals(mapping_id);
			case MappingPackage.MAPPING_DEFINITION__MAPPING_TYPE:
				return MAPPING_TYPE_EDEFAULT == null ? mapping_type != null : !MAPPING_TYPE_EDEFAULT.equals(mapping_type);
			case MappingPackage.MAPPING_DEFINITION__MEMBER_MAPPING:
				return memberMapping != null;
			case MappingPackage.MAPPING_DEFINITION__VARIABLE_MAPPING:
				return variableMapping != null;
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
		result.append(" (name: ");
		result.append(name);
		result.append(", code: ");
		result.append(code);
		result.append(", maintenance_agency_id: ");
		result.append(maintenance_agency_id);
		result.append(", mapping_id: ");
		result.append(mapping_id);
		result.append(", mapping_type: ");
		result.append(mapping_type);
		result.append(')');
		return result.toString();
	}

} //MAPPING_DEFINITIONImpl
