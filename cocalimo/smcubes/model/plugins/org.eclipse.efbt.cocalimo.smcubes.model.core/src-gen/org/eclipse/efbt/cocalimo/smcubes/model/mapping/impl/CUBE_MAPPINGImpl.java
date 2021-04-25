/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING;
import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_TO_CUBE;
import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>CUBE MAPPING</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.CUBE_MAPPINGImpl#getCube_mapping_id <em>Cube mapping id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.CUBE_MAPPINGImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.CUBE_MAPPINGImpl#getSource_cube <em>Source cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.CUBE_MAPPINGImpl#getTarget_cube <em>Target cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.CUBE_MAPPINGImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.CUBE_MAPPINGImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.CUBE_MAPPINGImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.CUBE_MAPPINGImpl#getMappingsToCube <em>Mappings To Cube</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CUBE_MAPPINGImpl extends MinimalEObjectImpl.Container implements CUBE_MAPPING
{
	/**
	 * The default value of the '{@link #getCube_mapping_id() <em>Cube mapping id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_mapping_id()
	 * @generated
	 * @ordered
	 */
	protected static final String CUBE_MAPPING_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getCube_mapping_id() <em>Cube mapping id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_mapping_id()
	 * @generated
	 * @ordered
	 */
	protected String cube_mapping_id = CUBE_MAPPING_ID_EDEFAULT;

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
	 * The cached value of the '{@link #getSource_cube() <em>Source cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSource_cube()
	 * @generated
	 * @ordered
	 */
	protected CUBE source_cube;

	/**
	 * The cached value of the '{@link #getTarget_cube() <em>Target cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTarget_cube()
	 * @generated
	 * @ordered
	 */
	protected CUBE target_cube;

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
	 * The cached value of the '{@link #getMappingsToCube() <em>Mappings To Cube</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMappingsToCube()
	 * @generated
	 * @ordered
	 */
	protected EList<MAPPING_TO_CUBE> mappingsToCube;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CUBE_MAPPINGImpl()
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
		return MappingPackage.Literals.CUBE_MAPPING;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getCube_mapping_id()
	{
		return cube_mapping_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube_mapping_id(String newCube_mapping_id)
	{
		String oldCube_mapping_id = cube_mapping_id;
		cube_mapping_id = newCube_mapping_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.CUBE_MAPPING__CUBE_MAPPING_ID, oldCube_mapping_id, cube_mapping_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.CUBE_MAPPING__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE getSource_cube()
	{
		if (source_cube != null && source_cube.eIsProxy()) {
			InternalEObject oldSource_cube = (InternalEObject)source_cube;
			source_cube = (CUBE)eResolveProxy(oldSource_cube);
			if (source_cube != oldSource_cube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, MappingPackage.CUBE_MAPPING__SOURCE_CUBE, oldSource_cube, source_cube));
			}
		}
		return source_cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE basicGetSource_cube()
	{
		return source_cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSource_cube(CUBE newSource_cube)
	{
		CUBE oldSource_cube = source_cube;
		source_cube = newSource_cube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.CUBE_MAPPING__SOURCE_CUBE, oldSource_cube, source_cube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE getTarget_cube()
	{
		if (target_cube != null && target_cube.eIsProxy()) {
			InternalEObject oldTarget_cube = (InternalEObject)target_cube;
			target_cube = (CUBE)eResolveProxy(oldTarget_cube);
			if (target_cube != oldTarget_cube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, MappingPackage.CUBE_MAPPING__TARGET_CUBE, oldTarget_cube, target_cube));
			}
		}
		return target_cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE basicGetTarget_cube()
	{
		return target_cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTarget_cube(CUBE newTarget_cube)
	{
		CUBE oldTarget_cube = target_cube;
		target_cube = newTarget_cube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.CUBE_MAPPING__TARGET_CUBE, oldTarget_cube, target_cube));
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
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.CUBE_MAPPING__NAME, oldName, name));
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
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.CUBE_MAPPING__CODE, oldCode, code));
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
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.CUBE_MAPPING__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<MAPPING_TO_CUBE> getMappingsToCube()
	{
		if (mappingsToCube == null) {
			mappingsToCube = new EObjectContainmentEList<MAPPING_TO_CUBE>(MAPPING_TO_CUBE.class, this, MappingPackage.CUBE_MAPPING__MAPPINGS_TO_CUBE);
		}
		return mappingsToCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case MappingPackage.CUBE_MAPPING__MAPPINGS_TO_CUBE:
				return ((InternalEList<?>)getMappingsToCube()).basicRemove(otherEnd, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
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
			case MappingPackage.CUBE_MAPPING__CUBE_MAPPING_ID:
				return getCube_mapping_id();
			case MappingPackage.CUBE_MAPPING__DESCRIPTION:
				return getDescription();
			case MappingPackage.CUBE_MAPPING__SOURCE_CUBE:
				if (resolve) return getSource_cube();
				return basicGetSource_cube();
			case MappingPackage.CUBE_MAPPING__TARGET_CUBE:
				if (resolve) return getTarget_cube();
				return basicGetTarget_cube();
			case MappingPackage.CUBE_MAPPING__NAME:
				return getName();
			case MappingPackage.CUBE_MAPPING__CODE:
				return getCode();
			case MappingPackage.CUBE_MAPPING__MAINTENANCE_AGENCY_ID:
				return getMaintenance_agency_id();
			case MappingPackage.CUBE_MAPPING__MAPPINGS_TO_CUBE:
				return getMappingsToCube();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case MappingPackage.CUBE_MAPPING__CUBE_MAPPING_ID:
				setCube_mapping_id((String)newValue);
				return;
			case MappingPackage.CUBE_MAPPING__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case MappingPackage.CUBE_MAPPING__SOURCE_CUBE:
				setSource_cube((CUBE)newValue);
				return;
			case MappingPackage.CUBE_MAPPING__TARGET_CUBE:
				setTarget_cube((CUBE)newValue);
				return;
			case MappingPackage.CUBE_MAPPING__NAME:
				setName((String)newValue);
				return;
			case MappingPackage.CUBE_MAPPING__CODE:
				setCode((String)newValue);
				return;
			case MappingPackage.CUBE_MAPPING__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((String)newValue);
				return;
			case MappingPackage.CUBE_MAPPING__MAPPINGS_TO_CUBE:
				getMappingsToCube().clear();
				getMappingsToCube().addAll((Collection<? extends MAPPING_TO_CUBE>)newValue);
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
			case MappingPackage.CUBE_MAPPING__CUBE_MAPPING_ID:
				setCube_mapping_id(CUBE_MAPPING_ID_EDEFAULT);
				return;
			case MappingPackage.CUBE_MAPPING__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case MappingPackage.CUBE_MAPPING__SOURCE_CUBE:
				setSource_cube((CUBE)null);
				return;
			case MappingPackage.CUBE_MAPPING__TARGET_CUBE:
				setTarget_cube((CUBE)null);
				return;
			case MappingPackage.CUBE_MAPPING__NAME:
				setName(NAME_EDEFAULT);
				return;
			case MappingPackage.CUBE_MAPPING__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case MappingPackage.CUBE_MAPPING__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id(MAINTENANCE_AGENCY_ID_EDEFAULT);
				return;
			case MappingPackage.CUBE_MAPPING__MAPPINGS_TO_CUBE:
				getMappingsToCube().clear();
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
			case MappingPackage.CUBE_MAPPING__CUBE_MAPPING_ID:
				return CUBE_MAPPING_ID_EDEFAULT == null ? cube_mapping_id != null : !CUBE_MAPPING_ID_EDEFAULT.equals(cube_mapping_id);
			case MappingPackage.CUBE_MAPPING__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case MappingPackage.CUBE_MAPPING__SOURCE_CUBE:
				return source_cube != null;
			case MappingPackage.CUBE_MAPPING__TARGET_CUBE:
				return target_cube != null;
			case MappingPackage.CUBE_MAPPING__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case MappingPackage.CUBE_MAPPING__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case MappingPackage.CUBE_MAPPING__MAINTENANCE_AGENCY_ID:
				return MAINTENANCE_AGENCY_ID_EDEFAULT == null ? maintenance_agency_id != null : !MAINTENANCE_AGENCY_ID_EDEFAULT.equals(maintenance_agency_id);
			case MappingPackage.CUBE_MAPPING__MAPPINGS_TO_CUBE:
				return mappingsToCube != null && !mappingsToCube.isEmpty();
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
		result.append(" (cube_mapping_id: ");
		result.append(cube_mapping_id);
		result.append(", description: ");
		result.append(description);
		result.append(", name: ");
		result.append(name);
		result.append(", code: ");
		result.append(code);
		result.append(", maintenance_agency_id: ");
		result.append(maintenance_agency_id);
		result.append(')');
		return result.toString();
	}

} //CUBE_MAPPINGImpl
