/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER;
import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM;
import org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>MEMBER MAPPING ITEM</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MEMBER_MAPPING_ITEMImpl#getIsSource <em>Is Source</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MEMBER_MAPPING_ITEMImpl#getMember <em>Member</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.impl.MEMBER_MAPPING_ITEMImpl#getVariable <em>Variable</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MEMBER_MAPPING_ITEMImpl extends MinimalEObjectImpl.Container implements MEMBER_MAPPING_ITEM
{
	/**
	 * The default value of the '{@link #getIsSource() <em>Is Source</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIsSource()
	 * @generated
	 * @ordered
	 */
	protected static final String IS_SOURCE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getIsSource() <em>Is Source</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIsSource()
	 * @generated
	 * @ordered
	 */
	protected String isSource = IS_SOURCE_EDEFAULT;

	/**
	 * The cached value of the '{@link #getMember() <em>Member</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMember()
	 * @generated
	 * @ordered
	 */
	protected MEMBER member;

	/**
	 * The cached value of the '{@link #getVariable() <em>Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE variable;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected MEMBER_MAPPING_ITEMImpl()
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
		return MappingPackage.Literals.MEMBER_MAPPING_ITEM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getIsSource()
	{
		return isSource;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setIsSource(String newIsSource)
	{
		String oldIsSource = isSource;
		isSource = newIsSource;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MEMBER_MAPPING_ITEM__IS_SOURCE, oldIsSource, isSource));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MEMBER getMember()
	{
		if (member != null && member.eIsProxy()) {
			InternalEObject oldMember = (InternalEObject)member;
			member = (MEMBER)eResolveProxy(oldMember);
			if (member != oldMember) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, MappingPackage.MEMBER_MAPPING_ITEM__MEMBER, oldMember, member));
			}
		}
		return member;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER basicGetMember()
	{
		return member;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMember(MEMBER newMember)
	{
		MEMBER oldMember = member;
		member = newMember;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MEMBER_MAPPING_ITEM__MEMBER, oldMember, member));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getVariable()
	{
		if (variable != null && variable.eIsProxy()) {
			InternalEObject oldVariable = (InternalEObject)variable;
			variable = (VARIABLE)eResolveProxy(oldVariable);
			if (variable != oldVariable) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, MappingPackage.MEMBER_MAPPING_ITEM__VARIABLE, oldVariable, variable));
			}
		}
		return variable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetVariable()
	{
		return variable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setVariable(VARIABLE newVariable)
	{
		VARIABLE oldVariable = variable;
		variable = newVariable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, MappingPackage.MEMBER_MAPPING_ITEM__VARIABLE, oldVariable, variable));
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
			case MappingPackage.MEMBER_MAPPING_ITEM__IS_SOURCE:
				return getIsSource();
			case MappingPackage.MEMBER_MAPPING_ITEM__MEMBER:
				if (resolve) return getMember();
				return basicGetMember();
			case MappingPackage.MEMBER_MAPPING_ITEM__VARIABLE:
				if (resolve) return getVariable();
				return basicGetVariable();
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
			case MappingPackage.MEMBER_MAPPING_ITEM__IS_SOURCE:
				setIsSource((String)newValue);
				return;
			case MappingPackage.MEMBER_MAPPING_ITEM__MEMBER:
				setMember((MEMBER)newValue);
				return;
			case MappingPackage.MEMBER_MAPPING_ITEM__VARIABLE:
				setVariable((VARIABLE)newValue);
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
			case MappingPackage.MEMBER_MAPPING_ITEM__IS_SOURCE:
				setIsSource(IS_SOURCE_EDEFAULT);
				return;
			case MappingPackage.MEMBER_MAPPING_ITEM__MEMBER:
				setMember((MEMBER)null);
				return;
			case MappingPackage.MEMBER_MAPPING_ITEM__VARIABLE:
				setVariable((VARIABLE)null);
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
			case MappingPackage.MEMBER_MAPPING_ITEM__IS_SOURCE:
				return IS_SOURCE_EDEFAULT == null ? isSource != null : !IS_SOURCE_EDEFAULT.equals(isSource);
			case MappingPackage.MEMBER_MAPPING_ITEM__MEMBER:
				return member != null;
			case MappingPackage.MEMBER_MAPPING_ITEM__VARIABLE:
				return variable != null;
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
		result.append(" (isSource: ");
		result.append(isSource);
		result.append(')');
		return result.toString();
	}

} //MEMBER_MAPPING_ITEMImpl
