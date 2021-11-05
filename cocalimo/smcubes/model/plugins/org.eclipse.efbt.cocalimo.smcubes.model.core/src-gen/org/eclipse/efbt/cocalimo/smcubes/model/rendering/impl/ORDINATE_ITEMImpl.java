/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER;
import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER_HIERARCHY;
import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE;
import org.eclipse.efbt.cocalimo.smcubes.model.rendering.ORDINATE_ITEM;
import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>ORDINATE ITEM</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.ORDINATE_ITEMImpl#getAxis_ordinate_id <em>Axis ordinate id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.ORDINATE_ITEMImpl#getIs_starting_member_included <em>Is starting member included</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.ORDINATE_ITEMImpl#getMember_hierarchy_id <em>Member hierarchy id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.ORDINATE_ITEMImpl#getMember_id <em>Member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.ORDINATE_ITEMImpl#getStarting_member_id <em>Starting member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.ORDINATE_ITEMImpl#getVariable_id <em>Variable id</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ORDINATE_ITEMImpl extends MinimalEObjectImpl.Container implements ORDINATE_ITEM {
	/**
	 * The cached value of the '{@link #getAxis_ordinate_id() <em>Axis ordinate id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAxis_ordinate_id()
	 * @generated
	 * @ordered
	 */
	protected AXIS_ORDINATE axis_ordinate_id;

	/**
	 * The default value of the '{@link #getIs_starting_member_included() <em>Is starting member included</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIs_starting_member_included()
	 * @generated
	 * @ordered
	 */
	protected static final Boolean IS_STARTING_MEMBER_INCLUDED_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getIs_starting_member_included() <em>Is starting member included</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIs_starting_member_included()
	 * @generated
	 * @ordered
	 */
	protected Boolean is_starting_member_included = IS_STARTING_MEMBER_INCLUDED_EDEFAULT;

	/**
	 * The cached value of the '{@link #getMember_hierarchy_id() <em>Member hierarchy id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMember_hierarchy_id()
	 * @generated
	 * @ordered
	 */
	protected MEMBER_HIERARCHY member_hierarchy_id;

	/**
	 * The cached value of the '{@link #getMember_id() <em>Member id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMember_id()
	 * @generated
	 * @ordered
	 */
	protected MEMBER member_id;

	/**
	 * The cached value of the '{@link #getStarting_member_id() <em>Starting member id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getStarting_member_id()
	 * @generated
	 * @ordered
	 */
	protected MEMBER starting_member_id;

	/**
	 * The cached value of the '{@link #getVariable_id() <em>Variable id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable_id()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE variable_id;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ORDINATE_ITEMImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return RenderingPackage.Literals.ORDINATE_ITEM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AXIS_ORDINATE getAxis_ordinate_id() {
		if (axis_ordinate_id != null && axis_ordinate_id.eIsProxy()) {
			InternalEObject oldAxis_ordinate_id = (InternalEObject)axis_ordinate_id;
			axis_ordinate_id = (AXIS_ORDINATE)eResolveProxy(oldAxis_ordinate_id);
			if (axis_ordinate_id != oldAxis_ordinate_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, RenderingPackage.ORDINATE_ITEM__AXIS_ORDINATE_ID, oldAxis_ordinate_id, axis_ordinate_id));
			}
		}
		return axis_ordinate_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AXIS_ORDINATE basicGetAxis_ordinate_id() {
		return axis_ordinate_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setAxis_ordinate_id(AXIS_ORDINATE newAxis_ordinate_id) {
		AXIS_ORDINATE oldAxis_ordinate_id = axis_ordinate_id;
		axis_ordinate_id = newAxis_ordinate_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.ORDINATE_ITEM__AXIS_ORDINATE_ID, oldAxis_ordinate_id, axis_ordinate_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Boolean getIs_starting_member_included() {
		return is_starting_member_included;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setIs_starting_member_included(Boolean newIs_starting_member_included) {
		Boolean oldIs_starting_member_included = is_starting_member_included;
		is_starting_member_included = newIs_starting_member_included;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.ORDINATE_ITEM__IS_STARTING_MEMBER_INCLUDED, oldIs_starting_member_included, is_starting_member_included));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MEMBER_HIERARCHY getMember_hierarchy_id() {
		if (member_hierarchy_id != null && member_hierarchy_id.eIsProxy()) {
			InternalEObject oldMember_hierarchy_id = (InternalEObject)member_hierarchy_id;
			member_hierarchy_id = (MEMBER_HIERARCHY)eResolveProxy(oldMember_hierarchy_id);
			if (member_hierarchy_id != oldMember_hierarchy_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, RenderingPackage.ORDINATE_ITEM__MEMBER_HIERARCHY_ID, oldMember_hierarchy_id, member_hierarchy_id));
			}
		}
		return member_hierarchy_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER_HIERARCHY basicGetMember_hierarchy_id() {
		return member_hierarchy_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMember_hierarchy_id(MEMBER_HIERARCHY newMember_hierarchy_id) {
		MEMBER_HIERARCHY oldMember_hierarchy_id = member_hierarchy_id;
		member_hierarchy_id = newMember_hierarchy_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.ORDINATE_ITEM__MEMBER_HIERARCHY_ID, oldMember_hierarchy_id, member_hierarchy_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MEMBER getMember_id() {
		if (member_id != null && member_id.eIsProxy()) {
			InternalEObject oldMember_id = (InternalEObject)member_id;
			member_id = (MEMBER)eResolveProxy(oldMember_id);
			if (member_id != oldMember_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, RenderingPackage.ORDINATE_ITEM__MEMBER_ID, oldMember_id, member_id));
			}
		}
		return member_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER basicGetMember_id() {
		return member_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMember_id(MEMBER newMember_id) {
		MEMBER oldMember_id = member_id;
		member_id = newMember_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.ORDINATE_ITEM__MEMBER_ID, oldMember_id, member_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MEMBER getStarting_member_id() {
		if (starting_member_id != null && starting_member_id.eIsProxy()) {
			InternalEObject oldStarting_member_id = (InternalEObject)starting_member_id;
			starting_member_id = (MEMBER)eResolveProxy(oldStarting_member_id);
			if (starting_member_id != oldStarting_member_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, RenderingPackage.ORDINATE_ITEM__STARTING_MEMBER_ID, oldStarting_member_id, starting_member_id));
			}
		}
		return starting_member_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER basicGetStarting_member_id() {
		return starting_member_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setStarting_member_id(MEMBER newStarting_member_id) {
		MEMBER oldStarting_member_id = starting_member_id;
		starting_member_id = newStarting_member_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.ORDINATE_ITEM__STARTING_MEMBER_ID, oldStarting_member_id, starting_member_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getVariable_id() {
		if (variable_id != null && variable_id.eIsProxy()) {
			InternalEObject oldVariable_id = (InternalEObject)variable_id;
			variable_id = (VARIABLE)eResolveProxy(oldVariable_id);
			if (variable_id != oldVariable_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, RenderingPackage.ORDINATE_ITEM__VARIABLE_ID, oldVariable_id, variable_id));
			}
		}
		return variable_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetVariable_id() {
		return variable_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setVariable_id(VARIABLE newVariable_id) {
		VARIABLE oldVariable_id = variable_id;
		variable_id = newVariable_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.ORDINATE_ITEM__VARIABLE_ID, oldVariable_id, variable_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case RenderingPackage.ORDINATE_ITEM__AXIS_ORDINATE_ID:
				if (resolve) return getAxis_ordinate_id();
				return basicGetAxis_ordinate_id();
			case RenderingPackage.ORDINATE_ITEM__IS_STARTING_MEMBER_INCLUDED:
				return getIs_starting_member_included();
			case RenderingPackage.ORDINATE_ITEM__MEMBER_HIERARCHY_ID:
				if (resolve) return getMember_hierarchy_id();
				return basicGetMember_hierarchy_id();
			case RenderingPackage.ORDINATE_ITEM__MEMBER_ID:
				if (resolve) return getMember_id();
				return basicGetMember_id();
			case RenderingPackage.ORDINATE_ITEM__STARTING_MEMBER_ID:
				if (resolve) return getStarting_member_id();
				return basicGetStarting_member_id();
			case RenderingPackage.ORDINATE_ITEM__VARIABLE_ID:
				if (resolve) return getVariable_id();
				return basicGetVariable_id();
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
			case RenderingPackage.ORDINATE_ITEM__AXIS_ORDINATE_ID:
				setAxis_ordinate_id((AXIS_ORDINATE)newValue);
				return;
			case RenderingPackage.ORDINATE_ITEM__IS_STARTING_MEMBER_INCLUDED:
				setIs_starting_member_included((Boolean)newValue);
				return;
			case RenderingPackage.ORDINATE_ITEM__MEMBER_HIERARCHY_ID:
				setMember_hierarchy_id((MEMBER_HIERARCHY)newValue);
				return;
			case RenderingPackage.ORDINATE_ITEM__MEMBER_ID:
				setMember_id((MEMBER)newValue);
				return;
			case RenderingPackage.ORDINATE_ITEM__STARTING_MEMBER_ID:
				setStarting_member_id((MEMBER)newValue);
				return;
			case RenderingPackage.ORDINATE_ITEM__VARIABLE_ID:
				setVariable_id((VARIABLE)newValue);
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
			case RenderingPackage.ORDINATE_ITEM__AXIS_ORDINATE_ID:
				setAxis_ordinate_id((AXIS_ORDINATE)null);
				return;
			case RenderingPackage.ORDINATE_ITEM__IS_STARTING_MEMBER_INCLUDED:
				setIs_starting_member_included(IS_STARTING_MEMBER_INCLUDED_EDEFAULT);
				return;
			case RenderingPackage.ORDINATE_ITEM__MEMBER_HIERARCHY_ID:
				setMember_hierarchy_id((MEMBER_HIERARCHY)null);
				return;
			case RenderingPackage.ORDINATE_ITEM__MEMBER_ID:
				setMember_id((MEMBER)null);
				return;
			case RenderingPackage.ORDINATE_ITEM__STARTING_MEMBER_ID:
				setStarting_member_id((MEMBER)null);
				return;
			case RenderingPackage.ORDINATE_ITEM__VARIABLE_ID:
				setVariable_id((VARIABLE)null);
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
			case RenderingPackage.ORDINATE_ITEM__AXIS_ORDINATE_ID:
				return axis_ordinate_id != null;
			case RenderingPackage.ORDINATE_ITEM__IS_STARTING_MEMBER_INCLUDED:
				return IS_STARTING_MEMBER_INCLUDED_EDEFAULT == null ? is_starting_member_included != null : !IS_STARTING_MEMBER_INCLUDED_EDEFAULT.equals(is_starting_member_included);
			case RenderingPackage.ORDINATE_ITEM__MEMBER_HIERARCHY_ID:
				return member_hierarchy_id != null;
			case RenderingPackage.ORDINATE_ITEM__MEMBER_ID:
				return member_id != null;
			case RenderingPackage.ORDINATE_ITEM__STARTING_MEMBER_ID:
				return starting_member_id != null;
			case RenderingPackage.ORDINATE_ITEM__VARIABLE_ID:
				return variable_id != null;
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
		result.append(" (is_starting_member_included: ");
		result.append(is_starting_member_included);
		result.append(')');
		return result.toString();
	}

} //ORDINATE_ITEMImpl
