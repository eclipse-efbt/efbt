/**
 */
package org.eclipse.efbt.openregspecs.model.core.impl;

import java.util.Date;

import org.eclipse.efbt.openregspecs.model.core.CorePackage;
import org.eclipse.efbt.openregspecs.model.core.MEMBER;
import org.eclipse.efbt.openregspecs.model.core.MEMBER_HIERARCHY;
import org.eclipse.efbt.openregspecs.model.core.MEMBER_HIERARCHY_NODE;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>MEMBER HIERARCHY NODE</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.MEMBER_HIERARCHY_NODEImpl#getComparator <em>Comparator</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.MEMBER_HIERARCHY_NODEImpl#getLevel <em>Level</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.MEMBER_HIERARCHY_NODEImpl#getMember_hierarchy_id <em>Member hierarchy id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.MEMBER_HIERARCHY_NODEImpl#getMember_id <em>Member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.MEMBER_HIERARCHY_NODEImpl#getOperator <em>Operator</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.MEMBER_HIERARCHY_NODEImpl#getParent_member_id <em>Parent member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.MEMBER_HIERARCHY_NODEImpl#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.MEMBER_HIERARCHY_NODEImpl#getValid_to <em>Valid to</em>}</li>
 * </ul>
 *
 * @generated
 */
public class MEMBER_HIERARCHY_NODEImpl extends MinimalEObjectImpl.Container implements MEMBER_HIERARCHY_NODE {
	/**
	 * The default value of the '{@link #getComparator() <em>Comparator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getComparator()
	 * @generated
	 * @ordered
	 */
	protected static final String COMPARATOR_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getComparator() <em>Comparator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getComparator()
	 * @generated
	 * @ordered
	 */
	protected String comparator = COMPARATOR_EDEFAULT;

	/**
	 * The default value of the '{@link #getLevel() <em>Level</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLevel()
	 * @generated
	 * @ordered
	 */
	protected static final int LEVEL_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getLevel() <em>Level</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLevel()
	 * @generated
	 * @ordered
	 */
	protected int level = LEVEL_EDEFAULT;

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
	 * The default value of the '{@link #getOperator() <em>Operator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOperator()
	 * @generated
	 * @ordered
	 */
	protected static final String OPERATOR_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getOperator() <em>Operator</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOperator()
	 * @generated
	 * @ordered
	 */
	protected String operator = OPERATOR_EDEFAULT;

	/**
	 * The cached value of the '{@link #getParent_member_id() <em>Parent member id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getParent_member_id()
	 * @generated
	 * @ordered
	 */
	protected MEMBER parent_member_id;

	/**
	 * The default value of the '{@link #getValid_from() <em>Valid from</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValid_from()
	 * @generated
	 * @ordered
	 */
	protected static final Date VALID_FROM_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getValid_from() <em>Valid from</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValid_from()
	 * @generated
	 * @ordered
	 */
	protected Date valid_from = VALID_FROM_EDEFAULT;

	/**
	 * The default value of the '{@link #getValid_to() <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValid_to()
	 * @generated
	 * @ordered
	 */
	protected static final Date VALID_TO_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getValid_to() <em>Valid to</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValid_to()
	 * @generated
	 * @ordered
	 */
	protected Date valid_to = VALID_TO_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected MEMBER_HIERARCHY_NODEImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return CorePackage.Literals.MEMBER_HIERARCHY_NODE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getComparator() {
		return comparator;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setComparator(String newComparator) {
		String oldComparator = comparator;
		comparator = newComparator;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY_NODE__COMPARATOR, oldComparator, comparator));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public int getLevel() {
		return level;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setLevel(int newLevel) {
		int oldLevel = level;
		level = newLevel;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY_NODE__LEVEL, oldLevel, level));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER_HIERARCHY getMember_hierarchy_id() {
		if (member_hierarchy_id != null && member_hierarchy_id.eIsProxy()) {
			InternalEObject oldMember_hierarchy_id = (InternalEObject)member_hierarchy_id;
			member_hierarchy_id = (MEMBER_HIERARCHY)eResolveProxy(oldMember_hierarchy_id);
			if (member_hierarchy_id != oldMember_hierarchy_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CorePackage.MEMBER_HIERARCHY_NODE__MEMBER_HIERARCHY_ID, oldMember_hierarchy_id, member_hierarchy_id));
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
	public void setMember_hierarchy_id(MEMBER_HIERARCHY newMember_hierarchy_id) {
		MEMBER_HIERARCHY oldMember_hierarchy_id = member_hierarchy_id;
		member_hierarchy_id = newMember_hierarchy_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY_NODE__MEMBER_HIERARCHY_ID, oldMember_hierarchy_id, member_hierarchy_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER getMember_id() {
		if (member_id != null && member_id.eIsProxy()) {
			InternalEObject oldMember_id = (InternalEObject)member_id;
			member_id = (MEMBER)eResolveProxy(oldMember_id);
			if (member_id != oldMember_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CorePackage.MEMBER_HIERARCHY_NODE__MEMBER_ID, oldMember_id, member_id));
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
	public void setMember_id(MEMBER newMember_id) {
		MEMBER oldMember_id = member_id;
		member_id = newMember_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY_NODE__MEMBER_ID, oldMember_id, member_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getOperator() {
		return operator;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOperator(String newOperator) {
		String oldOperator = operator;
		operator = newOperator;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY_NODE__OPERATOR, oldOperator, operator));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER getParent_member_id() {
		if (parent_member_id != null && parent_member_id.eIsProxy()) {
			InternalEObject oldParent_member_id = (InternalEObject)parent_member_id;
			parent_member_id = (MEMBER)eResolveProxy(oldParent_member_id);
			if (parent_member_id != oldParent_member_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CorePackage.MEMBER_HIERARCHY_NODE__PARENT_MEMBER_ID, oldParent_member_id, parent_member_id));
			}
		}
		return parent_member_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MEMBER basicGetParent_member_id() {
		return parent_member_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setParent_member_id(MEMBER newParent_member_id) {
		MEMBER oldParent_member_id = parent_member_id;
		parent_member_id = newParent_member_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY_NODE__PARENT_MEMBER_ID, oldParent_member_id, parent_member_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Date getValid_from() {
		return valid_from;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setValid_from(Date newValid_from) {
		Date oldValid_from = valid_from;
		valid_from = newValid_from;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY_NODE__VALID_FROM, oldValid_from, valid_from));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Date getValid_to() {
		return valid_to;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setValid_to(Date newValid_to) {
		Date oldValid_to = valid_to;
		valid_to = newValid_to;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.MEMBER_HIERARCHY_NODE__VALID_TO, oldValid_to, valid_to));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case CorePackage.MEMBER_HIERARCHY_NODE__COMPARATOR:
				return getComparator();
			case CorePackage.MEMBER_HIERARCHY_NODE__LEVEL:
				return getLevel();
			case CorePackage.MEMBER_HIERARCHY_NODE__MEMBER_HIERARCHY_ID:
				if (resolve) return getMember_hierarchy_id();
				return basicGetMember_hierarchy_id();
			case CorePackage.MEMBER_HIERARCHY_NODE__MEMBER_ID:
				if (resolve) return getMember_id();
				return basicGetMember_id();
			case CorePackage.MEMBER_HIERARCHY_NODE__OPERATOR:
				return getOperator();
			case CorePackage.MEMBER_HIERARCHY_NODE__PARENT_MEMBER_ID:
				if (resolve) return getParent_member_id();
				return basicGetParent_member_id();
			case CorePackage.MEMBER_HIERARCHY_NODE__VALID_FROM:
				return getValid_from();
			case CorePackage.MEMBER_HIERARCHY_NODE__VALID_TO:
				return getValid_to();
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
			case CorePackage.MEMBER_HIERARCHY_NODE__COMPARATOR:
				setComparator((String)newValue);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__LEVEL:
				setLevel((Integer)newValue);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__MEMBER_HIERARCHY_ID:
				setMember_hierarchy_id((MEMBER_HIERARCHY)newValue);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__MEMBER_ID:
				setMember_id((MEMBER)newValue);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__OPERATOR:
				setOperator((String)newValue);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__PARENT_MEMBER_ID:
				setParent_member_id((MEMBER)newValue);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__VALID_FROM:
				setValid_from((Date)newValue);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__VALID_TO:
				setValid_to((Date)newValue);
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
			case CorePackage.MEMBER_HIERARCHY_NODE__COMPARATOR:
				setComparator(COMPARATOR_EDEFAULT);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__LEVEL:
				setLevel(LEVEL_EDEFAULT);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__MEMBER_HIERARCHY_ID:
				setMember_hierarchy_id((MEMBER_HIERARCHY)null);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__MEMBER_ID:
				setMember_id((MEMBER)null);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__OPERATOR:
				setOperator(OPERATOR_EDEFAULT);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__PARENT_MEMBER_ID:
				setParent_member_id((MEMBER)null);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__VALID_FROM:
				setValid_from(VALID_FROM_EDEFAULT);
				return;
			case CorePackage.MEMBER_HIERARCHY_NODE__VALID_TO:
				setValid_to(VALID_TO_EDEFAULT);
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
			case CorePackage.MEMBER_HIERARCHY_NODE__COMPARATOR:
				return COMPARATOR_EDEFAULT == null ? comparator != null : !COMPARATOR_EDEFAULT.equals(comparator);
			case CorePackage.MEMBER_HIERARCHY_NODE__LEVEL:
				return level != LEVEL_EDEFAULT;
			case CorePackage.MEMBER_HIERARCHY_NODE__MEMBER_HIERARCHY_ID:
				return member_hierarchy_id != null;
			case CorePackage.MEMBER_HIERARCHY_NODE__MEMBER_ID:
				return member_id != null;
			case CorePackage.MEMBER_HIERARCHY_NODE__OPERATOR:
				return OPERATOR_EDEFAULT == null ? operator != null : !OPERATOR_EDEFAULT.equals(operator);
			case CorePackage.MEMBER_HIERARCHY_NODE__PARENT_MEMBER_ID:
				return parent_member_id != null;
			case CorePackage.MEMBER_HIERARCHY_NODE__VALID_FROM:
				return VALID_FROM_EDEFAULT == null ? valid_from != null : !VALID_FROM_EDEFAULT.equals(valid_from);
			case CorePackage.MEMBER_HIERARCHY_NODE__VALID_TO:
				return VALID_TO_EDEFAULT == null ? valid_to != null : !VALID_TO_EDEFAULT.equals(valid_to);
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
		result.append(" (comparator: ");
		result.append(comparator);
		result.append(", level: ");
		result.append(level);
		result.append(", operator: ");
		result.append(operator);
		result.append(", valid_from: ");
		result.append(valid_from);
		result.append(", valid_to: ");
		result.append(valid_to);
		result.append(')');
		return result.toString();
	}

} //MEMBER_HIERARCHY_NODEImpl
