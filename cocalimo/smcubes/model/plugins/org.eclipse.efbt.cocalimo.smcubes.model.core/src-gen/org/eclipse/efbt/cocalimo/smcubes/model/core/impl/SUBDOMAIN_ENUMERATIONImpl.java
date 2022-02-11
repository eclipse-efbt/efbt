/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core.impl;

import java.util.Collection;
import java.util.Date;

import org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage;
import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER;
import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN;
import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ClassifierImpl;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>SUBDOMAIN ENUMERATION</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAIN_ENUMERATIONImpl#getMember_ids <em>Member ids</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAIN_ENUMERATIONImpl#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAIN_ENUMERATIONImpl#getSubdomain_id <em>Subdomain id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAIN_ENUMERATIONImpl#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.core.impl.SUBDOMAIN_ENUMERATIONImpl#getValid_to <em>Valid to</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SUBDOMAIN_ENUMERATIONImpl extends ClassifierImpl implements SUBDOMAIN_ENUMERATION {
	/**
	 * The cached value of the '{@link #getMember_ids() <em>Member ids</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMember_ids()
	 * @generated
	 * @ordered
	 */
	protected EList<MEMBER> member_ids;

	/**
	 * The default value of the '{@link #getOrder() <em>Order</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOrder()
	 * @generated
	 * @ordered
	 */
	protected static final int ORDER_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getOrder() <em>Order</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOrder()
	 * @generated
	 * @ordered
	 */
	protected int order = ORDER_EDEFAULT;

	/**
	 * The cached value of the '{@link #getSubdomain_id() <em>Subdomain id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubdomain_id()
	 * @generated
	 * @ordered
	 */
	protected SUBDOMAIN subdomain_id;

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
	protected SUBDOMAIN_ENUMERATIONImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return CorePackage.Literals.SUBDOMAIN_ENUMERATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<MEMBER> getMember_ids() {
		if (member_ids == null) {
			member_ids = new EObjectResolvingEList<MEMBER>(MEMBER.class, this, CorePackage.SUBDOMAIN_ENUMERATION__MEMBER_IDS);
		}
		return member_ids;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public int getOrder() {
		return order;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setOrder(int newOrder) {
		int oldOrder = order;
		order = newOrder;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.SUBDOMAIN_ENUMERATION__ORDER, oldOrder, order));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SUBDOMAIN getSubdomain_id() {
		if (subdomain_id != null && subdomain_id.eIsProxy()) {
			InternalEObject oldSubdomain_id = (InternalEObject)subdomain_id;
			subdomain_id = (SUBDOMAIN)eResolveProxy(oldSubdomain_id);
			if (subdomain_id != oldSubdomain_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CorePackage.SUBDOMAIN_ENUMERATION__SUBDOMAIN_ID, oldSubdomain_id, subdomain_id));
			}
		}
		return subdomain_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SUBDOMAIN basicGetSubdomain_id() {
		return subdomain_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSubdomain_id(SUBDOMAIN newSubdomain_id) {
		SUBDOMAIN oldSubdomain_id = subdomain_id;
		subdomain_id = newSubdomain_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.SUBDOMAIN_ENUMERATION__SUBDOMAIN_ID, oldSubdomain_id, subdomain_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Date getValid_from() {
		return valid_from;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setValid_from(Date newValid_from) {
		Date oldValid_from = valid_from;
		valid_from = newValid_from;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.SUBDOMAIN_ENUMERATION__VALID_FROM, oldValid_from, valid_from));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Date getValid_to() {
		return valid_to;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setValid_to(Date newValid_to) {
		Date oldValid_to = valid_to;
		valid_to = newValid_to;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.SUBDOMAIN_ENUMERATION__VALID_TO, oldValid_to, valid_to));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case CorePackage.SUBDOMAIN_ENUMERATION__MEMBER_IDS:
				return getMember_ids();
			case CorePackage.SUBDOMAIN_ENUMERATION__ORDER:
				return getOrder();
			case CorePackage.SUBDOMAIN_ENUMERATION__SUBDOMAIN_ID:
				if (resolve) return getSubdomain_id();
				return basicGetSubdomain_id();
			case CorePackage.SUBDOMAIN_ENUMERATION__VALID_FROM:
				return getValid_from();
			case CorePackage.SUBDOMAIN_ENUMERATION__VALID_TO:
				return getValid_to();
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
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case CorePackage.SUBDOMAIN_ENUMERATION__MEMBER_IDS:
				getMember_ids().clear();
				getMember_ids().addAll((Collection<? extends MEMBER>)newValue);
				return;
			case CorePackage.SUBDOMAIN_ENUMERATION__ORDER:
				setOrder((Integer)newValue);
				return;
			case CorePackage.SUBDOMAIN_ENUMERATION__SUBDOMAIN_ID:
				setSubdomain_id((SUBDOMAIN)newValue);
				return;
			case CorePackage.SUBDOMAIN_ENUMERATION__VALID_FROM:
				setValid_from((Date)newValue);
				return;
			case CorePackage.SUBDOMAIN_ENUMERATION__VALID_TO:
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
			case CorePackage.SUBDOMAIN_ENUMERATION__MEMBER_IDS:
				getMember_ids().clear();
				return;
			case CorePackage.SUBDOMAIN_ENUMERATION__ORDER:
				setOrder(ORDER_EDEFAULT);
				return;
			case CorePackage.SUBDOMAIN_ENUMERATION__SUBDOMAIN_ID:
				setSubdomain_id((SUBDOMAIN)null);
				return;
			case CorePackage.SUBDOMAIN_ENUMERATION__VALID_FROM:
				setValid_from(VALID_FROM_EDEFAULT);
				return;
			case CorePackage.SUBDOMAIN_ENUMERATION__VALID_TO:
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
			case CorePackage.SUBDOMAIN_ENUMERATION__MEMBER_IDS:
				return member_ids != null && !member_ids.isEmpty();
			case CorePackage.SUBDOMAIN_ENUMERATION__ORDER:
				return order != ORDER_EDEFAULT;
			case CorePackage.SUBDOMAIN_ENUMERATION__SUBDOMAIN_ID:
				return subdomain_id != null;
			case CorePackage.SUBDOMAIN_ENUMERATION__VALID_FROM:
				return VALID_FROM_EDEFAULT == null ? valid_from != null : !VALID_FROM_EDEFAULT.equals(valid_from);
			case CorePackage.SUBDOMAIN_ENUMERATION__VALID_TO:
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
		result.append(" (order: ");
		result.append(order);
		result.append(", valid_from: ");
		result.append(valid_from);
		result.append(", valid_to: ");
		result.append(valid_to);
		result.append(')');
		return result.toString();
	}

} //SUBDOMAIN_ENUMERATIONImpl
