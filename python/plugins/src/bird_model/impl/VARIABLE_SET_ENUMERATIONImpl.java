/**
 */
package bird_model.impl;

import bird_model.Bird_modelPackage;
import bird_model.SUBDOMAIN;
import bird_model.VARIABLE;
import bird_model.VARIABLE_SET;
import bird_model.VARIABLE_SET_ENUMERATION;

import java.util.Date;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VARIABLE SET ENUMERATION</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link bird_model.impl.VARIABLE_SET_ENUMERATIONImpl#isIs_flow <em>Is flow</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLE_SET_ENUMERATIONImpl#getOrder <em>Order</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLE_SET_ENUMERATIONImpl#getSubdomain_id <em>Subdomain id</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLE_SET_ENUMERATIONImpl#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLE_SET_ENUMERATIONImpl#getValid_to <em>Valid to</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLE_SET_ENUMERATIONImpl#getVariable_id <em>Variable id</em>}</li>
 *   <li>{@link bird_model.impl.VARIABLE_SET_ENUMERATIONImpl#getVariable_set_id <em>Variable set id</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VARIABLE_SET_ENUMERATIONImpl extends MinimalEObjectImpl.Container implements VARIABLE_SET_ENUMERATION {
	/**
	 * The default value of the '{@link #isIs_flow() <em>Is flow</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_flow()
	 * @generated
	 * @ordered
	 */
	protected static final boolean IS_FLOW_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isIs_flow() <em>Is flow</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_flow()
	 * @generated
	 * @ordered
	 */
	protected boolean is_flow = IS_FLOW_EDEFAULT;

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
	 * The cached value of the '{@link #getVariable_id() <em>Variable id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable_id()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE variable_id;

	/**
	 * The cached value of the '{@link #getVariable_set_id() <em>Variable set id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVariable_set_id()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE_SET variable_set_id;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VARIABLE_SET_ENUMERATIONImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Bird_modelPackage.Literals.VARIABLE_SET_ENUMERATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isIs_flow() {
		return is_flow;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setIs_flow(boolean newIs_flow) {
		boolean oldIs_flow = is_flow;
		is_flow = newIs_flow;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE_SET_ENUMERATION__IS_FLOW, oldIs_flow, is_flow));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public int getOrder() {
		return order;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOrder(int newOrder) {
		int oldOrder = order;
		order = newOrder;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE_SET_ENUMERATION__ORDER, oldOrder, order));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SUBDOMAIN getSubdomain_id() {
		if (subdomain_id != null && subdomain_id.eIsProxy()) {
			InternalEObject oldSubdomain_id = (InternalEObject)subdomain_id;
			subdomain_id = (SUBDOMAIN)eResolveProxy(oldSubdomain_id);
			if (subdomain_id != oldSubdomain_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Bird_modelPackage.VARIABLE_SET_ENUMERATION__SUBDOMAIN_ID, oldSubdomain_id, subdomain_id));
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
	public void setSubdomain_id(SUBDOMAIN newSubdomain_id) {
		SUBDOMAIN oldSubdomain_id = subdomain_id;
		subdomain_id = newSubdomain_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE_SET_ENUMERATION__SUBDOMAIN_ID, oldSubdomain_id, subdomain_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE_SET_ENUMERATION__VALID_FROM, oldValid_from, valid_from));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE_SET_ENUMERATION__VALID_TO, oldValid_to, valid_to));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE getVariable_id() {
		if (variable_id != null && variable_id.eIsProxy()) {
			InternalEObject oldVariable_id = (InternalEObject)variable_id;
			variable_id = (VARIABLE)eResolveProxy(oldVariable_id);
			if (variable_id != oldVariable_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Bird_modelPackage.VARIABLE_SET_ENUMERATION__VARIABLE_ID, oldVariable_id, variable_id));
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
	public void setVariable_id(VARIABLE newVariable_id) {
		VARIABLE oldVariable_id = variable_id;
		variable_id = newVariable_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE_SET_ENUMERATION__VARIABLE_ID, oldVariable_id, variable_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE_SET getVariable_set_id() {
		if (variable_set_id != null && variable_set_id.eIsProxy()) {
			InternalEObject oldVariable_set_id = (InternalEObject)variable_set_id;
			variable_set_id = (VARIABLE_SET)eResolveProxy(oldVariable_set_id);
			if (variable_set_id != oldVariable_set_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Bird_modelPackage.VARIABLE_SET_ENUMERATION__VARIABLE_SET_ID, oldVariable_set_id, variable_set_id));
			}
		}
		return variable_set_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE_SET basicGetVariable_set_id() {
		return variable_set_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVariable_set_id(VARIABLE_SET newVariable_set_id) {
		VARIABLE_SET oldVariable_set_id = variable_set_id;
		variable_set_id = newVariable_set_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.VARIABLE_SET_ENUMERATION__VARIABLE_SET_ID, oldVariable_set_id, variable_set_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__IS_FLOW:
				return isIs_flow();
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__ORDER:
				return getOrder();
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__SUBDOMAIN_ID:
				if (resolve) return getSubdomain_id();
				return basicGetSubdomain_id();
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VALID_FROM:
				return getValid_from();
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VALID_TO:
				return getValid_to();
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VARIABLE_ID:
				if (resolve) return getVariable_id();
				return basicGetVariable_id();
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VARIABLE_SET_ID:
				if (resolve) return getVariable_set_id();
				return basicGetVariable_set_id();
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
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__IS_FLOW:
				setIs_flow((Boolean)newValue);
				return;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__ORDER:
				setOrder((Integer)newValue);
				return;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__SUBDOMAIN_ID:
				setSubdomain_id((SUBDOMAIN)newValue);
				return;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VALID_FROM:
				setValid_from((Date)newValue);
				return;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VALID_TO:
				setValid_to((Date)newValue);
				return;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VARIABLE_ID:
				setVariable_id((VARIABLE)newValue);
				return;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VARIABLE_SET_ID:
				setVariable_set_id((VARIABLE_SET)newValue);
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
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__IS_FLOW:
				setIs_flow(IS_FLOW_EDEFAULT);
				return;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__ORDER:
				setOrder(ORDER_EDEFAULT);
				return;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__SUBDOMAIN_ID:
				setSubdomain_id((SUBDOMAIN)null);
				return;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VALID_FROM:
				setValid_from(VALID_FROM_EDEFAULT);
				return;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VALID_TO:
				setValid_to(VALID_TO_EDEFAULT);
				return;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VARIABLE_ID:
				setVariable_id((VARIABLE)null);
				return;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VARIABLE_SET_ID:
				setVariable_set_id((VARIABLE_SET)null);
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
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__IS_FLOW:
				return is_flow != IS_FLOW_EDEFAULT;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__ORDER:
				return order != ORDER_EDEFAULT;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__SUBDOMAIN_ID:
				return subdomain_id != null;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VALID_FROM:
				return VALID_FROM_EDEFAULT == null ? valid_from != null : !VALID_FROM_EDEFAULT.equals(valid_from);
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VALID_TO:
				return VALID_TO_EDEFAULT == null ? valid_to != null : !VALID_TO_EDEFAULT.equals(valid_to);
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VARIABLE_ID:
				return variable_id != null;
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION__VARIABLE_SET_ID:
				return variable_set_id != null;
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
		result.append(" (is_flow: ");
		result.append(is_flow);
		result.append(", order: ");
		result.append(order);
		result.append(", valid_from: ");
		result.append(valid_from);
		result.append(", valid_to: ");
		result.append(valid_to);
		result.append(')');
		return result.toString();
	}

} //VARIABLE_SET_ENUMERATIONImpl
