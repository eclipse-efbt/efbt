/**
 */
package org.eclipse.efbt.openregspecs.model.rendering.impl;

import org.eclipse.efbt.openregspecs.model.rendering.AXIS;
import org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage;
import org.eclipse.efbt.openregspecs.model.rendering.TABLE;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>AXIS</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.AXISImpl#getAxis_id <em>Axis id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.AXISImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.AXISImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.AXISImpl#getIs_open_axis <em>Is open axis</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.AXISImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.AXISImpl#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.AXISImpl#getOrientation <em>Orientation</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.AXISImpl#getTable_id <em>Table id</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AXISImpl extends MinimalEObjectImpl.Container implements AXIS {
	/**
	 * The default value of the '{@link #getAxis_id() <em>Axis id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAxis_id()
	 * @generated
	 * @ordered
	 */
	protected static final String AXIS_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getAxis_id() <em>Axis id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAxis_id()
	 * @generated
	 * @ordered
	 */
	protected String axis_id = AXIS_ID_EDEFAULT;

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
	 * The default value of the '{@link #getIs_open_axis() <em>Is open axis</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIs_open_axis()
	 * @generated
	 * @ordered
	 */
	protected static final Boolean IS_OPEN_AXIS_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getIs_open_axis() <em>Is open axis</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIs_open_axis()
	 * @generated
	 * @ordered
	 */
	protected Boolean is_open_axis = IS_OPEN_AXIS_EDEFAULT;

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
	 * The default value of the '{@link #getOrientation() <em>Orientation</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOrientation()
	 * @generated
	 * @ordered
	 */
	protected static final String ORIENTATION_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getOrientation() <em>Orientation</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOrientation()
	 * @generated
	 * @ordered
	 */
	protected String orientation = ORIENTATION_EDEFAULT;

	/**
	 * The cached value of the '{@link #getTable_id() <em>Table id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTable_id()
	 * @generated
	 * @ordered
	 */
	protected TABLE table_id;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AXISImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return RenderingPackage.Literals.AXIS;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getAxis_id() {
		return axis_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setAxis_id(String newAxis_id) {
		String oldAxis_id = axis_id;
		axis_id = newAxis_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS__AXIS_ID, oldAxis_id, axis_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getCode() {
		return code;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCode(String newCode) {
		String oldCode = code;
		code = newCode;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDescription(String newDescription) {
		String oldDescription = description;
		description = newDescription;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Boolean getIs_open_axis() {
		return is_open_axis;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setIs_open_axis(Boolean newIs_open_axis) {
		Boolean oldIs_open_axis = is_open_axis;
		is_open_axis = newIs_open_axis;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS__IS_OPEN_AXIS, oldIs_open_axis, is_open_axis));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS__NAME, oldName, name));
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
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS__ORDER, oldOrder, order));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getOrientation() {
		return orientation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOrientation(String newOrientation) {
		String oldOrientation = orientation;
		orientation = newOrientation;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS__ORIENTATION, oldOrientation, orientation));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TABLE getTable_id() {
		if (table_id != null && table_id.eIsProxy()) {
			InternalEObject oldTable_id = (InternalEObject)table_id;
			table_id = (TABLE)eResolveProxy(oldTable_id);
			if (table_id != oldTable_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, RenderingPackage.AXIS__TABLE_ID, oldTable_id, table_id));
			}
		}
		return table_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TABLE basicGetTable_id() {
		return table_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setTable_id(TABLE newTable_id) {
		TABLE oldTable_id = table_id;
		table_id = newTable_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS__TABLE_ID, oldTable_id, table_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case RenderingPackage.AXIS__AXIS_ID:
				return getAxis_id();
			case RenderingPackage.AXIS__CODE:
				return getCode();
			case RenderingPackage.AXIS__DESCRIPTION:
				return getDescription();
			case RenderingPackage.AXIS__IS_OPEN_AXIS:
				return getIs_open_axis();
			case RenderingPackage.AXIS__NAME:
				return getName();
			case RenderingPackage.AXIS__ORDER:
				return getOrder();
			case RenderingPackage.AXIS__ORIENTATION:
				return getOrientation();
			case RenderingPackage.AXIS__TABLE_ID:
				if (resolve) return getTable_id();
				return basicGetTable_id();
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
			case RenderingPackage.AXIS__AXIS_ID:
				setAxis_id((String)newValue);
				return;
			case RenderingPackage.AXIS__CODE:
				setCode((String)newValue);
				return;
			case RenderingPackage.AXIS__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case RenderingPackage.AXIS__IS_OPEN_AXIS:
				setIs_open_axis((Boolean)newValue);
				return;
			case RenderingPackage.AXIS__NAME:
				setName((String)newValue);
				return;
			case RenderingPackage.AXIS__ORDER:
				setOrder((Integer)newValue);
				return;
			case RenderingPackage.AXIS__ORIENTATION:
				setOrientation((String)newValue);
				return;
			case RenderingPackage.AXIS__TABLE_ID:
				setTable_id((TABLE)newValue);
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
			case RenderingPackage.AXIS__AXIS_ID:
				setAxis_id(AXIS_ID_EDEFAULT);
				return;
			case RenderingPackage.AXIS__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case RenderingPackage.AXIS__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case RenderingPackage.AXIS__IS_OPEN_AXIS:
				setIs_open_axis(IS_OPEN_AXIS_EDEFAULT);
				return;
			case RenderingPackage.AXIS__NAME:
				setName(NAME_EDEFAULT);
				return;
			case RenderingPackage.AXIS__ORDER:
				setOrder(ORDER_EDEFAULT);
				return;
			case RenderingPackage.AXIS__ORIENTATION:
				setOrientation(ORIENTATION_EDEFAULT);
				return;
			case RenderingPackage.AXIS__TABLE_ID:
				setTable_id((TABLE)null);
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
			case RenderingPackage.AXIS__AXIS_ID:
				return AXIS_ID_EDEFAULT == null ? axis_id != null : !AXIS_ID_EDEFAULT.equals(axis_id);
			case RenderingPackage.AXIS__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case RenderingPackage.AXIS__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case RenderingPackage.AXIS__IS_OPEN_AXIS:
				return IS_OPEN_AXIS_EDEFAULT == null ? is_open_axis != null : !IS_OPEN_AXIS_EDEFAULT.equals(is_open_axis);
			case RenderingPackage.AXIS__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case RenderingPackage.AXIS__ORDER:
				return order != ORDER_EDEFAULT;
			case RenderingPackage.AXIS__ORIENTATION:
				return ORIENTATION_EDEFAULT == null ? orientation != null : !ORIENTATION_EDEFAULT.equals(orientation);
			case RenderingPackage.AXIS__TABLE_ID:
				return table_id != null;
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
		result.append(" (axis_id: ");
		result.append(axis_id);
		result.append(", code: ");
		result.append(code);
		result.append(", description: ");
		result.append(description);
		result.append(", is_open_axis: ");
		result.append(is_open_axis);
		result.append(", name: ");
		result.append(name);
		result.append(", order: ");
		result.append(order);
		result.append(", orientation: ");
		result.append(orientation);
		result.append(')');
		return result.toString();
	}

} //AXISImpl
