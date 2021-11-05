/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS;
import org.eclipse.efbt.cocalimo.smcubes.model.rendering.AXIS_ORDINATE;
import org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>AXIS ORDINATE</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl#getAxis_id <em>Axis id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl#getAxis_ordinate_id <em>Axis ordinate id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl#getIs_abstract_header <em>Is abstract header</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl#getLevel <em>Level</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl#getParent_axis_ordinate_id <em>Parent axis ordinate id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.impl.AXIS_ORDINATEImpl#getPath <em>Path</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AXIS_ORDINATEImpl extends MinimalEObjectImpl.Container implements AXIS_ORDINATE {
	/**
	 * The cached value of the '{@link #getAxis_id() <em>Axis id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAxis_id()
	 * @generated
	 * @ordered
	 */
	protected AXIS axis_id;

	/**
	 * The default value of the '{@link #getAxis_ordinate_id() <em>Axis ordinate id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAxis_ordinate_id()
	 * @generated
	 * @ordered
	 */
	protected static final String AXIS_ORDINATE_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getAxis_ordinate_id() <em>Axis ordinate id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAxis_ordinate_id()
	 * @generated
	 * @ordered
	 */
	protected String axis_ordinate_id = AXIS_ORDINATE_ID_EDEFAULT;

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
	 * The default value of the '{@link #getIs_abstract_header() <em>Is abstract header</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIs_abstract_header()
	 * @generated
	 * @ordered
	 */
	protected static final Boolean IS_ABSTRACT_HEADER_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getIs_abstract_header() <em>Is abstract header</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIs_abstract_header()
	 * @generated
	 * @ordered
	 */
	protected Boolean is_abstract_header = IS_ABSTRACT_HEADER_EDEFAULT;

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
	 * The cached value of the '{@link #getParent_axis_ordinate_id() <em>Parent axis ordinate id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getParent_axis_ordinate_id()
	 * @generated
	 * @ordered
	 */
	protected AXIS_ORDINATE parent_axis_ordinate_id;

	/**
	 * The default value of the '{@link #getPath() <em>Path</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getPath()
	 * @generated
	 * @ordered
	 */
	protected static final String PATH_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getPath() <em>Path</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getPath()
	 * @generated
	 * @ordered
	 */
	protected String path = PATH_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AXIS_ORDINATEImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return RenderingPackage.Literals.AXIS_ORDINATE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AXIS getAxis_id() {
		if (axis_id != null && axis_id.eIsProxy()) {
			InternalEObject oldAxis_id = (InternalEObject)axis_id;
			axis_id = (AXIS)eResolveProxy(oldAxis_id);
			if (axis_id != oldAxis_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, RenderingPackage.AXIS_ORDINATE__AXIS_ID, oldAxis_id, axis_id));
			}
		}
		return axis_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AXIS basicGetAxis_id() {
		return axis_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setAxis_id(AXIS newAxis_id) {
		AXIS oldAxis_id = axis_id;
		axis_id = newAxis_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS_ORDINATE__AXIS_ID, oldAxis_id, axis_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getAxis_ordinate_id() {
		return axis_ordinate_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setAxis_ordinate_id(String newAxis_ordinate_id) {
		String oldAxis_ordinate_id = axis_ordinate_id;
		axis_ordinate_id = newAxis_ordinate_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS_ORDINATE__AXIS_ORDINATE_ID, oldAxis_ordinate_id, axis_ordinate_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getCode() {
		return code;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCode(String newCode) {
		String oldCode = code;
		code = newCode;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS_ORDINATE__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getDescription() {
		return description;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setDescription(String newDescription) {
		String oldDescription = description;
		description = newDescription;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS_ORDINATE__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Boolean getIs_abstract_header() {
		return is_abstract_header;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setIs_abstract_header(Boolean newIs_abstract_header) {
		Boolean oldIs_abstract_header = is_abstract_header;
		is_abstract_header = newIs_abstract_header;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS_ORDINATE__IS_ABSTRACT_HEADER, oldIs_abstract_header, is_abstract_header));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public int getLevel() {
		return level;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setLevel(int newLevel) {
		int oldLevel = level;
		level = newLevel;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS_ORDINATE__LEVEL, oldLevel, level));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS_ORDINATE__NAME, oldName, name));
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
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS_ORDINATE__ORDER, oldOrder, order));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AXIS_ORDINATE getParent_axis_ordinate_id() {
		if (parent_axis_ordinate_id != null && parent_axis_ordinate_id.eIsProxy()) {
			InternalEObject oldParent_axis_ordinate_id = (InternalEObject)parent_axis_ordinate_id;
			parent_axis_ordinate_id = (AXIS_ORDINATE)eResolveProxy(oldParent_axis_ordinate_id);
			if (parent_axis_ordinate_id != oldParent_axis_ordinate_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, RenderingPackage.AXIS_ORDINATE__PARENT_AXIS_ORDINATE_ID, oldParent_axis_ordinate_id, parent_axis_ordinate_id));
			}
		}
		return parent_axis_ordinate_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AXIS_ORDINATE basicGetParent_axis_ordinate_id() {
		return parent_axis_ordinate_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setParent_axis_ordinate_id(AXIS_ORDINATE newParent_axis_ordinate_id) {
		AXIS_ORDINATE oldParent_axis_ordinate_id = parent_axis_ordinate_id;
		parent_axis_ordinate_id = newParent_axis_ordinate_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS_ORDINATE__PARENT_AXIS_ORDINATE_ID, oldParent_axis_ordinate_id, parent_axis_ordinate_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getPath() {
		return path;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setPath(String newPath) {
		String oldPath = path;
		path = newPath;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.AXIS_ORDINATE__PATH, oldPath, path));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case RenderingPackage.AXIS_ORDINATE__AXIS_ID:
				if (resolve) return getAxis_id();
				return basicGetAxis_id();
			case RenderingPackage.AXIS_ORDINATE__AXIS_ORDINATE_ID:
				return getAxis_ordinate_id();
			case RenderingPackage.AXIS_ORDINATE__CODE:
				return getCode();
			case RenderingPackage.AXIS_ORDINATE__DESCRIPTION:
				return getDescription();
			case RenderingPackage.AXIS_ORDINATE__IS_ABSTRACT_HEADER:
				return getIs_abstract_header();
			case RenderingPackage.AXIS_ORDINATE__LEVEL:
				return getLevel();
			case RenderingPackage.AXIS_ORDINATE__NAME:
				return getName();
			case RenderingPackage.AXIS_ORDINATE__ORDER:
				return getOrder();
			case RenderingPackage.AXIS_ORDINATE__PARENT_AXIS_ORDINATE_ID:
				if (resolve) return getParent_axis_ordinate_id();
				return basicGetParent_axis_ordinate_id();
			case RenderingPackage.AXIS_ORDINATE__PATH:
				return getPath();
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
			case RenderingPackage.AXIS_ORDINATE__AXIS_ID:
				setAxis_id((AXIS)newValue);
				return;
			case RenderingPackage.AXIS_ORDINATE__AXIS_ORDINATE_ID:
				setAxis_ordinate_id((String)newValue);
				return;
			case RenderingPackage.AXIS_ORDINATE__CODE:
				setCode((String)newValue);
				return;
			case RenderingPackage.AXIS_ORDINATE__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case RenderingPackage.AXIS_ORDINATE__IS_ABSTRACT_HEADER:
				setIs_abstract_header((Boolean)newValue);
				return;
			case RenderingPackage.AXIS_ORDINATE__LEVEL:
				setLevel((Integer)newValue);
				return;
			case RenderingPackage.AXIS_ORDINATE__NAME:
				setName((String)newValue);
				return;
			case RenderingPackage.AXIS_ORDINATE__ORDER:
				setOrder((Integer)newValue);
				return;
			case RenderingPackage.AXIS_ORDINATE__PARENT_AXIS_ORDINATE_ID:
				setParent_axis_ordinate_id((AXIS_ORDINATE)newValue);
				return;
			case RenderingPackage.AXIS_ORDINATE__PATH:
				setPath((String)newValue);
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
			case RenderingPackage.AXIS_ORDINATE__AXIS_ID:
				setAxis_id((AXIS)null);
				return;
			case RenderingPackage.AXIS_ORDINATE__AXIS_ORDINATE_ID:
				setAxis_ordinate_id(AXIS_ORDINATE_ID_EDEFAULT);
				return;
			case RenderingPackage.AXIS_ORDINATE__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case RenderingPackage.AXIS_ORDINATE__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case RenderingPackage.AXIS_ORDINATE__IS_ABSTRACT_HEADER:
				setIs_abstract_header(IS_ABSTRACT_HEADER_EDEFAULT);
				return;
			case RenderingPackage.AXIS_ORDINATE__LEVEL:
				setLevel(LEVEL_EDEFAULT);
				return;
			case RenderingPackage.AXIS_ORDINATE__NAME:
				setName(NAME_EDEFAULT);
				return;
			case RenderingPackage.AXIS_ORDINATE__ORDER:
				setOrder(ORDER_EDEFAULT);
				return;
			case RenderingPackage.AXIS_ORDINATE__PARENT_AXIS_ORDINATE_ID:
				setParent_axis_ordinate_id((AXIS_ORDINATE)null);
				return;
			case RenderingPackage.AXIS_ORDINATE__PATH:
				setPath(PATH_EDEFAULT);
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
			case RenderingPackage.AXIS_ORDINATE__AXIS_ID:
				return axis_id != null;
			case RenderingPackage.AXIS_ORDINATE__AXIS_ORDINATE_ID:
				return AXIS_ORDINATE_ID_EDEFAULT == null ? axis_ordinate_id != null : !AXIS_ORDINATE_ID_EDEFAULT.equals(axis_ordinate_id);
			case RenderingPackage.AXIS_ORDINATE__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case RenderingPackage.AXIS_ORDINATE__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case RenderingPackage.AXIS_ORDINATE__IS_ABSTRACT_HEADER:
				return IS_ABSTRACT_HEADER_EDEFAULT == null ? is_abstract_header != null : !IS_ABSTRACT_HEADER_EDEFAULT.equals(is_abstract_header);
			case RenderingPackage.AXIS_ORDINATE__LEVEL:
				return level != LEVEL_EDEFAULT;
			case RenderingPackage.AXIS_ORDINATE__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case RenderingPackage.AXIS_ORDINATE__ORDER:
				return order != ORDER_EDEFAULT;
			case RenderingPackage.AXIS_ORDINATE__PARENT_AXIS_ORDINATE_ID:
				return parent_axis_ordinate_id != null;
			case RenderingPackage.AXIS_ORDINATE__PATH:
				return PATH_EDEFAULT == null ? path != null : !PATH_EDEFAULT.equals(path);
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
		result.append(" (axis_ordinate_id: ");
		result.append(axis_ordinate_id);
		result.append(", code: ");
		result.append(code);
		result.append(", description: ");
		result.append(description);
		result.append(", is_abstract_header: ");
		result.append(is_abstract_header);
		result.append(", level: ");
		result.append(level);
		result.append(", name: ");
		result.append(name);
		result.append(", order: ");
		result.append(order);
		result.append(", path: ");
		result.append(path);
		result.append(')');
		return result.toString();
	}

} //AXIS_ORDINATEImpl
