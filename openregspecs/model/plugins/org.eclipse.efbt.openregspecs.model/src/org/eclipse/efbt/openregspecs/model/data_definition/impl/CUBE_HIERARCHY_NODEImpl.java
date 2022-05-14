/**
 */
package org.eclipse.efbt.openregspecs.model.data_definition.impl;

import java.util.Date;

import org.eclipse.efbt.openregspecs.model.data_definition.CUBE_GROUP;
import org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY;
import org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY_NODE;
import org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>CUBE HIERARCHY NODE</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl#getCube_group_id <em>Cube group id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl#getCube_hierarchy_id <em>Cube hierarchy id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl#getLevel <em>Level</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl#getNODE_CODE <em>NODE CODE</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl#getNode_name <em>Node name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl#getParent_node_code <em>Parent node code</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.data_definition.impl.CUBE_HIERARCHY_NODEImpl#getValid_to <em>Valid to</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CUBE_HIERARCHY_NODEImpl extends MinimalEObjectImpl.Container implements CUBE_HIERARCHY_NODE {
	/**
	 * The cached value of the '{@link #getCube_group_id() <em>Cube group id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_group_id()
	 * @generated
	 * @ordered
	 */
	protected CUBE_GROUP cube_group_id;

	/**
	 * The cached value of the '{@link #getCube_hierarchy_id() <em>Cube hierarchy id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_hierarchy_id()
	 * @generated
	 * @ordered
	 */
	protected CUBE_HIERARCHY cube_hierarchy_id;

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
	 * The default value of the '{@link #getNODE_CODE() <em>NODE CODE</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getNODE_CODE()
	 * @generated
	 * @ordered
	 */
	protected static final String NODE_CODE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getNODE_CODE() <em>NODE CODE</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getNODE_CODE()
	 * @generated
	 * @ordered
	 */
	protected String nODE_CODE = NODE_CODE_EDEFAULT;

	/**
	 * The default value of the '{@link #getNode_name() <em>Node name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getNode_name()
	 * @generated
	 * @ordered
	 */
	protected static final String NODE_NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getNode_name() <em>Node name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getNode_name()
	 * @generated
	 * @ordered
	 */
	protected String node_name = NODE_NAME_EDEFAULT;

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
	 * The cached value of the '{@link #getParent_node_code() <em>Parent node code</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getParent_node_code()
	 * @generated
	 * @ordered
	 */
	protected CUBE_HIERARCHY_NODE parent_node_code;

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
	protected CUBE_HIERARCHY_NODEImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_definitionPackage.Literals.CUBE_HIERARCHY_NODE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE_GROUP getCube_group_id() {
		if (cube_group_id != null && cube_group_id.eIsProxy()) {
			InternalEObject oldCube_group_id = (InternalEObject)cube_group_id;
			cube_group_id = (CUBE_GROUP)eResolveProxy(oldCube_group_id);
			if (cube_group_id != oldCube_group_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_HIERARCHY_NODE__CUBE_GROUP_ID, oldCube_group_id, cube_group_id));
			}
		}
		return cube_group_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE_GROUP basicGetCube_group_id() {
		return cube_group_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCube_group_id(CUBE_GROUP newCube_group_id) {
		CUBE_GROUP oldCube_group_id = cube_group_id;
		cube_group_id = newCube_group_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY_NODE__CUBE_GROUP_ID, oldCube_group_id, cube_group_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE_HIERARCHY getCube_hierarchy_id() {
		if (cube_hierarchy_id != null && cube_hierarchy_id.eIsProxy()) {
			InternalEObject oldCube_hierarchy_id = (InternalEObject)cube_hierarchy_id;
			cube_hierarchy_id = (CUBE_HIERARCHY)eResolveProxy(oldCube_hierarchy_id);
			if (cube_hierarchy_id != oldCube_hierarchy_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_HIERARCHY_NODE__CUBE_HIERARCHY_ID, oldCube_hierarchy_id, cube_hierarchy_id));
			}
		}
		return cube_hierarchy_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE_HIERARCHY basicGetCube_hierarchy_id() {
		return cube_hierarchy_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCube_hierarchy_id(CUBE_HIERARCHY newCube_hierarchy_id) {
		CUBE_HIERARCHY oldCube_hierarchy_id = cube_hierarchy_id;
		cube_hierarchy_id = newCube_hierarchy_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY_NODE__CUBE_HIERARCHY_ID, oldCube_hierarchy_id, cube_hierarchy_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY_NODE__LEVEL, oldLevel, level));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getNODE_CODE() {
		return nODE_CODE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setNODE_CODE(String newNODE_CODE) {
		String oldNODE_CODE = nODE_CODE;
		nODE_CODE = newNODE_CODE;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY_NODE__NODE_CODE, oldNODE_CODE, nODE_CODE));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getNode_name() {
		return node_name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setNode_name(String newNode_name) {
		String oldNode_name = node_name;
		node_name = newNode_name;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY_NODE__NODE_NAME, oldNode_name, node_name));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY_NODE__ORDER, oldOrder, order));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE_HIERARCHY_NODE getParent_node_code() {
		if (parent_node_code != null && parent_node_code.eIsProxy()) {
			InternalEObject oldParent_node_code = (InternalEObject)parent_node_code;
			parent_node_code = (CUBE_HIERARCHY_NODE)eResolveProxy(oldParent_node_code);
			if (parent_node_code != oldParent_node_code) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_HIERARCHY_NODE__PARENT_NODE_CODE, oldParent_node_code, parent_node_code));
			}
		}
		return parent_node_code;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE_HIERARCHY_NODE basicGetParent_node_code() {
		return parent_node_code;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setParent_node_code(CUBE_HIERARCHY_NODE newParent_node_code) {
		CUBE_HIERARCHY_NODE oldParent_node_code = parent_node_code;
		parent_node_code = newParent_node_code;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY_NODE__PARENT_NODE_CODE, oldParent_node_code, parent_node_code));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY_NODE__VALID_FROM, oldValid_from, valid_from));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_HIERARCHY_NODE__VALID_TO, oldValid_to, valid_to));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__CUBE_GROUP_ID:
				if (resolve) return getCube_group_id();
				return basicGetCube_group_id();
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__CUBE_HIERARCHY_ID:
				if (resolve) return getCube_hierarchy_id();
				return basicGetCube_hierarchy_id();
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__LEVEL:
				return getLevel();
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__NODE_CODE:
				return getNODE_CODE();
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__NODE_NAME:
				return getNode_name();
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__ORDER:
				return getOrder();
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__PARENT_NODE_CODE:
				if (resolve) return getParent_node_code();
				return basicGetParent_node_code();
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__VALID_FROM:
				return getValid_from();
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__VALID_TO:
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
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__CUBE_GROUP_ID:
				setCube_group_id((CUBE_GROUP)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__CUBE_HIERARCHY_ID:
				setCube_hierarchy_id((CUBE_HIERARCHY)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__LEVEL:
				setLevel((Integer)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__NODE_CODE:
				setNODE_CODE((String)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__NODE_NAME:
				setNode_name((String)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__ORDER:
				setOrder((Integer)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__PARENT_NODE_CODE:
				setParent_node_code((CUBE_HIERARCHY_NODE)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__VALID_FROM:
				setValid_from((Date)newValue);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__VALID_TO:
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
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__CUBE_GROUP_ID:
				setCube_group_id((CUBE_GROUP)null);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__CUBE_HIERARCHY_ID:
				setCube_hierarchy_id((CUBE_HIERARCHY)null);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__LEVEL:
				setLevel(LEVEL_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__NODE_CODE:
				setNODE_CODE(NODE_CODE_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__NODE_NAME:
				setNode_name(NODE_NAME_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__ORDER:
				setOrder(ORDER_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__PARENT_NODE_CODE:
				setParent_node_code((CUBE_HIERARCHY_NODE)null);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__VALID_FROM:
				setValid_from(VALID_FROM_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__VALID_TO:
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
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__CUBE_GROUP_ID:
				return cube_group_id != null;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__CUBE_HIERARCHY_ID:
				return cube_hierarchy_id != null;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__LEVEL:
				return level != LEVEL_EDEFAULT;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__NODE_CODE:
				return NODE_CODE_EDEFAULT == null ? nODE_CODE != null : !NODE_CODE_EDEFAULT.equals(nODE_CODE);
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__NODE_NAME:
				return NODE_NAME_EDEFAULT == null ? node_name != null : !NODE_NAME_EDEFAULT.equals(node_name);
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__ORDER:
				return order != ORDER_EDEFAULT;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__PARENT_NODE_CODE:
				return parent_node_code != null;
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__VALID_FROM:
				return VALID_FROM_EDEFAULT == null ? valid_from != null : !VALID_FROM_EDEFAULT.equals(valid_from);
			case Data_definitionPackage.CUBE_HIERARCHY_NODE__VALID_TO:
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
		result.append(" (level: ");
		result.append(level);
		result.append(", nODE_CODE: ");
		result.append(nODE_CODE);
		result.append(", node_name: ");
		result.append(node_name);
		result.append(", order: ");
		result.append(order);
		result.append(", valid_from: ");
		result.append(valid_from);
		result.append(", valid_to: ");
		result.append(valid_to);
		result.append(')');
		return result.toString();
	}

} //CUBE_HIERARCHY_NODEImpl
