/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER;
import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN;
import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;
import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE_SET;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_DMNSN;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.TYP_RL;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>CUBE STRUCTURE ITEM</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#getAttribute_associated_variable <em>Attribute associated variable</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#getCube_structure_id <em>Cube structure id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#getDimension_type <em>Dimension type</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#isIs_flow <em>Is flow</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#isIs_mandatory <em>Is mandatory</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#getMember_id <em>Member id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#getOrder <em>Order</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#getRole <em>Role</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#getSubdomain_id <em>Subdomain id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#getVariable_id <em>Variable id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#getVariable_set_id <em>Variable set id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#isIsIdentifier <em>Is Identifier</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl.CUBE_STRUCTURE_ITEMImpl#getCube_variable_code <em>Cube variable code</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CUBE_STRUCTURE_ITEMImpl extends MinimalEObjectImpl.Container implements CUBE_STRUCTURE_ITEM {
	/**
	 * The cached value of the '{@link #getAttribute_associated_variable() <em>Attribute associated variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAttribute_associated_variable()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE attribute_associated_variable;

	/**
	 * The cached value of the '{@link #getCube_structure_id() <em>Cube structure id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_structure_id()
	 * @generated
	 * @ordered
	 */
	protected CUBE_STRUCTURE cube_structure_id;

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
	 * The default value of the '{@link #getDimension_type() <em>Dimension type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDimension_type()
	 * @generated
	 * @ordered
	 */
	protected static final TYP_DMNSN DIMENSION_TYPE_EDEFAULT = TYP_DMNSN.B;

	/**
	 * The cached value of the '{@link #getDimension_type() <em>Dimension type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDimension_type()
	 * @generated
	 * @ordered
	 */
	protected TYP_DMNSN dimension_type = DIMENSION_TYPE_EDEFAULT;

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
	 * The default value of the '{@link #isIs_mandatory() <em>Is mandatory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_mandatory()
	 * @generated
	 * @ordered
	 */
	protected static final boolean IS_MANDATORY_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isIs_mandatory() <em>Is mandatory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIs_mandatory()
	 * @generated
	 * @ordered
	 */
	protected boolean is_mandatory = IS_MANDATORY_EDEFAULT;

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
	 * The default value of the '{@link #getRole() <em>Role</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRole()
	 * @generated
	 * @ordered
	 */
	protected static final TYP_RL ROLE_EDEFAULT = TYP_RL.O;

	/**
	 * The cached value of the '{@link #getRole() <em>Role</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRole()
	 * @generated
	 * @ordered
	 */
	protected TYP_RL role = ROLE_EDEFAULT;

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
	 * The default value of the '{@link #isIsIdentifier() <em>Is Identifier</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIsIdentifier()
	 * @generated
	 * @ordered
	 */
	protected static final boolean IS_IDENTIFIER_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isIsIdentifier() <em>Is Identifier</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isIsIdentifier()
	 * @generated
	 * @ordered
	 */
	protected boolean isIdentifier = IS_IDENTIFIER_EDEFAULT;

	/**
	 * The default value of the '{@link #getCube_variable_code() <em>Cube variable code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_variable_code()
	 * @generated
	 * @ordered
	 */
	protected static final String CUBE_VARIABLE_CODE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getCube_variable_code() <em>Cube variable code</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube_variable_code()
	 * @generated
	 * @ordered
	 */
	protected String cube_variable_code = CUBE_VARIABLE_CODE_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CUBE_STRUCTURE_ITEMImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_definitionPackage.Literals.CUBE_STRUCTURE_ITEM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getAttribute_associated_variable() {
		if (attribute_associated_variable != null && attribute_associated_variable.eIsProxy()) {
			InternalEObject oldAttribute_associated_variable = (InternalEObject)attribute_associated_variable;
			attribute_associated_variable = (VARIABLE)eResolveProxy(oldAttribute_associated_variable);
			if (attribute_associated_variable != oldAttribute_associated_variable) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_STRUCTURE_ITEM__ATTRIBUTE_ASSOCIATED_VARIABLE, oldAttribute_associated_variable, attribute_associated_variable));
			}
		}
		return attribute_associated_variable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetAttribute_associated_variable() {
		return attribute_associated_variable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setAttribute_associated_variable(VARIABLE newAttribute_associated_variable) {
		VARIABLE oldAttribute_associated_variable = attribute_associated_variable;
		attribute_associated_variable = newAttribute_associated_variable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__ATTRIBUTE_ASSOCIATED_VARIABLE, oldAttribute_associated_variable, attribute_associated_variable));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE_STRUCTURE getCube_structure_id() {
		if (cube_structure_id != null && cube_structure_id.eIsProxy()) {
			InternalEObject oldCube_structure_id = (InternalEObject)cube_structure_id;
			cube_structure_id = (CUBE_STRUCTURE)eResolveProxy(oldCube_structure_id);
			if (cube_structure_id != oldCube_structure_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_STRUCTURE_ITEM__CUBE_STRUCTURE_ID, oldCube_structure_id, cube_structure_id));
			}
		}
		return cube_structure_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE_STRUCTURE basicGetCube_structure_id() {
		return cube_structure_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube_structure_id(CUBE_STRUCTURE newCube_structure_id) {
		CUBE_STRUCTURE oldCube_structure_id = cube_structure_id;
		cube_structure_id = newCube_structure_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__CUBE_STRUCTURE_ID, oldCube_structure_id, cube_structure_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TYP_DMNSN getDimension_type() {
		return dimension_type;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setDimension_type(TYP_DMNSN newDimension_type) {
		TYP_DMNSN oldDimension_type = dimension_type;
		dimension_type = newDimension_type == null ? DIMENSION_TYPE_EDEFAULT : newDimension_type;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__DIMENSION_TYPE, oldDimension_type, dimension_type));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isIs_flow() {
		return is_flow;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setIs_flow(boolean newIs_flow) {
		boolean oldIs_flow = is_flow;
		is_flow = newIs_flow;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_FLOW, oldIs_flow, is_flow));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isIs_mandatory() {
		return is_mandatory;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setIs_mandatory(boolean newIs_mandatory) {
		boolean oldIs_mandatory = is_mandatory;
		is_mandatory = newIs_mandatory;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_MANDATORY, oldIs_mandatory, is_mandatory));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_STRUCTURE_ITEM__MEMBER_ID, oldMember_id, member_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__MEMBER_ID, oldMember_id, member_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__ORDER, oldOrder, order));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TYP_RL getRole() {
		return role;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setRole(TYP_RL newRole) {
		TYP_RL oldRole = role;
		role = newRole == null ? ROLE_EDEFAULT : newRole;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__ROLE, oldRole, role));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_STRUCTURE_ITEM__SUBDOMAIN_ID, oldSubdomain_id, subdomain_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__SUBDOMAIN_ID, oldSubdomain_id, subdomain_id));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_STRUCTURE_ITEM__VARIABLE_ID, oldVariable_id, variable_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__VARIABLE_ID, oldVariable_id, variable_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE_SET getVariable_set_id() {
		if (variable_set_id != null && variable_set_id.eIsProxy()) {
			InternalEObject oldVariable_set_id = (InternalEObject)variable_set_id;
			variable_set_id = (VARIABLE_SET)eResolveProxy(oldVariable_set_id);
			if (variable_set_id != oldVariable_set_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Data_definitionPackage.CUBE_STRUCTURE_ITEM__VARIABLE_SET_ID, oldVariable_set_id, variable_set_id));
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
	@Override
	public void setVariable_set_id(VARIABLE_SET newVariable_set_id) {
		VARIABLE_SET oldVariable_set_id = variable_set_id;
		variable_set_id = newVariable_set_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__VARIABLE_SET_ID, oldVariable_set_id, variable_set_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean isIsIdentifier() {
		return isIdentifier;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setIsIdentifier(boolean newIsIdentifier) {
		boolean oldIsIdentifier = isIdentifier;
		isIdentifier = newIsIdentifier;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_IDENTIFIER, oldIsIdentifier, isIdentifier));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getCube_variable_code() {
		return cube_variable_code;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube_variable_code(String newCube_variable_code) {
		String oldCube_variable_code = cube_variable_code;
		cube_variable_code = newCube_variable_code;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_definitionPackage.CUBE_STRUCTURE_ITEM__CUBE_VARIABLE_CODE, oldCube_variable_code, cube_variable_code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ATTRIBUTE_ASSOCIATED_VARIABLE:
				if (resolve) return getAttribute_associated_variable();
				return basicGetAttribute_associated_variable();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__CUBE_STRUCTURE_ID:
				if (resolve) return getCube_structure_id();
				return basicGetCube_structure_id();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__DESCRIPTION:
				return getDescription();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__DIMENSION_TYPE:
				return getDimension_type();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_FLOW:
				return isIs_flow();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_MANDATORY:
				return isIs_mandatory();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__MEMBER_ID:
				if (resolve) return getMember_id();
				return basicGetMember_id();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ORDER:
				return getOrder();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ROLE:
				return getRole();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__SUBDOMAIN_ID:
				if (resolve) return getSubdomain_id();
				return basicGetSubdomain_id();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__VARIABLE_ID:
				if (resolve) return getVariable_id();
				return basicGetVariable_id();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__VARIABLE_SET_ID:
				if (resolve) return getVariable_set_id();
				return basicGetVariable_set_id();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_IDENTIFIER:
				return isIsIdentifier();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__CUBE_VARIABLE_CODE:
				return getCube_variable_code();
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
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ATTRIBUTE_ASSOCIATED_VARIABLE:
				setAttribute_associated_variable((VARIABLE)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__CUBE_STRUCTURE_ID:
				setCube_structure_id((CUBE_STRUCTURE)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__DIMENSION_TYPE:
				setDimension_type((TYP_DMNSN)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_FLOW:
				setIs_flow((Boolean)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_MANDATORY:
				setIs_mandatory((Boolean)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__MEMBER_ID:
				setMember_id((MEMBER)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ORDER:
				setOrder((Integer)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ROLE:
				setRole((TYP_RL)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__SUBDOMAIN_ID:
				setSubdomain_id((SUBDOMAIN)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__VARIABLE_ID:
				setVariable_id((VARIABLE)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__VARIABLE_SET_ID:
				setVariable_set_id((VARIABLE_SET)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_IDENTIFIER:
				setIsIdentifier((Boolean)newValue);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__CUBE_VARIABLE_CODE:
				setCube_variable_code((String)newValue);
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
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ATTRIBUTE_ASSOCIATED_VARIABLE:
				setAttribute_associated_variable((VARIABLE)null);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__CUBE_STRUCTURE_ID:
				setCube_structure_id((CUBE_STRUCTURE)null);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__DIMENSION_TYPE:
				setDimension_type(DIMENSION_TYPE_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_FLOW:
				setIs_flow(IS_FLOW_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_MANDATORY:
				setIs_mandatory(IS_MANDATORY_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__MEMBER_ID:
				setMember_id((MEMBER)null);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ORDER:
				setOrder(ORDER_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ROLE:
				setRole(ROLE_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__SUBDOMAIN_ID:
				setSubdomain_id((SUBDOMAIN)null);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__VARIABLE_ID:
				setVariable_id((VARIABLE)null);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__VARIABLE_SET_ID:
				setVariable_set_id((VARIABLE_SET)null);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_IDENTIFIER:
				setIsIdentifier(IS_IDENTIFIER_EDEFAULT);
				return;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__CUBE_VARIABLE_CODE:
				setCube_variable_code(CUBE_VARIABLE_CODE_EDEFAULT);
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
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ATTRIBUTE_ASSOCIATED_VARIABLE:
				return attribute_associated_variable != null;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__CUBE_STRUCTURE_ID:
				return cube_structure_id != null;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__DIMENSION_TYPE:
				return dimension_type != DIMENSION_TYPE_EDEFAULT;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_FLOW:
				return is_flow != IS_FLOW_EDEFAULT;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_MANDATORY:
				return is_mandatory != IS_MANDATORY_EDEFAULT;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__MEMBER_ID:
				return member_id != null;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ORDER:
				return order != ORDER_EDEFAULT;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__ROLE:
				return role != ROLE_EDEFAULT;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__SUBDOMAIN_ID:
				return subdomain_id != null;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__VARIABLE_ID:
				return variable_id != null;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__VARIABLE_SET_ID:
				return variable_set_id != null;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__IS_IDENTIFIER:
				return isIdentifier != IS_IDENTIFIER_EDEFAULT;
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM__CUBE_VARIABLE_CODE:
				return CUBE_VARIABLE_CODE_EDEFAULT == null ? cube_variable_code != null : !CUBE_VARIABLE_CODE_EDEFAULT.equals(cube_variable_code);
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
		result.append(" (description: ");
		result.append(description);
		result.append(", dimension_type: ");
		result.append(dimension_type);
		result.append(", is_flow: ");
		result.append(is_flow);
		result.append(", is_mandatory: ");
		result.append(is_mandatory);
		result.append(", order: ");
		result.append(order);
		result.append(", role: ");
		result.append(role);
		result.append(", isIdentifier: ");
		result.append(isIdentifier);
		result.append(", cube_variable_code: ");
		result.append(cube_variable_code);
		result.append(')');
		return result.toString();
	}

} //CUBE_STRUCTURE_ITEMImpl
