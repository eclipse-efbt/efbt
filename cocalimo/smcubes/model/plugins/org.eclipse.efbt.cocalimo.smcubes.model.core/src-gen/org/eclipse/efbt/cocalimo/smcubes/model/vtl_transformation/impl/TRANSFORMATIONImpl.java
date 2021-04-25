/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION;
import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>TRANSFORMATION</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATIONImpl#getTransformation_id <em>Transformation id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATIONImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATIONImpl#getExpression <em>Expression</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATIONImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATIONImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATIONImpl#getMaintenance_agency_id <em>Maintenance agency id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATIONImpl#getDisplayName <em>Display Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TRANSFORMATIONImpl extends MinimalEObjectImpl.Container implements TRANSFORMATION
{
	/**
	 * The default value of the '{@link #getTransformation_id() <em>Transformation id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTransformation_id()
	 * @generated
	 * @ordered
	 */
	protected static final String TRANSFORMATION_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getTransformation_id() <em>Transformation id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTransformation_id()
	 * @generated
	 * @ordered
	 */
	protected String transformation_id = TRANSFORMATION_ID_EDEFAULT;

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
	 * The default value of the '{@link #getExpression() <em>Expression</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExpression()
	 * @generated
	 * @ordered
	 */
	protected static final String EXPRESSION_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getExpression() <em>Expression</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExpression()
	 * @generated
	 * @ordered
	 */
	protected String expression = EXPRESSION_EDEFAULT;

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
	 * The default value of the '{@link #getDisplayName() <em>Display Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDisplayName()
	 * @generated
	 * @ordered
	 */
	protected static final String DISPLAY_NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getDisplayName() <em>Display Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDisplayName()
	 * @generated
	 * @ordered
	 */
	protected String displayName = DISPLAY_NAME_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TRANSFORMATIONImpl()
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
		return Vtl_transformationPackage.Literals.TRANSFORMATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getTransformation_id()
	{
		return transformation_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTransformation_id(String newTransformation_id)
	{
		String oldTransformation_id = transformation_id;
		transformation_id = newTransformation_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION__TRANSFORMATION_ID, oldTransformation_id, transformation_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getExpression()
	{
		return expression;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setExpression(String newExpression)
	{
		String oldExpression = expression;
		expression = newExpression;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION__EXPRESSION, oldExpression, expression));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION__NAME, oldName, name));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION__CODE, oldCode, code));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION__MAINTENANCE_AGENCY_ID, oldMaintenance_agency_id, maintenance_agency_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getDisplayName()
	{
		return displayName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setDisplayName(String newDisplayName)
	{
		String oldDisplayName = displayName;
		displayName = newDisplayName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION__DISPLAY_NAME, oldDisplayName, displayName));
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
			case Vtl_transformationPackage.TRANSFORMATION__TRANSFORMATION_ID:
				return getTransformation_id();
			case Vtl_transformationPackage.TRANSFORMATION__DESCRIPTION:
				return getDescription();
			case Vtl_transformationPackage.TRANSFORMATION__EXPRESSION:
				return getExpression();
			case Vtl_transformationPackage.TRANSFORMATION__NAME:
				return getName();
			case Vtl_transformationPackage.TRANSFORMATION__CODE:
				return getCode();
			case Vtl_transformationPackage.TRANSFORMATION__MAINTENANCE_AGENCY_ID:
				return getMaintenance_agency_id();
			case Vtl_transformationPackage.TRANSFORMATION__DISPLAY_NAME:
				return getDisplayName();
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
			case Vtl_transformationPackage.TRANSFORMATION__TRANSFORMATION_ID:
				setTransformation_id((String)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION__EXPRESSION:
				setExpression((String)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION__NAME:
				setName((String)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION__CODE:
				setCode((String)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id((String)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION__DISPLAY_NAME:
				setDisplayName((String)newValue);
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
			case Vtl_transformationPackage.TRANSFORMATION__TRANSFORMATION_ID:
				setTransformation_id(TRANSFORMATION_ID_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION__EXPRESSION:
				setExpression(EXPRESSION_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION__MAINTENANCE_AGENCY_ID:
				setMaintenance_agency_id(MAINTENANCE_AGENCY_ID_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION__DISPLAY_NAME:
				setDisplayName(DISPLAY_NAME_EDEFAULT);
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
			case Vtl_transformationPackage.TRANSFORMATION__TRANSFORMATION_ID:
				return TRANSFORMATION_ID_EDEFAULT == null ? transformation_id != null : !TRANSFORMATION_ID_EDEFAULT.equals(transformation_id);
			case Vtl_transformationPackage.TRANSFORMATION__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case Vtl_transformationPackage.TRANSFORMATION__EXPRESSION:
				return EXPRESSION_EDEFAULT == null ? expression != null : !EXPRESSION_EDEFAULT.equals(expression);
			case Vtl_transformationPackage.TRANSFORMATION__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Vtl_transformationPackage.TRANSFORMATION__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case Vtl_transformationPackage.TRANSFORMATION__MAINTENANCE_AGENCY_ID:
				return MAINTENANCE_AGENCY_ID_EDEFAULT == null ? maintenance_agency_id != null : !MAINTENANCE_AGENCY_ID_EDEFAULT.equals(maintenance_agency_id);
			case Vtl_transformationPackage.TRANSFORMATION__DISPLAY_NAME:
				return DISPLAY_NAME_EDEFAULT == null ? displayName != null : !DISPLAY_NAME_EDEFAULT.equals(displayName);
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
		result.append(" (transformation_id: ");
		result.append(transformation_id);
		result.append(", description: ");
		result.append(description);
		result.append(", expression: ");
		result.append(expression);
		result.append(", name: ");
		result.append(name);
		result.append(", code: ");
		result.append(code);
		result.append(", maintenance_agency_id: ");
		result.append(maintenance_agency_id);
		result.append(", displayName: ");
		result.append(displayName);
		result.append(')');
		return result.toString();
	}

} //TRANSFORMATIONImpl
