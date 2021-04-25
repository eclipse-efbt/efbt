/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl;

import java.util.Date;

import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME;
import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.Vtl_transformationPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>TRANSFORMATION SCHEME</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATION_SCHEMEImpl#getTransformation_scheme_id <em>Transformation scheme id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATION_SCHEMEImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATION_SCHEMEImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATION_SCHEMEImpl#getCode <em>Code</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATION_SCHEMEImpl#getVersion <em>Version</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATION_SCHEMEImpl#getValid_to <em>Valid to</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATION_SCHEMEImpl#getValid_from <em>Valid from</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.impl.TRANSFORMATION_SCHEMEImpl#getDisplayName <em>Display Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TRANSFORMATION_SCHEMEImpl extends MinimalEObjectImpl.Container implements TRANSFORMATION_SCHEME
{
	/**
	 * The default value of the '{@link #getTransformation_scheme_id() <em>Transformation scheme id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTransformation_scheme_id()
	 * @generated
	 * @ordered
	 */
	protected static final String TRANSFORMATION_SCHEME_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getTransformation_scheme_id() <em>Transformation scheme id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTransformation_scheme_id()
	 * @generated
	 * @ordered
	 */
	protected String transformation_scheme_id = TRANSFORMATION_SCHEME_ID_EDEFAULT;

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
	 * The default value of the '{@link #getVersion() <em>Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVersion()
	 * @generated
	 * @ordered
	 */
	protected static final String VERSION_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getVersion() <em>Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVersion()
	 * @generated
	 * @ordered
	 */
	protected String version = VERSION_EDEFAULT;

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
	protected TRANSFORMATION_SCHEMEImpl()
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
		return Vtl_transformationPackage.Literals.TRANSFORMATION_SCHEME;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getTransformation_scheme_id()
	{
		return transformation_scheme_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTransformation_scheme_id(String newTransformation_scheme_id)
	{
		String oldTransformation_scheme_id = transformation_scheme_id;
		transformation_scheme_id = newTransformation_scheme_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION_SCHEME__TRANSFORMATION_SCHEME_ID, oldTransformation_scheme_id, transformation_scheme_id));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION_SCHEME__DESCRIPTION, oldDescription, description));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION_SCHEME__NAME, oldName, name));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION_SCHEME__CODE, oldCode, code));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getVersion()
	{
		return version;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setVersion(String newVersion)
	{
		String oldVersion = version;
		version = newVersion;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION_SCHEME__VERSION, oldVersion, version));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Date getValid_to()
	{
		return valid_to;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setValid_to(Date newValid_to)
	{
		Date oldValid_to = valid_to;
		valid_to = newValid_to;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION_SCHEME__VALID_TO, oldValid_to, valid_to));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Date getValid_from()
	{
		return valid_from;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setValid_from(Date newValid_from)
	{
		Date oldValid_from = valid_from;
		valid_from = newValid_from;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION_SCHEME__VALID_FROM, oldValid_from, valid_from));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Vtl_transformationPackage.TRANSFORMATION_SCHEME__DISPLAY_NAME, oldDisplayName, displayName));
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
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__TRANSFORMATION_SCHEME_ID:
				return getTransformation_scheme_id();
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__DESCRIPTION:
				return getDescription();
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__NAME:
				return getName();
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__CODE:
				return getCode();
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VERSION:
				return getVersion();
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VALID_TO:
				return getValid_to();
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VALID_FROM:
				return getValid_from();
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__DISPLAY_NAME:
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
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__TRANSFORMATION_SCHEME_ID:
				setTransformation_scheme_id((String)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__NAME:
				setName((String)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__CODE:
				setCode((String)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VERSION:
				setVersion((String)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VALID_TO:
				setValid_to((Date)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VALID_FROM:
				setValid_from((Date)newValue);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__DISPLAY_NAME:
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
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__TRANSFORMATION_SCHEME_ID:
				setTransformation_scheme_id(TRANSFORMATION_SCHEME_ID_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__CODE:
				setCode(CODE_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VERSION:
				setVersion(VERSION_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VALID_TO:
				setValid_to(VALID_TO_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VALID_FROM:
				setValid_from(VALID_FROM_EDEFAULT);
				return;
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__DISPLAY_NAME:
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
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__TRANSFORMATION_SCHEME_ID:
				return TRANSFORMATION_SCHEME_ID_EDEFAULT == null ? transformation_scheme_id != null : !TRANSFORMATION_SCHEME_ID_EDEFAULT.equals(transformation_scheme_id);
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__CODE:
				return CODE_EDEFAULT == null ? code != null : !CODE_EDEFAULT.equals(code);
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VERSION:
				return VERSION_EDEFAULT == null ? version != null : !VERSION_EDEFAULT.equals(version);
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VALID_TO:
				return VALID_TO_EDEFAULT == null ? valid_to != null : !VALID_TO_EDEFAULT.equals(valid_to);
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__VALID_FROM:
				return VALID_FROM_EDEFAULT == null ? valid_from != null : !VALID_FROM_EDEFAULT.equals(valid_from);
			case Vtl_transformationPackage.TRANSFORMATION_SCHEME__DISPLAY_NAME:
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
		result.append(" (transformation_scheme_id: ");
		result.append(transformation_scheme_id);
		result.append(", description: ");
		result.append(description);
		result.append(", name: ");
		result.append(name);
		result.append(", code: ");
		result.append(code);
		result.append(", version: ");
		result.append(version);
		result.append(", valid_to: ");
		result.append(valid_to);
		result.append(", valid_from: ");
		result.append(valid_from);
		result.append(", displayName: ");
		result.append(displayName);
		result.append(')');
		return result.toString();
	}

} //TRANSFORMATION_SCHEMEImpl
