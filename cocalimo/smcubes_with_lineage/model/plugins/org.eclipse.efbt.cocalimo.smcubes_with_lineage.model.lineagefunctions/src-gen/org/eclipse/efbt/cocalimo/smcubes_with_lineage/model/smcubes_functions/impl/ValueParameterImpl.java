/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.impl.ParameterImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.Smcubes_functionsPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.ValueParameter;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Value Parameter</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.ValueParameterImpl#getDataType <em>Data Type</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.smcubes_functions.impl.ValueParameterImpl#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ValueParameterImpl extends ParameterImpl implements ValueParameter
{
	/**
	 * The default value of the '{@link #getDataType() <em>Data Type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDataType()
	 * @generated
	 * @ordered
	 */
	protected static final FACET_VALUE_TYPE DATA_TYPE_EDEFAULT = FACET_VALUE_TYPE.BIG_INTEGER;

	/**
	 * The cached value of the '{@link #getDataType() <em>Data Type</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDataType()
	 * @generated
	 * @ordered
	 */
	protected FACET_VALUE_TYPE dataType = DATA_TYPE_EDEFAULT;

	/**
	 * The default value of the '{@link #getValue() <em>Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValue()
	 * @generated
	 * @ordered
	 */
	protected static final String VALUE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getValue() <em>Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValue()
	 * @generated
	 * @ordered
	 */
	protected String value = VALUE_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ValueParameterImpl()
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
		return Smcubes_functionsPackage.Literals.VALUE_PARAMETER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FACET_VALUE_TYPE getDataType()
	{
		return dataType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDataType(FACET_VALUE_TYPE newDataType)
	{
		FACET_VALUE_TYPE oldDataType = dataType;
		dataType = newDataType == null ? DATA_TYPE_EDEFAULT : newDataType;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Smcubes_functionsPackage.VALUE_PARAMETER__DATA_TYPE, oldDataType, dataType));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getValue()
	{
		return value;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setValue(String newValue)
	{
		String oldValue = value;
		value = newValue;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Smcubes_functionsPackage.VALUE_PARAMETER__VALUE, oldValue, value));
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
			case Smcubes_functionsPackage.VALUE_PARAMETER__DATA_TYPE:
				return getDataType();
			case Smcubes_functionsPackage.VALUE_PARAMETER__VALUE:
				return getValue();
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
			case Smcubes_functionsPackage.VALUE_PARAMETER__DATA_TYPE:
				setDataType((FACET_VALUE_TYPE)newValue);
				return;
			case Smcubes_functionsPackage.VALUE_PARAMETER__VALUE:
				setValue((String)newValue);
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
			case Smcubes_functionsPackage.VALUE_PARAMETER__DATA_TYPE:
				setDataType(DATA_TYPE_EDEFAULT);
				return;
			case Smcubes_functionsPackage.VALUE_PARAMETER__VALUE:
				setValue(VALUE_EDEFAULT);
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
			case Smcubes_functionsPackage.VALUE_PARAMETER__DATA_TYPE:
				return dataType != DATA_TYPE_EDEFAULT;
			case Smcubes_functionsPackage.VALUE_PARAMETER__VALUE:
				return VALUE_EDEFAULT == null ? value != null : !VALUE_EDEFAULT.equals(value);
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
		result.append(" (dataType: ");
		result.append(dataType);
		result.append(", value: ");
		result.append(value);
		result.append(')');
		return result.toString();
	}

} //ValueParameterImpl
