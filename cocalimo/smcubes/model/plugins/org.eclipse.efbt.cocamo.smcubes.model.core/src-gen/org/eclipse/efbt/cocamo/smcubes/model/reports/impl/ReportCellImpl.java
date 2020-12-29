/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.reports.impl;

import org.eclipse.efbt.cocamo.smcubes.model.data_definition.COMBINATION;

import org.eclipse.efbt.cocamo.smcubes.model.reports.ReportCell;
import org.eclipse.efbt.cocamo.smcubes.model.reports.ReportsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Report Cell</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.reports.impl.ReportCellImpl#getCombination <em>Combination</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocamo.smcubes.model.reports.impl.ReportCellImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ReportCellImpl extends MinimalEObjectImpl.Container implements ReportCell
{
	/**
	 * The cached value of the '{@link #getCombination() <em>Combination</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCombination()
	 * @generated
	 * @ordered
	 */
	protected COMBINATION combination;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ReportCellImpl()
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
		return ReportsPackage.Literals.REPORT_CELL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public COMBINATION getCombination()
	{
		if (combination != null && combination.eIsProxy())
		{
			InternalEObject oldCombination = (InternalEObject)combination;
			combination = (COMBINATION)eResolveProxy(oldCombination);
			if (combination != oldCombination)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, ReportsPackage.REPORT_CELL__COMBINATION, oldCombination, combination));
			}
		}
		return combination;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public COMBINATION basicGetCombination()
	{
		return combination;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCombination(COMBINATION newCombination)
	{
		COMBINATION oldCombination = combination;
		combination = newCombination;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ReportsPackage.REPORT_CELL__COMBINATION, oldCombination, combination));
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
			eNotify(new ENotificationImpl(this, Notification.SET, ReportsPackage.REPORT_CELL__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID)
		{
			case ReportsPackage.REPORT_CELL__COMBINATION:
				if (resolve) return getCombination();
				return basicGetCombination();
			case ReportsPackage.REPORT_CELL__NAME:
				return getName();
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
		switch (featureID)
		{
			case ReportsPackage.REPORT_CELL__COMBINATION:
				setCombination((COMBINATION)newValue);
				return;
			case ReportsPackage.REPORT_CELL__NAME:
				setName((String)newValue);
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
		switch (featureID)
		{
			case ReportsPackage.REPORT_CELL__COMBINATION:
				setCombination((COMBINATION)null);
				return;
			case ReportsPackage.REPORT_CELL__NAME:
				setName(NAME_EDEFAULT);
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
		switch (featureID)
		{
			case ReportsPackage.REPORT_CELL__COMBINATION:
				return combination != null;
			case ReportsPackage.REPORT_CELL__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
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
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //ReportCellImpl
