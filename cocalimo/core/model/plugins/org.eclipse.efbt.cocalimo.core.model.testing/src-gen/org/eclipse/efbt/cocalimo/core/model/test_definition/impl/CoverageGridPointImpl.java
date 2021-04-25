/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Coverage Grid Point</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridPointImpl#getCount <em>Count</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridPointImpl#getObjects <em>Objects</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CoverageGridPointImpl extends MinimalEObjectImpl.Container implements CoverageGridPoint
{
	/**
	 * The default value of the '{@link #getCount() <em>Count</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCount()
	 * @generated
	 * @ordered
	 */
	protected static final int COUNT_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getCount() <em>Count</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCount()
	 * @generated
	 * @ordered
	 */
	protected int count = COUNT_EDEFAULT;

	/**
	 * The cached value of the '{@link #getObjects() <em>Objects</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getObjects()
	 * @generated
	 * @ordered
	 */
	protected EList<EObject> objects;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CoverageGridPointImpl()
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
		return Test_definitionPackage.Literals.COVERAGE_GRID_POINT;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public int getCount()
	{
		return count;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCount(int newCount)
	{
		int oldCount = count;
		count = newCount;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.COVERAGE_GRID_POINT__COUNT, oldCount, count));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<EObject> getObjects()
	{
		if (objects == null) {
			objects = new EObjectResolvingEList<EObject>(EObject.class, this, Test_definitionPackage.COVERAGE_GRID_POINT__OBJECTS);
		}
		return objects;
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
			case Test_definitionPackage.COVERAGE_GRID_POINT__COUNT:
				return getCount();
			case Test_definitionPackage.COVERAGE_GRID_POINT__OBJECTS:
				return getObjects();
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
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case Test_definitionPackage.COVERAGE_GRID_POINT__COUNT:
				setCount((Integer)newValue);
				return;
			case Test_definitionPackage.COVERAGE_GRID_POINT__OBJECTS:
				getObjects().clear();
				getObjects().addAll((Collection<? extends EObject>)newValue);
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
			case Test_definitionPackage.COVERAGE_GRID_POINT__COUNT:
				setCount(COUNT_EDEFAULT);
				return;
			case Test_definitionPackage.COVERAGE_GRID_POINT__OBJECTS:
				getObjects().clear();
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
			case Test_definitionPackage.COVERAGE_GRID_POINT__COUNT:
				return count != COUNT_EDEFAULT;
			case Test_definitionPackage.COVERAGE_GRID_POINT__OBJECTS:
				return objects != null && !objects.isEmpty();
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
		result.append(" (count: ");
		result.append(count);
		result.append(')');
		return result.toString();
	}

} //CoverageGridPointImpl
