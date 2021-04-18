/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.impl;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestContraints;
import org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid;
import org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGridPoint;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Coverage Grid</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridImpl#getCoverageTestSet <em>Coverage Test Set</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridImpl#getCoverageGridPoints <em>Coverage Grid Points</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.CoverageGridImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CoverageGridImpl extends MinimalEObjectImpl.Container implements CoverageGrid
{
	/**
	 * The cached value of the '{@link #getCoverageTestSet() <em>Coverage Test Set</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCoverageTestSet()
	 * @generated
	 * @ordered
	 */
	protected BDDTestContraints coverageTestSet;

	/**
	 * The cached value of the '{@link #getCoverageGridPoints() <em>Coverage Grid Points</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCoverageGridPoints()
	 * @generated
	 * @ordered
	 */
	protected CoverageGridPoint coverageGridPoints;

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
	protected CoverageGridImpl()
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
		return Test_definitionPackage.Literals.COVERAGE_GRID;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestContraints getCoverageTestSet()
	{
		if (coverageTestSet != null && coverageTestSet.eIsProxy()) {
			InternalEObject oldCoverageTestSet = (InternalEObject)coverageTestSet;
			coverageTestSet = (BDDTestContraints)eResolveProxy(oldCoverageTestSet);
			if (coverageTestSet != oldCoverageTestSet) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Test_definitionPackage.COVERAGE_GRID__COVERAGE_TEST_SET, oldCoverageTestSet, coverageTestSet));
			}
		}
		return coverageTestSet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BDDTestContraints basicGetCoverageTestSet()
	{
		return coverageTestSet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCoverageTestSet(BDDTestContraints newCoverageTestSet)
	{
		BDDTestContraints oldCoverageTestSet = coverageTestSet;
		coverageTestSet = newCoverageTestSet;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.COVERAGE_GRID__COVERAGE_TEST_SET, oldCoverageTestSet, coverageTestSet));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CoverageGridPoint getCoverageGridPoints()
	{
		return coverageGridPoints;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetCoverageGridPoints(CoverageGridPoint newCoverageGridPoints, NotificationChain msgs)
	{
		CoverageGridPoint oldCoverageGridPoints = coverageGridPoints;
		coverageGridPoints = newCoverageGridPoints;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Test_definitionPackage.COVERAGE_GRID__COVERAGE_GRID_POINTS, oldCoverageGridPoints, newCoverageGridPoints);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCoverageGridPoints(CoverageGridPoint newCoverageGridPoints)
	{
		if (newCoverageGridPoints != coverageGridPoints) {
			NotificationChain msgs = null;
			if (coverageGridPoints != null)
				msgs = ((InternalEObject)coverageGridPoints).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.COVERAGE_GRID__COVERAGE_GRID_POINTS, null, msgs);
			if (newCoverageGridPoints != null)
				msgs = ((InternalEObject)newCoverageGridPoints).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Test_definitionPackage.COVERAGE_GRID__COVERAGE_GRID_POINTS, null, msgs);
			msgs = basicSetCoverageGridPoints(newCoverageGridPoints, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.COVERAGE_GRID__COVERAGE_GRID_POINTS, newCoverageGridPoints, newCoverageGridPoints));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.COVERAGE_GRID__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case Test_definitionPackage.COVERAGE_GRID__COVERAGE_GRID_POINTS:
				return basicSetCoverageGridPoints(null, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
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
			case Test_definitionPackage.COVERAGE_GRID__COVERAGE_TEST_SET:
				if (resolve) return getCoverageTestSet();
				return basicGetCoverageTestSet();
			case Test_definitionPackage.COVERAGE_GRID__COVERAGE_GRID_POINTS:
				return getCoverageGridPoints();
			case Test_definitionPackage.COVERAGE_GRID__NAME:
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
		switch (featureID) {
			case Test_definitionPackage.COVERAGE_GRID__COVERAGE_TEST_SET:
				setCoverageTestSet((BDDTestContraints)newValue);
				return;
			case Test_definitionPackage.COVERAGE_GRID__COVERAGE_GRID_POINTS:
				setCoverageGridPoints((CoverageGridPoint)newValue);
				return;
			case Test_definitionPackage.COVERAGE_GRID__NAME:
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
		switch (featureID) {
			case Test_definitionPackage.COVERAGE_GRID__COVERAGE_TEST_SET:
				setCoverageTestSet((BDDTestContraints)null);
				return;
			case Test_definitionPackage.COVERAGE_GRID__COVERAGE_GRID_POINTS:
				setCoverageGridPoints((CoverageGridPoint)null);
				return;
			case Test_definitionPackage.COVERAGE_GRID__NAME:
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
		switch (featureID) {
			case Test_definitionPackage.COVERAGE_GRID__COVERAGE_TEST_SET:
				return coverageTestSet != null;
			case Test_definitionPackage.COVERAGE_GRID__COVERAGE_GRID_POINTS:
				return coverageGridPoints != null;
			case Test_definitionPackage.COVERAGE_GRID__NAME:
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

} //CoverageGridImpl
