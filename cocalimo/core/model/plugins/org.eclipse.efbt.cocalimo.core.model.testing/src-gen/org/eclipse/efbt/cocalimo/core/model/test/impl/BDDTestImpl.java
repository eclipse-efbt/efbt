/**
 */
package org.eclipse.efbt.cocalimo.core.model.test.impl;

import org.eclipse.efbt.cocalimo.core.model.test.BDDTest;
import org.eclipse.efbt.cocalimo.core.model.test.TestPackage;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestDefinition;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>BDD Test</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test.impl.BDDTestImpl#getTestDefinition <em>Test Definition</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BDDTestImpl extends TestImpl implements BDDTest
{
	/**
	 * The cached value of the '{@link #getTestDefinition() <em>Test Definition</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestDefinition()
	 * @generated
	 * @ordered
	 */
	protected BDDTestDefinition testDefinition;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BDDTestImpl()
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
		return TestPackage.Literals.BDD_TEST;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BDDTestDefinition getTestDefinition()
	{
		if (testDefinition != null && testDefinition.eIsProxy()) {
			InternalEObject oldTestDefinition = (InternalEObject)testDefinition;
			testDefinition = (BDDTestDefinition)eResolveProxy(oldTestDefinition);
			if (testDefinition != oldTestDefinition) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TestPackage.BDD_TEST__TEST_DEFINITION, oldTestDefinition, testDefinition));
			}
		}
		return testDefinition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BDDTestDefinition basicGetTestDefinition()
	{
		return testDefinition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTestDefinition(BDDTestDefinition newTestDefinition)
	{
		BDDTestDefinition oldTestDefinition = testDefinition;
		testDefinition = newTestDefinition;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestPackage.BDD_TEST__TEST_DEFINITION, oldTestDefinition, testDefinition));
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
			case TestPackage.BDD_TEST__TEST_DEFINITION:
				if (resolve) return getTestDefinition();
				return basicGetTestDefinition();
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
			case TestPackage.BDD_TEST__TEST_DEFINITION:
				setTestDefinition((BDDTestDefinition)newValue);
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
			case TestPackage.BDD_TEST__TEST_DEFINITION:
				setTestDefinition((BDDTestDefinition)null);
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
			case TestPackage.BDD_TEST__TEST_DEFINITION:
				return testDefinition != null;
		}
		return super.eIsSet(featureID);
	}

} //BDDTestImpl
