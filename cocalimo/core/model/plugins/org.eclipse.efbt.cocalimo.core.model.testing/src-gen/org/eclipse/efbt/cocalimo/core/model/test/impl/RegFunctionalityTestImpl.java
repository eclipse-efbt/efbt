/**
 */
package org.eclipse.efbt.cocalimo.core.model.test.impl;

import org.eclipse.efbt.cocalimo.core.model.test.RegFunctionalityTest;
import org.eclipse.efbt.cocalimo.core.model.test.TestPackage;

import org.eclipse.efbt.cocalimo.core.model.test_definition.RegFunctionalityTestDefinition;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Reg Functionality Test</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test.impl.RegFunctionalityTestImpl#getTestDefinition <em>Test Definition</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RegFunctionalityTestImpl extends TestImpl implements RegFunctionalityTest
{
	/**
	 * The cached value of the '{@link #getTestDefinition() <em>Test Definition</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestDefinition()
	 * @generated
	 * @ordered
	 */
	protected RegFunctionalityTestDefinition testDefinition;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RegFunctionalityTestImpl()
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
		return TestPackage.Literals.REG_FUNCTIONALITY_TEST;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RegFunctionalityTestDefinition getTestDefinition()
	{
		if (testDefinition != null && testDefinition.eIsProxy()) {
			InternalEObject oldTestDefinition = (InternalEObject)testDefinition;
			testDefinition = (RegFunctionalityTestDefinition)eResolveProxy(oldTestDefinition);
			if (testDefinition != oldTestDefinition) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TestPackage.REG_FUNCTIONALITY_TEST__TEST_DEFINITION, oldTestDefinition, testDefinition));
			}
		}
		return testDefinition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RegFunctionalityTestDefinition basicGetTestDefinition()
	{
		return testDefinition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTestDefinition(RegFunctionalityTestDefinition newTestDefinition)
	{
		RegFunctionalityTestDefinition oldTestDefinition = testDefinition;
		testDefinition = newTestDefinition;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestPackage.REG_FUNCTIONALITY_TEST__TEST_DEFINITION, oldTestDefinition, testDefinition));
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
			case TestPackage.REG_FUNCTIONALITY_TEST__TEST_DEFINITION:
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
			case TestPackage.REG_FUNCTIONALITY_TEST__TEST_DEFINITION:
				setTestDefinition((RegFunctionalityTestDefinition)newValue);
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
			case TestPackage.REG_FUNCTIONALITY_TEST__TEST_DEFINITION:
				setTestDefinition((RegFunctionalityTestDefinition)null);
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
			case TestPackage.REG_FUNCTIONALITY_TEST__TEST_DEFINITION:
				return testDefinition != null;
		}
		return super.eIsSet(featureID);
	}

} //RegFunctionalityTestImpl
