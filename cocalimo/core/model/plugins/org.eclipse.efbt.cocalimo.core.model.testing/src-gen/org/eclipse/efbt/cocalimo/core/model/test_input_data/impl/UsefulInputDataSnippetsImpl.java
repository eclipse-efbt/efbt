/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_input_data.impl;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.TestInputData;
import org.eclipse.efbt.cocalimo.core.model.test_input_data.Test_input_dataPackage;
import org.eclipse.efbt.cocalimo.core.model.test_input_data.UsefulInputDataSnippets;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Useful Input Data Snippets</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.UsefulInputDataSnippetsImpl#getTestInputData <em>Test Input Data</em>}</li>
 * </ul>
 *
 * @generated
 */
public class UsefulInputDataSnippetsImpl extends MinimalEObjectImpl.Container implements UsefulInputDataSnippets
{
	/**
	 * The cached value of the '{@link #getTestInputData() <em>Test Input Data</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTestInputData()
	 * @generated
	 * @ordered
	 */
	protected TestInputData testInputData;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected UsefulInputDataSnippetsImpl()
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
		return Test_input_dataPackage.Literals.USEFUL_INPUT_DATA_SNIPPETS;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TestInputData getTestInputData()
	{
		return testInputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetTestInputData(TestInputData newTestInputData, NotificationChain msgs)
	{
		TestInputData oldTestInputData = testInputData;
		testInputData = newTestInputData;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS__TEST_INPUT_DATA, oldTestInputData, newTestInputData);
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
	public void setTestInputData(TestInputData newTestInputData)
	{
		if (newTestInputData != testInputData) {
			NotificationChain msgs = null;
			if (testInputData != null)
				msgs = ((InternalEObject)testInputData).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS__TEST_INPUT_DATA, null, msgs);
			if (newTestInputData != null)
				msgs = ((InternalEObject)newTestInputData).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS__TEST_INPUT_DATA, null, msgs);
			msgs = basicSetTestInputData(newTestInputData, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS__TEST_INPUT_DATA, newTestInputData, newTestInputData));
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
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS__TEST_INPUT_DATA:
				return basicSetTestInputData(null, msgs);
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
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS__TEST_INPUT_DATA:
				return getTestInputData();
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
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS__TEST_INPUT_DATA:
				setTestInputData((TestInputData)newValue);
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
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS__TEST_INPUT_DATA:
				setTestInputData((TestInputData)null);
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
			case Test_input_dataPackage.USEFUL_INPUT_DATA_SNIPPETS__TEST_INPUT_DATA:
				return testInputData != null;
		}
		return super.eIsSet(featureID);
	}

} //UsefulInputDataSnippetsImpl
