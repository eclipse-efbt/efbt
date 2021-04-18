/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl;

import org.eclipse.efbt.cocalimo.core.model.test.impl.BDDTestImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesBDDTest;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesReportResults;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesTestInputData;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>SM Cubes BDD Test</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesBDDTestImpl#getInputData <em>Input Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesBDDTestImpl#getExpectedResults <em>Expected Results</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SMCubesBDDTestImpl extends BDDTestImpl implements SMCubesBDDTest
{
	/**
	 * The cached value of the '{@link #getInputData() <em>Input Data</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInputData()
	 * @generated
	 * @ordered
	 */
	protected SMCubesTestInputData inputData;

	/**
	 * The cached value of the '{@link #getExpectedResults() <em>Expected Results</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExpectedResults()
	 * @generated
	 * @ordered
	 */
	protected SMCubesReportResults expectedResults;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SMCubesBDDTestImpl()
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
		return CocamoPackage.Literals.SM_CUBES_BDD_TEST;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesTestInputData getInputData()
	{
		return inputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetInputData(SMCubesTestInputData newInputData, NotificationChain msgs)
	{
		SMCubesTestInputData oldInputData = inputData;
		inputData = newInputData;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_BDD_TEST__INPUT_DATA, oldInputData, newInputData);
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
	public void setInputData(SMCubesTestInputData newInputData)
	{
		if (newInputData != inputData) {
			NotificationChain msgs = null;
			if (inputData != null)
				msgs = ((InternalEObject)inputData).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_BDD_TEST__INPUT_DATA, null, msgs);
			if (newInputData != null)
				msgs = ((InternalEObject)newInputData).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - CocamoPackage.SM_CUBES_BDD_TEST__INPUT_DATA, null, msgs);
			msgs = basicSetInputData(newInputData, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_BDD_TEST__INPUT_DATA, newInputData, newInputData));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesReportResults getExpectedResults()
	{
		if (expectedResults != null && expectedResults.eIsProxy()) {
			InternalEObject oldExpectedResults = (InternalEObject)expectedResults;
			expectedResults = (SMCubesReportResults)eResolveProxy(oldExpectedResults);
			if (expectedResults != oldExpectedResults) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CocamoPackage.SM_CUBES_BDD_TEST__EXPECTED_RESULTS, oldExpectedResults, expectedResults));
			}
		}
		return expectedResults;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SMCubesReportResults basicGetExpectedResults()
	{
		return expectedResults;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setExpectedResults(SMCubesReportResults newExpectedResults)
	{
		SMCubesReportResults oldExpectedResults = expectedResults;
		expectedResults = newExpectedResults;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CocamoPackage.SM_CUBES_BDD_TEST__EXPECTED_RESULTS, oldExpectedResults, expectedResults));
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
			case CocamoPackage.SM_CUBES_BDD_TEST__INPUT_DATA:
				return basicSetInputData(null, msgs);
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
			case CocamoPackage.SM_CUBES_BDD_TEST__INPUT_DATA:
				return getInputData();
			case CocamoPackage.SM_CUBES_BDD_TEST__EXPECTED_RESULTS:
				if (resolve) return getExpectedResults();
				return basicGetExpectedResults();
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
			case CocamoPackage.SM_CUBES_BDD_TEST__INPUT_DATA:
				setInputData((SMCubesTestInputData)newValue);
				return;
			case CocamoPackage.SM_CUBES_BDD_TEST__EXPECTED_RESULTS:
				setExpectedResults((SMCubesReportResults)newValue);
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
			case CocamoPackage.SM_CUBES_BDD_TEST__INPUT_DATA:
				setInputData((SMCubesTestInputData)null);
				return;
			case CocamoPackage.SM_CUBES_BDD_TEST__EXPECTED_RESULTS:
				setExpectedResults((SMCubesReportResults)null);
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
			case CocamoPackage.SM_CUBES_BDD_TEST__INPUT_DATA:
				return inputData != null;
			case CocamoPackage.SM_CUBES_BDD_TEST__EXPECTED_RESULTS:
				return expectedResults != null;
		}
		return super.eIsSet(featureID);
	}

} //SMCubesBDDTestImpl
