/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.program.impl;

import org.eclipse.efbt.cocalimo.core.model.test.impl.RegFunctionalityTestImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.program.ProgramPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesRegFunctionalityTest;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesReportResults;
import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesTestInputData;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>SM Cubes Reg Functionality Test</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesRegFunctionalityTestImpl#getInputData <em>Input Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.program.impl.SMCubesRegFunctionalityTestImpl#getExpectedResults <em>Expected Results</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SMCubesRegFunctionalityTestImpl extends RegFunctionalityTestImpl implements SMCubesRegFunctionalityTest {
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
	 * The cached value of the '{@link #getExpectedResults() <em>Expected Results</em>}' containment reference.
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
	protected SMCubesRegFunctionalityTestImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return ProgramPackage.Literals.SM_CUBES_REG_FUNCTIONALITY_TEST;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesTestInputData getInputData() {
		return inputData;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetInputData(SMCubesTestInputData newInputData, NotificationChain msgs) {
		SMCubesTestInputData oldInputData = inputData;
		inputData = newInputData;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__INPUT_DATA, oldInputData, newInputData);
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
	public void setInputData(SMCubesTestInputData newInputData) {
		if (newInputData != inputData) {
			NotificationChain msgs = null;
			if (inputData != null)
				msgs = ((InternalEObject)inputData).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__INPUT_DATA, null, msgs);
			if (newInputData != null)
				msgs = ((InternalEObject)newInputData).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__INPUT_DATA, null, msgs);
			msgs = basicSetInputData(newInputData, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__INPUT_DATA, newInputData, newInputData));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SMCubesReportResults getExpectedResults() {
		return expectedResults;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetExpectedResults(SMCubesReportResults newExpectedResults, NotificationChain msgs) {
		SMCubesReportResults oldExpectedResults = expectedResults;
		expectedResults = newExpectedResults;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__EXPECTED_RESULTS, oldExpectedResults, newExpectedResults);
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
	public void setExpectedResults(SMCubesReportResults newExpectedResults) {
		if (newExpectedResults != expectedResults) {
			NotificationChain msgs = null;
			if (expectedResults != null)
				msgs = ((InternalEObject)expectedResults).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__EXPECTED_RESULTS, null, msgs);
			if (newExpectedResults != null)
				msgs = ((InternalEObject)newExpectedResults).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__EXPECTED_RESULTS, null, msgs);
			msgs = basicSetExpectedResults(newExpectedResults, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__EXPECTED_RESULTS, newExpectedResults, newExpectedResults));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__INPUT_DATA:
				return basicSetInputData(null, msgs);
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__EXPECTED_RESULTS:
				return basicSetExpectedResults(null, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__INPUT_DATA:
				return getInputData();
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__EXPECTED_RESULTS:
				return getExpectedResults();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__INPUT_DATA:
				setInputData((SMCubesTestInputData)newValue);
				return;
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__EXPECTED_RESULTS:
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
	public void eUnset(int featureID) {
		switch (featureID) {
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__INPUT_DATA:
				setInputData((SMCubesTestInputData)null);
				return;
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__EXPECTED_RESULTS:
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
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__INPUT_DATA:
				return inputData != null;
			case ProgramPackage.SM_CUBES_REG_FUNCTIONALITY_TEST__EXPECTED_RESULTS:
				return expectedResults != null;
		}
		return super.eIsSet(featureID);
	}

} //SMCubesRegFunctionalityTestImpl
